from __future__ import annotations

from collections import deque
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from math import sqrt

from sublimine.config import ThresholdsConfig
from sublimine.contracts.types import SignalEvent, Venue
from sublimine.events.microbars import MicroBar
from sublimine.events.scoring import clamp_score

_EPS = 1e-12


def _bar_range(bar: MicroBar) -> float:
    return bar.high - bar.low


def _upper_wick(bar: MicroBar) -> float:
    return bar.high - max(bar.open, bar.close)


def _lower_wick(bar: MicroBar) -> float:
    return min(bar.open, bar.close) - bar.low


def _bar_direction(bar: MicroBar) -> int:
    if bar.close > bar.open:
        return 1
    if bar.close < bar.open:
        return -1
    return 0


def _close_off_ratio(bar: MicroBar, direction: int) -> float:
    rng = max(_bar_range(bar), _EPS)
    if direction > 0:
        return (bar.high - bar.close) / rng
    return (bar.close - bar.low) / rng


def _counter_wick_ratio(bar: MicroBar, direction: int) -> float:
    rng = max(_bar_range(bar), _EPS)
    if direction > 0:
        return _lower_wick(bar) / rng
    return _upper_wick(bar) / rng


def _overlap_ratio(prev: MicroBar, curr: MicroBar) -> float:
    overlap = max(0.0, min(prev.high, curr.high) - max(prev.low, curr.low))
    return overlap / max(_bar_range(curr), _EPS)


def _bps(value: float, base: float) -> float:
    return value / max(base, _EPS) * 10_000.0


def _ms(ts_utc: datetime) -> int:
    if ts_utc.tzinfo is None:
        ts_utc = ts_utc.replace(tzinfo=timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = ts_utc - epoch
    return delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000


def _dir_str(direction: int) -> str:
    return "BUY" if direction > 0 else "SELL"


@dataclass(frozen=True)
class DLVState:
    stage: str = "idle"  # idle|pause|await_breakout
    direction: int = 0
    pre_range_high: float = 0.0
    pre_range_low: float = 0.0
    run_quality: float = 0.0
    avg_run_range: float = 0.0
    pause_bars: tuple[MicroBar, ...] = ()
    pause_high: float = 0.0
    pause_low: float = 0.0
    pause_range: float = 0.0


@dataclass(frozen=True)
class SAFEntry:
    ts_end: datetime
    reach_bps: float
    ofi_abs: float


@dataclass(frozen=True)
class SAFState:
    stage: str = "idle"  # idle|collecting|await_break
    attack_side: int = 0
    level: float = 0.0
    attacks: tuple[SAFEntry, ...] = ()


@dataclass(frozen=True)
class AFSState:
    stage: str = "idle"  # idle|acceptance
    sweep_direction: int = 0
    pre_high: float = 0.0
    pre_low: float = 0.0
    sweep_bar: MicroBar | None = None
    acceptance_bars: tuple[MicroBar, ...] = ()


@dataclass(frozen=True)
class PERState:
    active: bool = False
    direction: int = 0
    old_range_high: float = 0.0
    old_range_low: float = 0.0
    peak_high: float = 0.0
    peak_low: float = 0.0
    pullback_low: float | None = None
    pullback_high: float | None = None
    pullback_seen: bool = False
    bars_since: int = 0
    last_bar: MicroBar | None = None


class SetupEngine:
    def __init__(self, *, symbol: str, venue: Venue, thresholds: ThresholdsConfig) -> None:
        self._symbol = symbol
        self._venue = venue
        self._t = thresholds
        self._history: deque[MicroBar] = deque(maxlen=self._max_history(thresholds))
        self._dlv = DLVState()
        self._saf = SAFState()
        self._afs = AFSState()
        self._per = PERState()

    def on_primitive_signal(self, signal: SignalEvent) -> None:
        if signal.meta.get("actionable") is True and signal.meta.get("setup") == "DLV":
            direction = 1 if signal.meta.get("direction") == "BUY" else -1
            pre_high = float(signal.meta.get("pre_range_high", 0.0))
            pre_low = float(signal.meta.get("pre_range_low", 0.0))
            peak_high = float(signal.meta.get("peak_high", signal.meta.get("pause_high", pre_high)))
            peak_low = float(signal.meta.get("peak_low", signal.meta.get("pause_low", pre_low)))
            self._per = PERState(
                active=True,
                direction=direction,
                old_range_high=pre_high,
                old_range_low=pre_low,
                peak_high=peak_high,
                peak_low=peak_low,
                pullback_low=None,
                pullback_high=None,
                pullback_seen=False,
                bars_since=0,
                last_bar=None,
            )

    def on_bar(self, bar: MicroBar) -> list[SignalEvent]:
        self._history.append(bar)

        signals: list[SignalEvent] = []

        per_signal = self._update_per(bar)
        if per_signal is not None:
            signals.append(per_signal)

        dlv_signal = self._update_dlv(bar)
        if dlv_signal is not None:
            signals.append(dlv_signal)
            self.on_primitive_signal(dlv_signal)

        saf_signal = self._update_saf(bar)
        if saf_signal is not None:
            signals.append(saf_signal)

        afs_signal = self._update_afs(bar)
        if afs_signal is not None:
            signals.append(afs_signal)

        return signals

    @staticmethod
    def _max_history(t: ThresholdsConfig) -> int:
        return max(
            t.dlv_pre_bars + t.dlv_run_bars + t.dlv_pause_bars + 8,
            t.afs_pre_bars + t.afs_hold_bars_max + 8,
            t.saf_level_bars + t.saf_min_attacks + 8,
            t.per_ttl_bars + 8,
            64,
        )

    def _update_dlv(self, bar: MicroBar) -> SignalEvent | None:
        t = self._t

        if self._dlv.stage == "idle":
            needed = t.dlv_pre_bars + t.dlv_run_bars
            if len(self._history) < needed:
                return None

            hist = list(self._history)
            run = hist[-t.dlv_run_bars :]
            pre = hist[-(t.dlv_pre_bars + t.dlv_run_bars) : -t.dlv_run_bars]
            pre_high = max(b.high for b in pre)
            pre_low = min(b.low for b in pre)
            direction = _bar_direction(run[0])
            if direction == 0 or any(_bar_direction(b) != direction for b in run):
                return None

            tol = t.dlv_retest_tolerance_bps / 10_000.0
            if direction > 0:
                min_allowed = pre_high * (1.0 + tol)
                if any(b.low <= min_allowed for b in run):
                    return None
            else:
                max_allowed = pre_low * (1.0 - tol)
                if any(b.high >= max_allowed for b in run):
                    return None

            quality_terms: list[float] = []
            for idx, b in enumerate(run):
                overlap = 0.0
                if idx > 0:
                    overlap = _overlap_ratio(run[idx - 1], b)
                    if overlap > t.dlv_max_overlap_ratio:
                        return None
                counter = _counter_wick_ratio(b, direction)
                close_off = _close_off_ratio(b, direction)
                if counter > t.dlv_max_counter_wick_ratio:
                    return None
                if close_off > t.dlv_max_close_off_ratio:
                    return None
                quality_terms.append((1.0 - overlap) * (1.0 - counter) * (1.0 - close_off))

            avg_run_range = sum(_bar_range(b) for b in run) / len(run)
            run_quality = sum(quality_terms) / len(quality_terms)

            self._dlv = DLVState(
                stage="pause",
                direction=direction,
                pre_range_high=pre_high,
                pre_range_low=pre_low,
                run_quality=run_quality,
                avg_run_range=avg_run_range,
                pause_bars=(),
                pause_high=0.0,
                pause_low=0.0,
                pause_range=0.0,
            )
            return None

        tol = t.dlv_retest_tolerance_bps / 10_000.0
        if self._dlv.direction > 0:
            if bar.low <= self._dlv.pre_range_high * (1.0 + tol):
                self._dlv = DLVState()
                return None
        else:
            if bar.high >= self._dlv.pre_range_low * (1.0 - tol):
                self._dlv = DLVState()
                return None

        if self._dlv.stage == "pause":
            pause = self._dlv.pause_bars + (bar,)
            pause_high = max(b.high for b in pause)
            pause_low = min(b.low for b in pause)
            pause_range = pause_high - pause_low
            if pause_range > t.dlv_pause_range_ratio * max(self._dlv.avg_run_range, _EPS):
                self._dlv = DLVState()
                return None
            if len(pause) < t.dlv_pause_bars:
                self._dlv = replace(self._dlv, pause_bars=pause)
                return None
            self._dlv = replace(
                self._dlv,
                stage="await_breakout",
                pause_bars=pause,
                pause_high=pause_high,
                pause_low=pause_low,
                pause_range=pause_range,
            )
            return None

        if self._dlv.stage != "await_breakout":
            return None

        breakout = False
        if self._dlv.direction > 0 and bar.close > self._dlv.pause_high:
            breakout = True
        if self._dlv.direction < 0 and bar.close < self._dlv.pause_low:
            breakout = True
        if not breakout:
            return None

        avg_run_range = max(self._dlv.avg_run_range, _EPS)
        pause_quality = clamp_score(1.0 - (self._dlv.pause_range / avg_run_range))
        score = clamp_score(sqrt(clamp_score(self._dlv.run_quality) * pause_quality))
        direction = _dir_str(self._dlv.direction)
        signal = SignalEvent(
            event_name="E1",
            symbol=self._symbol,
            venue=self._venue,
            ts_utc=bar.ts_end,
            score_0_1=score,
            reason_codes=["DLV", "dlv_run", "dlv_pause", "dlv_breakout"],
            meta={
                "actionable": True,
                "setup": "DLV",
                "direction": direction,
                "pre_range_high": self._dlv.pre_range_high,
                "pre_range_low": self._dlv.pre_range_low,
                "pause_high": self._dlv.pause_high,
                "pause_low": self._dlv.pause_low,
                "peak_high": bar.high,
                "peak_low": bar.low,
            },
        )
        self._dlv = DLVState()
        return signal

    def _update_saf(self, bar: MicroBar) -> SignalEvent | None:
        t = self._t

        if len(self._history) < 2:
            return None
        prev = list(self._history)[-2]

        if self._saf.stage == "await_break":
            if self._saf.attacks and _ms(bar.ts_end) - _ms(self._saf.attacks[-1].ts_end) > t.saf_window_ms:
                self._saf = SAFState()
                return None
            if self._saf.attack_side > 0 and bar.close < prev.low:
                return self._emit_saf(bar, prev)
            if self._saf.attack_side < 0 and bar.close > prev.high:
                return self._emit_saf(bar, prev)
            return None

        return_bps = _bps(abs(bar.close - bar.open), bar.open)
        attack_side = 1 if bar.ofi_mean > 0 else -1 if bar.ofi_mean < 0 else 0
        is_attack = (
            attack_side != 0
            and bar.ofi_abs_mean >= t.saf_min_ofi_abs
            and bar.replenishment_mean >= t.saf_min_replenishment
            and abs(return_bps) <= t.saf_max_return_bps
        )
        if not is_attack:
            if (
                self._saf.stage == "collecting"
                and self._saf.attacks
                and _ms(bar.ts_end) - _ms(self._saf.attacks[0].ts_end) > t.saf_window_ms
            ):
                self._saf = SAFState()
            return None

        if len(self._history) < t.saf_level_bars + 1:
            return None
        hist = list(self._history)
        level_bars = hist[-t.saf_level_bars - 1 : -1]
        if attack_side > 0:
            level = max(b.high for b in level_bars)
            reach = max(0.0, _bps(level - bar.high, level))
        else:
            level = min(b.low for b in level_bars)
            reach = max(0.0, _bps(bar.low - level, level))

        entry = SAFEntry(ts_end=bar.ts_end, reach_bps=reach, ofi_abs=bar.ofi_abs_mean)

        if self._saf.stage == "idle":
            self._saf = SAFState(stage="collecting", attack_side=attack_side, level=level, attacks=(entry,))
            return None

        if self._saf.attack_side != attack_side:
            self._saf = SAFState(stage="collecting", attack_side=attack_side, level=level, attacks=(entry,))
            return None

        if self._saf.attacks and _ms(bar.ts_end) - _ms(self._saf.attacks[0].ts_end) > t.saf_window_ms:
            self._saf = SAFState(stage="collecting", attack_side=attack_side, level=level, attacks=(entry,))
            return None

        level_diff_bps = _bps(abs(level - self._saf.level), self._saf.level)
        if level_diff_bps > t.saf_level_tolerance_bps:
            self._saf = SAFState(stage="collecting", attack_side=attack_side, level=level, attacks=(entry,))
            return None

        attacks = self._saf.attacks + (entry,)
        self._saf = replace(self._saf, attacks=attacks)

        if len(attacks) < t.saf_min_attacks:
            return None

        reach_worsen = attacks[-1].reach_bps - attacks[0].reach_bps
        if reach_worsen < t.saf_reach_worsen_bps:
            return None
        if attacks[-1].ofi_abs > attacks[0].ofi_abs * t.saf_ofi_decay_ratio:
            return None

        self._saf = replace(self._saf, stage="await_break")
        return None

    def _emit_saf(self, bar: MicroBar, prev: MicroBar) -> SignalEvent:
        t = self._t
        attacks = self._saf.attacks
        reach_worsen = attacks[-1].reach_bps - attacks[0].reach_bps if attacks else 0.0
        reach_quality = 1.0 if t.saf_reach_worsen_bps <= 0 else clamp_score(reach_worsen / t.saf_reach_worsen_bps)
        target_ofi = (attacks[0].ofi_abs * t.saf_ofi_decay_ratio) if attacks else 0.0
        last_ofi = attacks[-1].ofi_abs if attacks else 0.0
        ofi_quality = 1.0 if target_ofi <= 0 else clamp_score(target_ofi / max(last_ofi, _EPS))
        score = clamp_score(sqrt(reach_quality * ofi_quality))

        reversal = -self._saf.attack_side
        signal = SignalEvent(
            event_name="E2",
            symbol=self._symbol,
            venue=self._venue,
            ts_utc=bar.ts_end,
            score_0_1=score,
            reason_codes=["SAF", "saf_confirmed", "structure_break"],
            meta={
                "actionable": True,
                "setup": "SAF",
                "direction": _dir_str(reversal),
                "level": self._saf.level,
                "reach_bps": attacks[-1].reach_bps if attacks else 0.0,
                "prev_high": prev.high,
                "prev_low": prev.low,
            },
        )
        self._saf = SAFState()
        return signal

    def _update_afs(self, bar: MicroBar) -> SignalEvent | None:
        t = self._t

        if self._afs.stage == "idle":
            if len(self._history) < t.afs_pre_bars + 1:
                return None
            hist = list(self._history)
            pre = hist[-t.afs_pre_bars - 1 : -1]
            pre_high = max(b.high for b in pre)
            pre_low = min(b.low for b in pre)
            up_sweep = bar.high >= pre_high * (1.0 + t.afs_sweep_bps / 10_000.0)
            down_sweep = bar.low <= pre_low * (1.0 - t.afs_sweep_bps / 10_000.0)
            if not up_sweep and not down_sweep:
                return None
            if up_sweep and down_sweep:
                up_ext = _bps(bar.high - pre_high, pre_high)
                down_ext = _bps(pre_low - bar.low, pre_low)
                sweep_dir = 1 if up_ext >= down_ext else -1
            else:
                sweep_dir = 1 if up_sweep else -1

            self._afs = AFSState(
                stage="acceptance",
                sweep_direction=sweep_dir,
                pre_high=pre_high,
                pre_low=pre_low,
                sweep_bar=bar,
                acceptance_bars=(),
            )
            return None

        if self._afs.stage != "acceptance" or self._afs.sweep_bar is None:
            self._afs = AFSState()
            return None

        accept_cond = bar.close > self._afs.pre_high if self._afs.sweep_direction > 0 else bar.close < self._afs.pre_low

        if accept_cond and len(self._afs.acceptance_bars) >= t.afs_hold_bars_max:
            self._afs = AFSState()
            return None

        if accept_cond:
            self._afs = replace(self._afs, acceptance_bars=self._afs.acceptance_bars + (bar,))
            return None

        if not self._afs.acceptance_bars:
            self._afs = AFSState()
            return None

        acc_high = max(b.high for b in self._afs.acceptance_bars)
        acc_low = min(b.low for b in self._afs.acceptance_bars)
        acc_range = acc_high - acc_low
        sweep_range = max(_bar_range(self._afs.sweep_bar), _EPS)

        if acc_range > t.afs_consol_range_ratio * sweep_range:
            self._afs = AFSState()
            return None

        if self._afs.sweep_direction > 0:
            follow_bps = _bps(acc_high - self._afs.sweep_bar.high, self._afs.sweep_bar.high)
        else:
            follow_bps = _bps(self._afs.sweep_bar.low - acc_low, self._afs.sweep_bar.low)
        if follow_bps > t.afs_followthrough_max_bps:
            self._afs = AFSState()
            return None

        if self._afs.sweep_direction > 0:
            is_fail = bar.close <= self._afs.pre_high and bar.close < acc_low
            direction = "SELL"
        else:
            is_fail = bar.close >= self._afs.pre_low and bar.close > acc_high
            direction = "BUY"

        if not is_fail:
            self._afs = AFSState()
            return None

        sweep_ext_bps = (
            _bps(self._afs.sweep_bar.high - self._afs.pre_high, self._afs.pre_high)
            if self._afs.sweep_direction > 0
            else _bps(self._afs.pre_low - self._afs.sweep_bar.low, self._afs.pre_low)
        )
        sweep_quality = 1.0 if t.afs_sweep_bps <= 0 else clamp_score(sweep_ext_bps / t.afs_sweep_bps)
        denom = max(t.afs_consol_range_ratio * sweep_range, _EPS)
        consol_quality = clamp_score(1.0 - (acc_range / denom))
        score = clamp_score(sqrt(sweep_quality * consol_quality))

        signal = SignalEvent(
            event_name="E3",
            symbol=self._symbol,
            venue=self._venue,
            ts_utc=bar.ts_end,
            score_0_1=score,
            reason_codes=["AFS", "afs_acceptance_failed"],
            meta={
                "actionable": True,
                "setup": "AFS",
                "direction": direction,
                "pre_range_high": self._afs.pre_high,
                "pre_range_low": self._afs.pre_low,
                "sweep_high": self._afs.sweep_bar.high,
                "sweep_low": self._afs.sweep_bar.low,
                "acceptance_high": acc_high,
                "acceptance_low": acc_low,
                "acceptance_range": acc_range,
            },
        )
        self._afs = AFSState()
        return signal

    def _update_per(self, bar: MicroBar) -> SignalEvent | None:
        t = self._t
        if not self._per.active:
            return None

        bars_since = self._per.bars_since + 1
        if bars_since > t.per_ttl_bars:
            self._per = PERState()
            return None

        peak_high = self._per.peak_high
        peak_low = self._per.peak_low
        if self._per.direction > 0:
            peak_high = max(peak_high, bar.high)
        else:
            peak_low = min(peak_low, bar.low)

        pullback_low = self._per.pullback_low
        pullback_high = self._per.pullback_high
        pullback_seen = self._per.pullback_seen
        if self._per.direction > 0:
            pullback_low = bar.low if pullback_low is None else min(pullback_low, bar.low)
            if self._per.last_bar is not None and bar.low < self._per.last_bar.low:
                pullback_seen = True
        else:
            pullback_high = bar.high if pullback_high is None else max(pullback_high, bar.high)
            if self._per.last_bar is not None and bar.high > self._per.last_bar.high:
                pullback_seen = True

        if self._per.direction > 0:
            hold_level = self._per.old_range_high * (1.0 + t.per_min_hold_bps / 10_000.0)
            if pullback_low is not None and pullback_low <= hold_level:
                self._per = PERState()
                return None
            depth_bps = _bps(peak_high - (pullback_low or peak_high), peak_high)
            if depth_bps > t.per_max_pullback_bps:
                self._per = PERState()
                return None
        else:
            hold_level = self._per.old_range_low * (1.0 - t.per_min_hold_bps / 10_000.0)
            if pullback_high is not None and pullback_high >= hold_level:
                self._per = PERState()
                return None
            depth_bps = _bps((pullback_high or peak_low) - peak_low, peak_low)
            if depth_bps > t.per_max_pullback_bps:
                self._per = PERState()
                return None

        prev = self._per.last_bar
        signal: SignalEvent | None = None
        if prev is not None and pullback_seen:
            if self._per.direction > 0 and bar.close > prev.high:
                signal = self._emit_per(bar, peak_high, pullback_low, depth_bps)
            if self._per.direction < 0 and bar.close < prev.low:
                signal = self._emit_per(bar, peak_low, pullback_high, depth_bps)

        self._per = replace(
            self._per,
            bars_since=bars_since,
            peak_high=peak_high,
            peak_low=peak_low,
            pullback_low=pullback_low,
            pullback_high=pullback_high,
            pullback_seen=pullback_seen,
            last_bar=bar,
        )
        return signal

    def _emit_per(
        self,
        bar: MicroBar,
        peak: float,
        pullback: float | None,
        depth_bps: float,
    ) -> SignalEvent:
        t = self._t

        if self._per.direction > 0:
            hold_bps = _bps((pullback or peak) - self._per.old_range_high, self._per.old_range_high)
        else:
            hold_bps = _bps(self._per.old_range_low - (pullback or peak), self._per.old_range_low)

        hold_quality = 1.0 if t.per_min_hold_bps <= 0 else clamp_score(hold_bps / t.per_min_hold_bps)
        depth_quality = 1.0 if t.per_max_pullback_bps <= 0 else clamp_score(1.0 - (depth_bps / t.per_max_pullback_bps))
        score = clamp_score(sqrt(hold_quality * depth_quality))

        signal = SignalEvent(
            event_name="E1",
            symbol=self._symbol,
            venue=self._venue,
            ts_utc=bar.ts_end,
            score_0_1=score,
            reason_codes=["PER", "per_reprice_confirmed"],
            meta={
                "actionable": True,
                "setup": "PER",
                "direction": _dir_str(self._per.direction),
                "old_range_high": self._per.old_range_high,
                "old_range_low": self._per.old_range_low,
                "peak": peak,
                "pullback": pullback,
                "depth_bps": depth_bps,
            },
        )
        self._per = PERState()
        return signal

