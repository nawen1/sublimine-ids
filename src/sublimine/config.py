from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import yaml


@dataclass(frozen=True)
class SymbolsConfig:
    leader: str
    exec_symbol: str

    @property
    def exec(self) -> str:
        return self.exec_symbol


@dataclass(frozen=True)
class ThresholdsConfig:
    window: int
    depth_k: int
    quantile_high: float
    quantile_low: float
    min_samples: int
    signal_score_min: float
    consensus_window_ms: int
    max_stale_ms: int
    bar_interval_ms: int = 500
    dlv_pre_bars: int = 20
    dlv_run_bars: int = 4
    dlv_pause_bars: int = 2
    dlv_max_overlap_ratio: float = 0.20
    dlv_max_counter_wick_ratio: float = 0.25
    dlv_max_close_off_ratio: float = 0.20
    dlv_pause_range_ratio: float = 0.40
    dlv_retest_tolerance_bps: float = 0.0
    afs_pre_bars: int = 20
    afs_sweep_bps: float = 10.0
    afs_hold_bars_max: int = 3
    afs_consol_range_ratio: float = 0.50
    afs_followthrough_max_bps: float = 5.0
    saf_level_bars: int = 20
    saf_window_ms: int = 8000
    saf_min_attacks: int = 4
    saf_level_tolerance_bps: float = 10.0
    saf_max_return_bps: float = 3.0
    saf_min_replenishment: float = 0.5
    saf_min_ofi_abs: float = 0.5
    saf_reach_worsen_bps: float = 1.0
    saf_ofi_decay_ratio: float = 0.7
    per_ttl_bars: int = 30
    per_min_hold_bps: float = 10.0
    per_max_pullback_bps: float = 80.0
    per_trigger_break: str = "bar_break"
    rlb_window_ms: int = 10000
    rlb_spike_bps: float = 15.0
    max_mid_diff_bps: float = 25.0


@dataclass(frozen=True)
class RiskPhaseConfig:
    risk_frac: float
    max_daily_loss: float


@dataclass(frozen=True)
class RiskConfig:
    phases: dict[str, RiskPhaseConfig]
    active_phase: str = "F0"


@dataclass(frozen=True)
class LiveConfig:
    out_dir: str
    journal_filename: str
    bybit_ws: str
    bybit_depth: int
    binance_ws: str
    binance_rest: str
    binance_depth: int
    binance_depth_interval_ms: int


@dataclass(frozen=True)
class EngineConfig:
    symbols: SymbolsConfig
    thresholds: ThresholdsConfig
    risk: RiskConfig
    live: LiveConfig | None = None


def _require(data: dict, key: str) -> Any:
    if key not in data:
        raise KeyError(f"Missing config key: {key}")
    return data[key]


def load_config(path: str) -> EngineConfig:
    with open(path, "r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    symbols_raw = _require(raw, "symbols")
    thresholds_raw = _require(raw, "thresholds")
    risk_raw = _require(raw, "risk_phases")

    symbols = SymbolsConfig(
        leader=str(_require(symbols_raw, "leader")),
        exec_symbol=str(_require(symbols_raw, "exec")),
    )

    thresholds = ThresholdsConfig(
        window=int(_require(thresholds_raw, "window")),
        depth_k=int(_require(thresholds_raw, "depth_k")),
        quantile_high=float(_require(thresholds_raw, "quantile_high")),
        quantile_low=float(_require(thresholds_raw, "quantile_low")),
        min_samples=int(_require(thresholds_raw, "min_samples")),
        signal_score_min=float(_require(thresholds_raw, "signal_score_min")),
        consensus_window_ms=int(thresholds_raw.get("consensus_window_ms", 750)),
        max_stale_ms=int(thresholds_raw.get("max_stale_ms", 2000)),
        bar_interval_ms=int(thresholds_raw.get("bar_interval_ms", 500)),
        dlv_pre_bars=int(thresholds_raw.get("dlv_pre_bars", 20)),
        dlv_run_bars=int(thresholds_raw.get("dlv_run_bars", 4)),
        dlv_pause_bars=int(thresholds_raw.get("dlv_pause_bars", 2)),
        dlv_max_overlap_ratio=float(thresholds_raw.get("dlv_max_overlap_ratio", 0.20)),
        dlv_max_counter_wick_ratio=float(thresholds_raw.get("dlv_max_counter_wick_ratio", 0.25)),
        dlv_max_close_off_ratio=float(thresholds_raw.get("dlv_max_close_off_ratio", 0.20)),
        dlv_pause_range_ratio=float(thresholds_raw.get("dlv_pause_range_ratio", 0.40)),
        dlv_retest_tolerance_bps=float(thresholds_raw.get("dlv_retest_tolerance_bps", 0.0)),
        afs_pre_bars=int(thresholds_raw.get("afs_pre_bars", 20)),
        afs_sweep_bps=float(thresholds_raw.get("afs_sweep_bps", 10.0)),
        afs_hold_bars_max=int(thresholds_raw.get("afs_hold_bars_max", 3)),
        afs_consol_range_ratio=float(thresholds_raw.get("afs_consol_range_ratio", 0.50)),
        afs_followthrough_max_bps=float(thresholds_raw.get("afs_followthrough_max_bps", 5.0)),
        saf_level_bars=int(thresholds_raw.get("saf_level_bars", 20)),
        saf_window_ms=int(thresholds_raw.get("saf_window_ms", 8000)),
        saf_min_attacks=int(thresholds_raw.get("saf_min_attacks", 4)),
        saf_level_tolerance_bps=float(thresholds_raw.get("saf_level_tolerance_bps", 10.0)),
        saf_max_return_bps=float(thresholds_raw.get("saf_max_return_bps", 3.0)),
        saf_min_replenishment=float(thresholds_raw.get("saf_min_replenishment", 0.5)),
        saf_min_ofi_abs=float(thresholds_raw.get("saf_min_ofi_abs", 0.5)),
        saf_reach_worsen_bps=float(thresholds_raw.get("saf_reach_worsen_bps", 1.0)),
        saf_ofi_decay_ratio=float(thresholds_raw.get("saf_ofi_decay_ratio", 0.7)),
        per_ttl_bars=int(thresholds_raw.get("per_ttl_bars", 30)),
        per_min_hold_bps=float(thresholds_raw.get("per_min_hold_bps", 10.0)),
        per_max_pullback_bps=float(thresholds_raw.get("per_max_pullback_bps", 80.0)),
        per_trigger_break=str(thresholds_raw.get("per_trigger_break", "bar_break")),
        rlb_window_ms=int(thresholds_raw.get("rlb_window_ms", 10000)),
        rlb_spike_bps=float(thresholds_raw.get("rlb_spike_bps", 15.0)),
        max_mid_diff_bps=float(thresholds_raw.get("max_mid_diff_bps", 25.0)),
    )

    phases = {}
    for name, values in risk_raw.items():
        phases[str(name)] = RiskPhaseConfig(
            risk_frac=float(_require(values, "risk_frac")),
            max_daily_loss=float(_require(values, "max_daily_loss")),
        )

    active_phase = str(raw.get("risk", {}).get("active_phase", "F0"))
    if active_phase not in phases:
        if "F0" in phases:
            active_phase = "F0"
        elif phases:
            active_phase = sorted(phases.keys())[0]
    risk = RiskConfig(phases=phases, active_phase=active_phase)

    live_raw = raw.get("live", {}) or {}
    out_dir = str(live_raw.get("out_dir", "_out/live"))
    journal_filename = str(live_raw.get("journal_filename", f"{symbols.leader.lower()}_live.jsonl"))
    live = LiveConfig(
        out_dir=out_dir,
        journal_filename=journal_filename,
        bybit_ws=str(live_raw.get("bybit_ws", "wss://stream.bybit.com/v5/public/spot")),
        bybit_depth=int(live_raw.get("bybit_depth", 50)),
        binance_ws=str(live_raw.get("binance_ws", "wss://stream.binance.com:9443/ws")),
        binance_rest=str(live_raw.get("binance_rest", "https://api.binance.com/api/v3/depth")),
        binance_depth=int(live_raw.get("binance_depth", 50)),
        binance_depth_interval_ms=int(live_raw.get("binance_depth_interval_ms", 100)),
    )

    return EngineConfig(symbols=symbols, thresholds=thresholds, risk=risk, live=live)
