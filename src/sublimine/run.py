from __future__ import annotations

import argparse
import os
from collections import deque
from datetime import datetime, timedelta
from dataclasses import replace
from math import sqrt
from pathlib import Path

from sublimine.config import EngineConfig, LiveConfig, load_config
from sublimine.contracts.types import EventType, SignalEvent, Venue
from sublimine.core.bus import EventBus
from sublimine.core.clock import utc_now
from sublimine.core.journal import JournalWriter
from sublimine.core.replay import ReplayEngine
from sublimine.exec.mt5_adapter import MockMT5Adapter, PaperAdapter
from sublimine.exec.oms import OMS
from sublimine.exec.router import OrderRouter
from sublimine.features import FeatureEngine, FeatureFrame
from sublimine.feeds.binance_ws import BinanceConnector
from sublimine.feeds.bybit_ws import BybitConnector
from sublimine.health import EngineGuard, EngineState, HealthMonitor
from sublimine.live import LiveRunner
from sublimine.risk.gates import RiskGates
from sublimine.strategy.playbooks import BTCPlaybook
from sublimine.events.detectors import DetectorConfig, DetectorEngine
from sublimine.events.scoring import clamp_score


def build_pipeline(
    bus: EventBus,
    config_path: str | None = None,
    config: EngineConfig | None = None,
    shadow: bool = True,
    exec_router: OrderRouter | None = None,
) -> dict:
    if config is None:
        if config_path is None:
            raise ValueError("config_path or config must be provided")
        config = load_config(config_path)
    feature_engines: dict[Venue, FeatureEngine] = {}
    detectors: dict[Venue, DetectorEngine] = {}
    playbook = BTCPlaybook(exec_symbol=config.symbols.exec)
    risk_gates = RiskGates()
    if exec_router is None:
        oms = OMS(
            venue=Venue.MT5,
            equity=1000.0,
            tick_size=1.0,
            tick_value_per_lot=1.0,
            vol_min=0.01,
            vol_step=0.01,
        )
        router = OrderRouter(adapter=MockMT5Adapter(), oms=oms, bus=bus, shadow=shadow)
    else:
        exec_router.shadow = shadow
        exec_router.bus = bus
        router = exec_router
    health = HealthMonitor(config.thresholds)
    guard = EngineGuard(config.thresholds)
    intents: list = []
    latest_signals: dict[Venue, SignalEvent] = {}
    last_mid: dict[Venue, tuple[datetime, float]] = {}
    mid_diffs: deque[tuple[datetime, float]] = deque()

    def _feature_engine_for(venue: Venue) -> FeatureEngine:
        engine = feature_engines.get(venue)
        if engine is None:
            engine = FeatureEngine(
                symbol=config.symbols.leader,
                depth_k=config.thresholds.depth_k,
                window=config.thresholds.window,
            )
            feature_engines[venue] = engine
        return engine

    def _detector_for(venue: Venue) -> DetectorEngine:
        detector = detectors.get(venue)
        if detector is None:
            detector = DetectorEngine(
                DetectorConfig(
                    window=config.thresholds.window,
                    quantile_high=config.thresholds.quantile_high,
                    quantile_low=config.thresholds.quantile_low,
                    min_samples=config.thresholds.min_samples,
                ),
                thresholds=config.thresholds,
            )
            detectors[venue] = detector
        return detector

    def on_snapshot(snapshot) -> None:
        health.observe_book(snapshot.venue, snapshot.ts_utc)
        features = _feature_engine_for(snapshot.venue).on_book_snapshot(snapshot)
        if features:
            bus.publish(EventType.FEATURE, features)

    def on_delta(delta) -> None:
        health.observe_book(delta.venue, delta.ts_utc)
        features = _feature_engine_for(delta.venue).on_book_delta(delta)
        if features:
            bus.publish(EventType.FEATURE, features)

    def on_trade(trade) -> None:
        health.observe_trade(trade.venue, trade.ts_utc, price=getattr(trade, "price", None))
        _feature_engine_for(trade.venue).on_trade(trade)

    def on_features(features: FeatureFrame) -> None:
        health.observe_feature(features.venue, features.ts_utc, features.mid)
        last_mid[features.venue] = (features.ts_utc, features.mid)
        if Venue.BYBIT in last_mid and Venue.BINANCE in last_mid:
            bybit_mid = last_mid[Venue.BYBIT][1]
            binance_mid = last_mid[Venue.BINANCE][1]
            mid_avg = (bybit_mid + binance_mid) / 2.0
            diff_bps = abs(bybit_mid - binance_mid) / max(mid_avg, 1e-12) * 10_000.0
            mid_diffs.append((features.ts_utc, diff_bps))
            cutoff = features.ts_utc - timedelta(milliseconds=config.thresholds.rlb_window_ms)
            while mid_diffs and mid_diffs[0][0] < cutoff:
                mid_diffs.popleft()

        detector = _detector_for(features.venue)
        for signal in detector.evaluate(features):
            bus.publish(EventType.EVENT_SIGNAL, signal)

    def _merge_reason_codes(*groups: list[str]) -> list[str]:
        merged: list[str] = []
        for group in groups:
            merged.extend(code for code in group if isinstance(code, str))
        return list(dict.fromkeys(merged))

    def on_signal(signal: SignalEvent) -> None:
        if signal.meta.get("actionable") is False:
            return
        if signal.meta.get("blocked_by_engine_state") is not None:
            return
        if "blocked_by_engine_state" in signal.reason_codes:
            return
        if any(code in signal.reason_codes for code in ("stale_feed_block", "engine_freeze_block", "engine_kill_block")):
            return
        if signal.venue not in (Venue.BYBIT, Venue.BINANCE):
            return

        latest_signals[signal.venue] = signal
        other_venue = Venue.BINANCE if signal.venue == Venue.BYBIT else Venue.BYBIT
        other = latest_signals.get(other_venue)
        if other is None:
            return
        if other.event_name != signal.event_name or other.symbol != signal.symbol:
            return
        if signal.meta.get("setup") is not None or other.meta.get("setup") is not None:
            if signal.meta.get("setup") != other.meta.get("setup"):
                return
        if signal.meta.get("direction") is not None and other.meta.get("direction") is not None:
            if signal.meta.get("direction") != other.meta.get("direction"):
                return

        dt_ms = abs((signal.ts_utc - other.ts_utc).total_seconds() * 1000.0)
        if dt_ms > config.thresholds.consensus_window_ms:
            return
        ref_ts = signal.ts_utc if signal.ts_utc >= other.ts_utc else other.ts_utc
        combined_score = sqrt(max(signal.score_0_1, 0.0) * max(other.score_0_1, 0.0))
        reason_codes = list(signal.reason_codes)

        setup_tag = signal.meta.get("setup")
        if setup_tag in {"SAF", "AFS"} and Venue.BYBIT in last_mid and Venue.BINANCE in last_mid and mid_diffs:
            bybit_mid = last_mid[Venue.BYBIT][1]
            binance_mid = last_mid[Venue.BINANCE][1]
            mid_avg = (bybit_mid + binance_mid) / 2.0
            current_diff_bps = abs(bybit_mid - binance_mid) / max(mid_avg, 1e-12) * 10_000.0
            cutoff = ref_ts - timedelta(milliseconds=config.thresholds.rlb_window_ms)
            recent = [diff for (ts, diff) in mid_diffs if cutoff <= ts <= ref_ts]
            spike_recent = bool(recent) and max(recent) >= config.thresholds.rlb_spike_bps
            now_aligned = current_diff_bps <= config.thresholds.max_mid_diff_bps
            if spike_recent and now_aligned:
                combined_score = clamp_score(combined_score * 1.10)
                reason_codes.append("RLB_confirmed")

        if combined_score < config.thresholds.signal_score_min:
            return

        snap = health.snapshot(symbol=signal.symbol, ref_ts=ref_ts)
        bus.publish(EventType.DATA_QUALITY, snap)
        state_event = guard.update(snap)
        if state_event is not None:
            bus.publish(EventType.ENGINE_STATE, state_event)
        if guard.current_state in {EngineState.FREEZE, EngineState.KILL}:
            block_reason = "engine_freeze_block" if guard.current_state == EngineState.FREEZE else "engine_kill_block"
            blocked_meta = dict(signal.meta)
            blocked_meta.update(
                {
                    "consensus_dt_ms": dt_ms,
                    "health_state": guard.current_state.value,
                    "health_score": snap.score_0_1,
                    "health_reasons": list(snap.reason_codes),
                    "mid_diff_bps": snap.mid_diff_bps,
                    "queue_depth": snap.queue_depth,
                    "blocked_by_engine_state": True,
                }
            )
            blocked = SignalEvent(
                event_name=signal.event_name,
                symbol=signal.symbol,
                venue=signal.venue,
                ts_utc=ref_ts,
                score_0_1=combined_score,
                reason_codes=reason_codes + ["blocked_by_engine_state", block_reason],
                meta=blocked_meta,
            )
            bus.publish(EventType.EVENT_SIGNAL, blocked)
            return
        if snap.score_0_1 <= 0.0:
            return

        consensus_signal = SignalEvent(
            event_name=signal.event_name,
            symbol=signal.symbol,
            venue=signal.venue,
            ts_utc=ref_ts,
            score_0_1=combined_score,
            reason_codes=reason_codes + ["consensus_confirmed"],
            meta=dict(signal.meta),
        )
        active_phase = config.risk.active_phase
        risk_frac = config.risk.phases[active_phase].risk_frac
        if guard.current_state == EngineState.DEGRADED:
            risk_frac *= config.thresholds.health_risk_scale_degraded
        intent = playbook.on_signal(consensus_signal, risk_frac)
        if intent is None:
            return
        if not risk_gates.allow_trade(intent.ts_utc):
            return
        risk_gates.record_trade(intent.ts_utc)
        scores_by_venue = {signal.venue: signal.score_0_1, other.venue: other.score_0_1}
        consensus_meta = {
            "venues": [venue.value for venue in (Venue.BYBIT, Venue.BINANCE) if venue in scores_by_venue],
            "scores": {venue.value: scores_by_venue[venue] for venue in (Venue.BYBIT, Venue.BINANCE) if venue in scores_by_venue},
            "dt_ms": dt_ms,
        }
        merged_meta = dict(consensus_signal.meta)
        merged_meta.update(intent.meta)
        for key in ("setup", "direction"):
            if key in consensus_signal.meta:
                merged_meta[key] = consensus_signal.meta[key]
        merged_meta["consensus"] = consensus_meta
        merged_meta["health"] = {
            "state": guard.current_state.value,
            "score_0_1": snap.score_0_1,
            "reasons": list(snap.reason_codes),
            "mid_diff_bps": snap.mid_diff_bps,
            "queue_depth": snap.queue_depth,
        }
        intent = replace(
            intent,
            score=combined_score,
            risk_frac=risk_frac,
            reason_codes=_merge_reason_codes(consensus_signal.reason_codes, intent.reason_codes),
            meta=merged_meta,
        )
        router.submit(intent)
        intents.append(intent)
        bus.publish(EventType.TRADE_INTENT, intent)

    bus.subscribe(EventType.BOOK_SNAPSHOT, on_snapshot)
    bus.subscribe(EventType.BOOK_DELTA, on_delta)
    bus.subscribe(EventType.TRADE, on_trade)
    bus.subscribe(EventType.FEATURE, on_features)
    bus.subscribe(EventType.EVENT_SIGNAL, on_signal)

    return {"config": config, "intents": intents, "health": health, "guard": guard}


def _attach_journal(bus: EventBus, writer: JournalWriter) -> None:
    def _record(event_type: EventType):
        def _handler(payload: object) -> None:
            writer.append(event_type, payload)

        return _handler

    for event_type in (
        EventType.BOOK_SNAPSHOT,
        EventType.BOOK_DELTA,
        EventType.TRADE,
        EventType.FEATURE,
        EventType.EVENT_SIGNAL,
        EventType.TRADE_INTENT,
        EventType.ORDER_REQUEST,
        EventType.ORDER_ACK,
        EventType.ORDER_FILL,
        EventType.POSITION_SNAPSHOT,
        EventType.DATA_QUALITY,
        EventType.ENGINE_STATE,
    ):
        bus.subscribe(event_type, _record(event_type))


def _live_journal_path(config: EngineConfig) -> str:
    live = _require_live_config(config.live)
    ts = utc_now()
    out_dir = Path(live.out_dir) / ts.strftime("%Y%m%d-%H%M%S")
    return str(out_dir / live.journal_filename)


def _require_live_config(live: LiveConfig | None) -> LiveConfig:
    if live is None:
        raise ValueError("Live configuration is required for shadow-live mode")
    return live


def _allow_live_mode(env: dict[str, str] | None = None) -> bool:
    env = dict(os.environ) if env is None else env
    return "PYTEST_CURRENT_TEST" not in env


def main() -> None:
    parser = argparse.ArgumentParser(description="SUBLIMINE IDS v2.1")
    parser.add_argument("--mode", choices=["shadow", "replay", "shadow-live", "paper-exec"], default="shadow")
    parser.add_argument("--config", required=True)
    parser.add_argument("--replay")
    args = parser.parse_args()

    if args.mode in {"shadow", "replay", "paper-exec"}:
        if not args.replay:
            parser.error("--replay is required for shadow/replay/paper-exec mode")
        bus = EventBus()
        config = load_config(args.config)
        exec_router = None
        shadow = True
        if args.mode == "paper-exec":
            oms = OMS(
                venue=Venue.MT5,
                equity=1000.0,
                tick_size=1.0,
                tick_value_per_lot=1.0,
                vol_min=0.01,
                vol_step=0.01,
            )
            exec_router = OrderRouter(adapter=PaperAdapter(), oms=oms, bus=bus, shadow=False)
            shadow = False
        pipeline_state = build_pipeline(bus, config=config, shadow=shadow, exec_router=exec_router)
        replay = ReplayEngine(
            bus,
            event_filter={EventType.BOOK_SNAPSHOT, EventType.BOOK_DELTA, EventType.TRADE, EventType.QUOTE},
        )
        replay.run(args.replay)
        print(f"Replay complete. Trade intents: {len(pipeline_state['intents'])}")
        return

    if not _allow_live_mode():
        raise RuntimeError("shadow-live mode is disabled under pytest")

    bus = EventBus()
    pipeline_state = build_pipeline(bus, config_path=args.config, shadow=True)
    config = pipeline_state["config"]
    health = pipeline_state.get("health")
    live = _require_live_config(config.live)
    journal_path = _live_journal_path(config)
    Path(journal_path).parent.mkdir(parents=True, exist_ok=True)
    writer = JournalWriter(journal_path)
    _attach_journal(bus, writer)
    connectors = [
        BybitConnector(symbol=config.symbols.leader, depth=live.bybit_depth, ws_url=live.bybit_ws),
        BinanceConnector(
            symbol=config.symbols.leader,
            depth=live.binance_depth,
            depth_interval_ms=live.binance_depth_interval_ms,
            ws_url=live.binance_ws,
            rest_url=live.binance_rest,
        ),
    ]

    def _on_tick() -> None:
        if health is None:
            return
        health.set_queue_depth(runner.queue_depth())
        for connector in connectors:
            if isinstance(connector, BinanceConnector):
                resync_events, desync_events = connector.drain_health_events()
                for ts in desync_events:
                    health.observe_desync(Venue.BINANCE, ts)
                for ts in resync_events:
                    health.observe_resync(Venue.BINANCE, ts)

    runner = LiveRunner(bus, connectors, on_tick=_on_tick)
    try:
        runner.run()
    except KeyboardInterrupt:
        pass
    finally:
        writer.close()


if __name__ == "__main__":
    main()
