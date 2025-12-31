from __future__ import annotations

import argparse
import os
from datetime import datetime
from dataclasses import replace
from math import sqrt
from pathlib import Path

from sublimine.config import EngineConfig, LiveConfig, load_config
from sublimine.contracts.types import EventType, SignalEvent, Venue
from sublimine.core.bus import EventBus
from sublimine.core.clock import utc_now
from sublimine.core.journal import JournalWriter
from sublimine.core.replay import ReplayEngine
from sublimine.exec.mt5_adapter import MockMT5Adapter
from sublimine.exec.router import OrderRouter
from sublimine.features import FeatureEngine, FeatureFrame
from sublimine.feeds.binance_ws import BinanceConnector
from sublimine.feeds.bybit_ws import BybitConnector
from sublimine.live import LiveRunner
from sublimine.risk.gates import RiskGates
from sublimine.strategy.playbooks import BTCPlaybook
from sublimine.events.detectors import DetectorConfig, DetectorEngine


def build_pipeline(
    bus: EventBus,
    config_path: str | None = None,
    config: EngineConfig | None = None,
    shadow: bool = True,
) -> dict:
    if config is None:
        if config_path is None:
            raise ValueError("config_path or config must be provided")
        config = load_config(config_path)
    feature_engines: dict[Venue, FeatureEngine] = {}
    detectors: dict[Venue, DetectorEngine] = {}
    playbook = BTCPlaybook()
    risk_gates = RiskGates()
    router = OrderRouter(adapter=MockMT5Adapter(), shadow=shadow)
    intents: list = []
    latest_signals: dict[Venue, SignalEvent] = {}
    last_book_ts: dict[Venue, datetime] = {}
    last_trade_ts: dict[Venue, datetime] = {}

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
                )
            )
            detectors[venue] = detector
        return detector

    def on_snapshot(snapshot) -> None:
        last_book_ts[snapshot.venue] = snapshot.ts_utc
        features = _feature_engine_for(snapshot.venue).on_book_snapshot(snapshot)
        if features:
            bus.publish(EventType.FEATURE, features)

    def on_delta(delta) -> None:
        last_book_ts[delta.venue] = delta.ts_utc
        features = _feature_engine_for(delta.venue).on_book_delta(delta)
        if features:
            bus.publish(EventType.FEATURE, features)

    def on_trade(trade) -> None:
        last_trade_ts[trade.venue] = trade.ts_utc
        _feature_engine_for(trade.venue).on_trade(trade)

    def on_features(features: FeatureFrame) -> None:
        detector = _detector_for(features.venue)
        for signal in detector.evaluate(features):
            bus.publish(EventType.EVENT_SIGNAL, signal)

    def _last_seen_ts(venue: Venue) -> datetime | None:
        book_ts = last_book_ts.get(venue)
        trade_ts = last_trade_ts.get(venue)
        if book_ts is not None and trade_ts is not None:
            return max(book_ts, trade_ts)
        return book_ts or trade_ts

    def _stale_venues(ref_ts: datetime) -> list[Venue]:
        stale: list[Venue] = []
        for venue in (Venue.BYBIT, Venue.BINANCE):
            last_ts = _last_seen_ts(venue)
            if last_ts is None:
                stale.append(venue)
                continue
            age_ms = max((ref_ts - last_ts).total_seconds() * 1000.0, 0.0)
            if age_ms > config.thresholds.max_stale_ms:
                stale.append(venue)
        return stale

    def on_signal(signal: SignalEvent) -> None:
        if "stale_feed_block" in signal.reason_codes:
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
        dt_ms = abs((signal.ts_utc - other.ts_utc).total_seconds() * 1000.0)
        if dt_ms > config.thresholds.consensus_window_ms:
            return
        combined_score = sqrt(max(signal.score_0_1, 0.0) * max(other.score_0_1, 0.0))
        if combined_score < config.thresholds.signal_score_min:
            return

        ref_ts = signal.ts_utc if signal.ts_utc >= other.ts_utc else other.ts_utc
        stale = _stale_venues(ref_ts)
        if stale:
            blocked = SignalEvent(
                event_name=signal.event_name,
                symbol=signal.symbol,
                venue=signal.venue,
                ts_utc=ref_ts,
                score_0_1=combined_score,
                reason_codes=list(signal.reason_codes) + ["stale_feed_block"],
                meta={
                    "stale_venues": [venue.value for venue in stale],
                    "max_stale_ms": config.thresholds.max_stale_ms,
                    "consensus_dt_ms": dt_ms,
                },
            )
            bus.publish(EventType.EVENT_SIGNAL, blocked)
            return

        consensus_signal = SignalEvent(
            event_name=signal.event_name,
            symbol=signal.symbol,
            venue=signal.venue,
            ts_utc=ref_ts,
            score_0_1=combined_score,
            reason_codes=list(signal.reason_codes) + ["consensus_confirmed"],
            meta=dict(signal.meta),
        )
        active_phase = config.risk.active_phase
        risk_frac = config.risk.phases[active_phase].risk_frac
        intent = playbook.on_signal(consensus_signal, risk_frac)
        if intent is None:
            return
        if not risk_gates.allow_trade(intent.ts_utc):
            return
        risk_gates.record_trade(intent.ts_utc)
        intent = replace(
            intent,
            score=combined_score,
            risk_frac=risk_frac,
            reason_codes=["consensus_confirmed"],
            meta={
                "venues": [signal.venue.value, other.venue.value],
                "scores": {signal.venue.value: signal.score_0_1, other.venue.value: other.score_0_1},
                "consensus_dt_ms": dt_ms,
            },
        )
        router.submit(intent)
        intents.append(intent)
        bus.publish(EventType.TRADE_INTENT, intent)

    bus.subscribe(EventType.BOOK_SNAPSHOT, on_snapshot)
    bus.subscribe(EventType.BOOK_DELTA, on_delta)
    bus.subscribe(EventType.TRADE, on_trade)
    bus.subscribe(EventType.FEATURE, on_features)
    bus.subscribe(EventType.EVENT_SIGNAL, on_signal)

    return {"config": config, "intents": intents}


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
    parser.add_argument("--mode", choices=["shadow", "replay", "shadow-live"], default="shadow")
    parser.add_argument("--config", required=True)
    parser.add_argument("--replay")
    args = parser.parse_args()

    if args.mode in {"shadow", "replay"}:
        if not args.replay:
            parser.error("--replay is required for shadow/replay mode")
        bus = EventBus()
        pipeline_state = build_pipeline(bus, config_path=args.config, shadow=True)
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
    runner = LiveRunner(bus, connectors)
    try:
        runner.run()
    except KeyboardInterrupt:
        pass
    finally:
        writer.close()


if __name__ == "__main__":
    main()
