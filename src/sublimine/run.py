from __future__ import annotations

import argparse

from sublimine.config import EngineConfig, load_config
from sublimine.contracts.types import EventType, SignalEvent
from sublimine.core.bus import EventBus
from sublimine.core.replay import ReplayEngine
from sublimine.exec.mt5_adapter import MockMT5Adapter
from sublimine.exec.router import OrderRouter
from sublimine.features import FeatureEngine, FeatureFrame
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
    feature_engine = FeatureEngine(
        symbol=config.symbols.leader,
        depth_k=config.thresholds.depth_k,
        window=config.thresholds.window,
    )
    detector = DetectorEngine(
        DetectorConfig(
            window=config.thresholds.window,
            quantile_high=config.thresholds.quantile_high,
            quantile_low=config.thresholds.quantile_low,
            min_samples=config.thresholds.min_samples,
        )
    )
    playbook = BTCPlaybook()
    risk_gates = RiskGates()
    router = OrderRouter(adapter=MockMT5Adapter(), shadow=shadow)
    intents: list = []

    def on_snapshot(snapshot) -> None:
        features = feature_engine.on_book_snapshot(snapshot)
        if features:
            bus.publish(EventType.FEATURE, features)

    def on_delta(delta) -> None:
        features = feature_engine.on_book_delta(delta)
        if features:
            bus.publish(EventType.FEATURE, features)

    def on_trade(trade) -> None:
        feature_engine.on_trade(trade)

    def on_features(features: FeatureFrame) -> None:
        for signal in detector.evaluate(features):
            bus.publish(EventType.EVENT_SIGNAL, signal)

    def on_signal(signal: SignalEvent) -> None:
        if signal.score_0_1 < config.thresholds.signal_score_min:
            return
        intent = playbook.on_signal(signal, config.risk.phases["F0"].risk_frac)
        if intent is None:
            return
        if not risk_gates.allow_trade(intent.ts_utc):
            return
        risk_gates.record_trade(intent.ts_utc)
        router.submit(intent)
        intents.append(intent)

    bus.subscribe(EventType.BOOK_SNAPSHOT, on_snapshot)
    bus.subscribe(EventType.BOOK_DELTA, on_delta)
    bus.subscribe(EventType.TRADE, on_trade)
    bus.subscribe(EventType.FEATURE, on_features)
    bus.subscribe(EventType.EVENT_SIGNAL, on_signal)

    return {"config": config, "intents": intents}


def main() -> None:
    parser = argparse.ArgumentParser(description="SUBLIMINE IDS v2.1")
    parser.add_argument("--mode", choices=["shadow", "replay"], default="shadow")
    parser.add_argument("--config", required=True)
    parser.add_argument("--replay", required=True)
    args = parser.parse_args()

    bus = EventBus()
    pipeline_state = build_pipeline(bus, config_path=args.config, shadow=True)
    replay = ReplayEngine(bus)
    replay.run(args.replay)

    print(f"Replay complete. Trade intents: {len(pipeline_state['intents'])}")


if __name__ == "__main__":
    main()
