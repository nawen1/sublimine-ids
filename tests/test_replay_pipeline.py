from datetime import datetime, timezone

from sublimine.config import EngineConfig, RiskConfig, RiskPhaseConfig, SymbolsConfig, ThresholdsConfig
from sublimine.contracts.types import BookDelta, BookLevel, BookSnapshot, EventType, Venue
from sublimine.core.bus import EventBus
from sublimine.core.replay import replay_events
from sublimine.run import build_pipeline


def test_replay_pipeline_triggers_intent():
    config = EngineConfig(
        symbols=SymbolsConfig(leader="BTCUSDT", exec_symbol="BTCUSD_CFD"),
        thresholds=ThresholdsConfig(
            window=5,
            depth_k=1,
            quantile_high=0.6,
            quantile_low=0.4,
            min_samples=2,
            signal_score_min=0.1,
        ),
        risk=RiskConfig(phases={"F0": RiskPhaseConfig(risk_frac=0.002, max_daily_loss=0.01)}),
    )

    ts0 = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    snapshot = BookSnapshot(
        symbol="BTCUSDT",
        venue=Venue.BYBIT,
        ts_utc=ts0,
        bids=[BookLevel(100.0, 5.0)],
        asks=[BookLevel(101.0, 5.0)],
        depth=1,
    )
    delta1 = BookDelta(
        symbol="BTCUSDT",
        venue=Venue.BYBIT,
        ts_utc=ts0,
        bids=[BookLevel(100.0, 4.0)],
        asks=[BookLevel(101.0, 4.0)],
        is_snapshot=False,
        update_id=2,
    )
    delta2 = BookDelta(
        symbol="BTCUSDT",
        venue=Venue.BYBIT,
        ts_utc=ts0,
        bids=[BookLevel(100.0, 1.5)],
        asks=[BookLevel(101.0, 0.5)],
        is_snapshot=False,
        update_id=3,
    )

    events = [
        (EventType.BOOK_SNAPSHOT, snapshot),
        (EventType.BOOK_DELTA, delta1),
        (EventType.BOOK_DELTA, delta2),
    ]

    bus = EventBus()
    state = build_pipeline(bus, config=config)
    replay_events(bus, events)

    assert len(state["intents"]) == 1

    bus2 = EventBus()
    state2 = build_pipeline(bus2, config=config)
    replay_events(bus2, events)

    assert len(state2["intents"]) == 1
    assert state2["intents"] == state["intents"]
