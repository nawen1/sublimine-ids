from datetime import datetime, timezone

from sublimine.config import EngineConfig, RiskConfig, RiskPhaseConfig, SymbolsConfig, ThresholdsConfig
from sublimine.contracts.types import EventType, SignalEvent, Side, TradePrint, Venue
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
            consensus_window_ms=750,
            max_stale_ms=2000,
        ),
        risk=RiskConfig(phases={"F0": RiskPhaseConfig(risk_frac=0.002, max_daily_loss=0.01)}),
    )

    ts0 = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    trade_bybit = TradePrint(
        symbol="BTCUSDT",
        venue=Venue.BYBIT,
        ts_utc=ts0,
        price=100.0,
        size=0.1,
        aggressor_side=Side.BUY,
    )
    trade_binance = TradePrint(
        symbol="BTCUSDT",
        venue=Venue.BINANCE,
        ts_utc=ts0,
        price=100.1,
        size=0.2,
        aggressor_side=Side.BUY,
    )
    signal_bybit = SignalEvent(
        event_name="E1",
        symbol="BTCUSDT",
        venue=Venue.BYBIT,
        ts_utc=ts0,
        score_0_1=0.8,
        reason_codes=[],
        meta={},
    )
    signal_binance = SignalEvent(
        event_name="E1",
        symbol="BTCUSDT",
        venue=Venue.BINANCE,
        ts_utc=ts0,
        score_0_1=0.8,
        reason_codes=[],
        meta={},
    )

    events = [
        (EventType.TRADE, trade_bybit),
        (EventType.TRADE, trade_binance),
        (EventType.EVENT_SIGNAL, signal_bybit),
        (EventType.EVENT_SIGNAL, signal_binance),
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
