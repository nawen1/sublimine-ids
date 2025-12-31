from datetime import datetime, timedelta, timezone

from sublimine.config import EngineConfig, RiskConfig, RiskPhaseConfig, SymbolsConfig, ThresholdsConfig
from sublimine.contracts.types import EventType, SignalEvent, Side, TradePrint, Venue
from sublimine.core.bus import EventBus
from sublimine.run import build_pipeline


def _config(
    *,
    active_phase: str = "F0",
    consensus_window_ms: int = 750,
    max_stale_ms: int = 2000,
) -> EngineConfig:
    return EngineConfig(
        symbols=SymbolsConfig(leader="BTCUSDT", exec_symbol="BTCUSD_CFD"),
        thresholds=ThresholdsConfig(
            window=5,
            depth_k=1,
            quantile_high=0.6,
            quantile_low=0.4,
            min_samples=2,
            signal_score_min=0.2,
            consensus_window_ms=consensus_window_ms,
            max_stale_ms=max_stale_ms,
        ),
        risk=RiskConfig(
            phases={
                "F0": RiskPhaseConfig(risk_frac=0.001, max_daily_loss=0.01),
                "F2": RiskPhaseConfig(risk_frac=0.003, max_daily_loss=0.015),
            },
            active_phase=active_phase,
        ),
    )


def _seed_trades(bus: EventBus, ts: datetime) -> None:
    bus.publish(
        EventType.TRADE,
        TradePrint(
            symbol="BTCUSDT",
            venue=Venue.BYBIT,
            ts_utc=ts,
            price=100.0,
            size=0.1,
            aggressor_side=Side.BUY,
        ),
    )
    bus.publish(
        EventType.TRADE,
        TradePrint(
            symbol="BTCUSDT",
            venue=Venue.BINANCE,
            ts_utc=ts,
            price=100.1,
            size=0.2,
            aggressor_side=Side.BUY,
        ),
    )


def _signal(venue: Venue, ts: datetime, score: float = 0.9) -> SignalEvent:
    return SignalEvent(
        event_name="E1",
        symbol="BTCUSDT",
        venue=venue,
        ts_utc=ts,
        score_0_1=score,
        reason_codes=[],
        meta={},
    )


def test_consensus_within_window_emits_intent():
    bus = EventBus()
    config = _config(consensus_window_ms=750, max_stale_ms=2000)
    state = build_pipeline(bus, config=config)
    ts0 = datetime(2023, 1, 1, tzinfo=timezone.utc)
    _seed_trades(bus, ts0)
    bus.publish(EventType.EVENT_SIGNAL, _signal(Venue.BYBIT, ts0))
    bus.publish(EventType.EVENT_SIGNAL, _signal(Venue.BINANCE, ts0 + timedelta(milliseconds=500)))
    assert len(state["intents"]) == 1


def test_consensus_outside_window_blocks_intent():
    bus = EventBus()
    config = _config(consensus_window_ms=500, max_stale_ms=2000)
    state = build_pipeline(bus, config=config)
    ts0 = datetime(2023, 1, 1, tzinfo=timezone.utc)
    _seed_trades(bus, ts0)
    bus.publish(EventType.EVENT_SIGNAL, _signal(Venue.BYBIT, ts0))
    bus.publish(EventType.EVENT_SIGNAL, _signal(Venue.BINANCE, ts0 + timedelta(milliseconds=1500)))
    assert state["intents"] == []


def test_consensus_requires_both_venues():
    bus = EventBus()
    config = _config()
    state = build_pipeline(bus, config=config)
    ts0 = datetime(2023, 1, 1, tzinfo=timezone.utc)
    _seed_trades(bus, ts0)
    bus.publish(EventType.EVENT_SIGNAL, _signal(Venue.BYBIT, ts0))
    assert state["intents"] == []


def test_active_phase_sets_risk_frac():
    bus = EventBus()
    config = _config(active_phase="F2")
    state = build_pipeline(bus, config=config)
    ts0 = datetime(2023, 1, 1, tzinfo=timezone.utc)
    _seed_trades(bus, ts0)
    bus.publish(EventType.EVENT_SIGNAL, _signal(Venue.BYBIT, ts0))
    bus.publish(EventType.EVENT_SIGNAL, _signal(Venue.BINANCE, ts0))
    assert len(state["intents"]) == 1
    assert state["intents"][0].risk_frac == config.risk.phases["F2"].risk_frac
