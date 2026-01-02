from datetime import datetime, timedelta, timezone

from sublimine.config import EngineConfig, RiskConfig, RiskPhaseConfig, SymbolsConfig, ThresholdsConfig
from sublimine.contracts.types import EventType, SignalEvent, Side, TradePrint, Venue
from sublimine.core.bus import EventBus
from sublimine.run import build_pipeline


def _config(*, consensus_window_ms: int = 750, max_stale_ms: int = 2000) -> EngineConfig:
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
            health_min_eps=0.0,
        ),
        risk=RiskConfig(phases={"F0": RiskPhaseConfig(risk_frac=0.001, max_daily_loss=0.01)}),
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


def _signal(*, venue: Venue, ts: datetime, setup: str, direction: str) -> SignalEvent:
    return SignalEvent(
        event_name="E1",
        symbol="BTCUSDT",
        venue=venue,
        ts_utc=ts,
        score_0_1=0.95,
        reason_codes=[setup],
        meta={"setup": setup, "direction": direction, "actionable": True},
    )


def test_playbook_routes_dlv_buy_by_meta_direction():
    bus = EventBus()
    state = build_pipeline(bus, config=_config())
    ts0 = datetime(2023, 1, 1, tzinfo=timezone.utc)
    _seed_trades(bus, ts0)

    bus.publish(EventType.EVENT_SIGNAL, _signal(venue=Venue.BYBIT, ts=ts0, setup="DLV", direction="BUY"))
    bus.publish(
        EventType.EVENT_SIGNAL,
        _signal(venue=Venue.BINANCE, ts=ts0 + timedelta(milliseconds=250), setup="DLV", direction="BUY"),
    )

    assert len(state["intents"]) == 1
    intent = state["intents"][0]
    assert intent.direction == Side.BUY
    assert intent.meta.get("setup") == "DLV"
    assert intent.meta.get("direction") == "BUY"
    assert "DLV" in intent.reason_codes
    assert "consensus_confirmed" in intent.reason_codes


def test_playbook_routes_per_sell_by_meta_direction():
    bus = EventBus()
    state = build_pipeline(bus, config=_config())
    ts0 = datetime(2023, 1, 1, tzinfo=timezone.utc)
    _seed_trades(bus, ts0)

    bus.publish(EventType.EVENT_SIGNAL, _signal(venue=Venue.BYBIT, ts=ts0, setup="PER", direction="SELL"))
    bus.publish(
        EventType.EVENT_SIGNAL,
        _signal(venue=Venue.BINANCE, ts=ts0 + timedelta(milliseconds=250), setup="PER", direction="SELL"),
    )

    assert len(state["intents"]) == 1
    intent = state["intents"][0]
    assert intent.direction == Side.SELL
    assert intent.meta.get("setup") == "PER"
    assert intent.meta.get("direction") == "SELL"
    assert "PER" in intent.reason_codes
    assert "consensus_confirmed" in intent.reason_codes
