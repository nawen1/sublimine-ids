from datetime import datetime, timezone

from sublimine.config import EngineConfig, RiskConfig, RiskPhaseConfig, SymbolsConfig, ThresholdsConfig
from sublimine.contracts.types import EventType, SignalEvent, Side, TradePrint, Venue
from sublimine.core.bus import EventBus
from sublimine.run import build_pipeline


def _config() -> EngineConfig:
    return EngineConfig(
        symbols=SymbolsConfig(leader="BTCUSDT", exec_symbol="BTCUSD_CFD"),
        thresholds=ThresholdsConfig(
            window=5,
            depth_k=1,
            quantile_high=0.6,
            quantile_low=0.4,
            min_samples=2,
            signal_score_min=0.2,
            consensus_window_ms=750,
            max_stale_ms=2000,
            max_mid_diff_bps=1.0,
            health_min_eps=0.0,
        ),
        risk=RiskConfig(phases={"F0": RiskPhaseConfig(risk_frac=0.001, max_daily_loss=0.01)}),
    )


def _signal(venue: Venue, ts: datetime) -> SignalEvent:
    return SignalEvent(
        event_name="E1",
        symbol="BTCUSDT",
        venue=venue,
        ts_utc=ts,
        score_0_1=0.9,
        reason_codes=[],
        meta={},
    )


def test_alignment_gate_blocks_intent() -> None:
    bus = EventBus()
    state = build_pipeline(bus, config=_config())
    data_quality: list = []
    bus.subscribe(EventType.DATA_QUALITY, data_quality.append)
    ts0 = datetime(2023, 1, 1, tzinfo=timezone.utc)

    bus.publish(
        EventType.TRADE,
        TradePrint(
            symbol="BTCUSDT",
            venue=Venue.BYBIT,
            ts_utc=ts0,
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
            ts_utc=ts0,
            price=110.0,
            size=0.1,
            aggressor_side=Side.BUY,
        ),
    )

    bus.publish(EventType.EVENT_SIGNAL, _signal(Venue.BYBIT, ts0))
    bus.publish(EventType.EVENT_SIGNAL, _signal(Venue.BINANCE, ts0))

    assert state["intents"] == []
    assert data_quality
    assert "mid_diff_high" in data_quality[-1].reason_codes
