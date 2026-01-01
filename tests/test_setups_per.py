from datetime import datetime, timedelta, timezone

from sublimine.config import ThresholdsConfig
from sublimine.contracts.types import SignalEvent, Venue
from sublimine.events.microbars import MicroBar
from sublimine.events.setups import SetupEngine


def _thresholds(**overrides) -> ThresholdsConfig:
    base = dict(
        window=5,
        depth_k=1,
        quantile_high=0.9,
        quantile_low=0.1,
        min_samples=2,
        signal_score_min=0.2,
        consensus_window_ms=750,
        max_stale_ms=2000,
    )
    base.update(overrides)
    return ThresholdsConfig(**base)


def _bar(ts, *, o, h, l, c) -> MicroBar:
    return MicroBar(
        open=o,
        high=h,
        low=l,
        close=c,
        ts_start=ts,
        ts_end=ts,
        n=1,
        ofi_mean=0.0,
        ofi_abs_mean=0.0,
        replenishment_mean=0.0,
    )


def test_per_emits_on_first_hl_break_after_bounded_pullback():
    thresholds = _thresholds(per_ttl_bars=10, per_min_hold_bps=10.0, per_max_pullback_bps=80.0)
    engine = SetupEngine(symbol="BTCUSDT", venue=Venue.BYBIT, thresholds=thresholds)

    ts0 = datetime(2023, 1, 1, tzinfo=timezone.utc)
    dlv = SignalEvent(
        event_name="E1",
        symbol="BTCUSDT",
        venue=Venue.BYBIT,
        ts_utc=ts0,
        score_0_1=0.9,
        reason_codes=["DLV"],
        meta={
            "actionable": True,
            "setup": "DLV",
            "direction": "BUY",
            "pre_range_high": 100.0,
            "pre_range_low": 90.0,
            "peak_high": 101.5,
            "peak_low": 101.0,
        },
    )
    engine.on_primitive_signal(dlv)

    bars = [
        _bar(ts0 + timedelta(milliseconds=500), o=101.2, h=101.4, l=101.0, c=101.3),
        _bar(ts0 + timedelta(milliseconds=1000), o=101.3, h=101.35, l=100.8, c=101.2),
        _bar(ts0 + timedelta(milliseconds=1500), o=101.2, h=101.6, l=101.1, c=101.5),
    ]

    emitted = []
    for idx, bar in enumerate(bars):
        signals = engine.on_bar(bar)
        if idx < len(bars) - 1:
            assert signals == []
        emitted.extend(signals)

    assert len(emitted) == 1
    signal = emitted[0]
    assert signal.event_name == "E1"
    assert signal.meta["actionable"] is True
    assert signal.meta["setup"] == "PER"
    assert signal.meta["direction"] == "BUY"
    assert "PER" in signal.reason_codes

