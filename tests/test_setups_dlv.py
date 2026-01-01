from datetime import datetime, timedelta, timezone

from sublimine.config import ThresholdsConfig
from sublimine.contracts.types import Venue
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


def test_dlv_emits_only_on_pause_breakout():
    t0 = datetime(2023, 1, 1, tzinfo=timezone.utc)
    thresholds = _thresholds(dlv_pre_bars=3, dlv_run_bars=2, dlv_pause_bars=2)
    engine = SetupEngine(symbol="BTCUSDT", venue=Venue.BYBIT, thresholds=thresholds)

    bars = [
        _bar(t0 + timedelta(milliseconds=0), o=100.0, h=101.0, l=99.0, c=100.0),
        _bar(t0 + timedelta(milliseconds=500), o=100.0, h=101.0, l=99.5, c=100.0),
        _bar(t0 + timedelta(milliseconds=1000), o=100.0, h=101.0, l=99.8, c=100.0),
        _bar(t0 + timedelta(milliseconds=1500), o=102.0, h=105.0, l=102.0, c=105.0),
        _bar(t0 + timedelta(milliseconds=2000), o=105.0, h=108.0, l=105.0, c=108.0),
        _bar(t0 + timedelta(milliseconds=2500), o=108.0, h=108.2, l=107.8, c=108.1),
        _bar(t0 + timedelta(milliseconds=3000), o=108.1, h=108.3, l=107.9, c=108.0),
        _bar(t0 + timedelta(milliseconds=3500), o=108.0, h=110.0, l=108.0, c=109.0),
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
    assert signal.meta["setup"] == "DLV"
    assert signal.meta["direction"] == "BUY"
    assert "DLV" in signal.reason_codes
    assert signal.meta["pre_range_high"] == 101.0
    assert signal.meta["pre_range_low"] == 99.0
    assert abs(signal.meta["pause_high"] - 108.3) < 1e-12
    assert abs(signal.meta["pause_low"] - 107.8) < 1e-12

