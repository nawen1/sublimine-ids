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


def test_afs_emits_on_acceptance_failure_return_inside():
    t0 = datetime(2023, 1, 1, tzinfo=timezone.utc)
    thresholds = _thresholds(
        afs_pre_bars=3,
        afs_sweep_bps=10.0,
        afs_hold_bars_max=2,
        afs_consol_range_ratio=0.50,
        afs_followthrough_max_bps=5.0,
    )
    engine = SetupEngine(symbol="BTCUSDT", venue=Venue.BYBIT, thresholds=thresholds)

    bars = [
        _bar(t0 + timedelta(milliseconds=0), o=100.0, h=101.0, l=99.0, c=100.0),
        _bar(t0 + timedelta(milliseconds=500), o=100.0, h=101.0, l=99.5, c=100.0),
        _bar(t0 + timedelta(milliseconds=1000), o=100.0, h=101.0, l=99.8, c=100.0),
        _bar(t0 + timedelta(milliseconds=1500), o=100.8, h=102.0, l=100.5, c=101.5),  # sweep up
        _bar(t0 + timedelta(milliseconds=2000), o=101.5, h=102.0, l=101.4, c=101.6),  # acceptance
        _bar(t0 + timedelta(milliseconds=2500), o=101.6, h=101.9, l=101.3, c=101.5),  # acceptance
        _bar(t0 + timedelta(milliseconds=3000), o=101.4, h=101.5, l=100.6, c=100.8),  # failure back inside
    ]

    emitted = []
    for idx, bar in enumerate(bars):
        signals = engine.on_bar(bar)
        if idx < len(bars) - 1:
            assert signals == []
        emitted.extend(signals)

    assert len(emitted) == 1
    signal = emitted[0]
    assert signal.event_name == "E3"
    assert signal.meta["actionable"] is True
    assert signal.meta["setup"] == "AFS"
    assert signal.meta["direction"] == "SELL"
    assert "AFS" in signal.reason_codes
    assert signal.meta["pre_range_high"] == 101.0
    assert signal.meta["pre_range_low"] == 99.0

