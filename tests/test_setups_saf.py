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


def _bar(ts, *, o, h, l, c, ofi_mean=0.0, ofi_abs=0.0, replen=0.0) -> MicroBar:
    return MicroBar(
        open=o,
        high=h,
        low=l,
        close=c,
        ts_start=ts,
        ts_end=ts,
        n=1,
        ofi_mean=ofi_mean,
        ofi_abs_mean=ofi_abs,
        replenishment_mean=replen,
    )


def test_saf_emits_after_fatigue_and_structure_break():
    t0 = datetime(2023, 1, 1, tzinfo=timezone.utc)
    thresholds = _thresholds(saf_level_bars=4, saf_min_attacks=4, saf_window_ms=8000)
    engine = SetupEngine(symbol="BTCUSDT", venue=Venue.BYBIT, thresholds=thresholds)

    bars = [
        _bar(t0 + timedelta(milliseconds=0), o=109.0, h=110.0, l=108.8, c=109.1),
        _bar(t0 + timedelta(milliseconds=500), o=109.1, h=110.0, l=108.9, c=109.0),
        _bar(t0 + timedelta(milliseconds=1000), o=109.0, h=110.0, l=108.9, c=109.05),
        _bar(t0 + timedelta(milliseconds=1500), o=109.05, h=110.0, l=108.95, c=109.0),
        _bar(
            t0 + timedelta(milliseconds=2000),
            o=109.50,
            h=109.99,
            l=109.40,
            c=109.503,
            ofi_mean=1.0,
            ofi_abs=1.0,
            replen=0.6,
        ),
        _bar(
            t0 + timedelta(milliseconds=2500),
            o=109.60,
            h=109.95,
            l=109.50,
            c=109.603,
            ofi_mean=1.0,
            ofi_abs=0.9,
            replen=0.6,
        ),
        _bar(
            t0 + timedelta(milliseconds=3000),
            o=109.70,
            h=109.90,
            l=109.60,
            c=109.703,
            ofi_mean=1.0,
            ofi_abs=0.8,
            replen=0.6,
        ),
        _bar(
            t0 + timedelta(milliseconds=3500),
            o=109.80,
            h=109.80,
            l=109.60,
            c=109.803,
            ofi_mean=1.0,
            ofi_abs=0.6,
            replen=0.6,
        ),
        _bar(
            t0 + timedelta(milliseconds=4000),
            o=109.50,
            h=109.60,
            l=108.80,
            c=109.00,
            ofi_mean=-0.1,
            ofi_abs=0.0,
            replen=0.0,
        ),
    ]

    emitted = []
    for idx, bar in enumerate(bars):
        signals = engine.on_bar(bar)
        if idx < len(bars) - 1:
            assert signals == []
        emitted.extend(signals)

    assert len(emitted) == 1
    signal = emitted[0]
    assert signal.event_name == "E2"
    assert signal.meta["actionable"] is True
    assert signal.meta["setup"] == "SAF"
    assert signal.meta["direction"] == "SELL"
    assert "SAF" in signal.reason_codes
    assert abs(signal.meta["level"] - 110.0) < 1e-9
    assert abs(signal.meta["reach_bps"] - ((110.0 - 109.8) / 110.0 * 10_000.0)) < 1e-6

