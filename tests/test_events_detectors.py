from datetime import datetime, timezone

from sublimine.contracts.types import Venue
from sublimine.events.detectors import DetectorConfig, DetectorEngine
from sublimine.features.feature_engine import FeatureFrame


def _frame(ts, depth_near, ofi_z, bias):
    return FeatureFrame(
        symbol="BTCUSDT",
        venue=Venue.BYBIT,
        ts_utc=ts,
        depth_near=depth_near,
        microprice_bias=bias,
        ofi_z=ofi_z,
        delta_size=0.0,
        price_progress=0.0,
        replenishment=0.0,
        sweep_distance=0.0,
        return_speed=0.0,
        post_sweep_absorption=0.0,
        basis_z=0.0,
        lead_lag=0.0,
        microprice=100.0,
        mid=100.0,
    )


def test_e1_detector_triggers():
    config = DetectorConfig(window=5, quantile_high=0.8, quantile_low=0.2, min_samples=3)
    detector = DetectorEngine(config)
    ts = datetime(2023, 1, 1, tzinfo=timezone.utc)

    detector.evaluate(_frame(ts, 100.0, 0.1, 0.1))
    detector.evaluate(_frame(ts, 90.0, 0.2, 0.2))
    detector.evaluate(_frame(ts, 80.0, 0.3, 0.3))
    signals = detector.evaluate(_frame(ts, 70.0, 0.5, 0.5))

    assert any(signal.event_name == "E1" for signal in signals)
