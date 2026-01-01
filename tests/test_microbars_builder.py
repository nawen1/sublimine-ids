from datetime import datetime, timedelta, timezone

from sublimine.contracts.types import Venue
from sublimine.events.microbars import MicroBarBuilder
from sublimine.features.feature_engine import FeatureFrame


def _frame(*, ts, mid, ofi_z, replenishment):
    return FeatureFrame(
        symbol="BTCUSDT",
        venue=Venue.BYBIT,
        ts_utc=ts,
        depth_near=0.0,
        microprice_bias=0.0,
        ofi_z=ofi_z,
        delta_size=0.0,
        price_progress=0.0,
        replenishment=replenishment,
        sweep_distance=0.0,
        return_speed=0.0,
        post_sweep_absorption=0.0,
        basis_z=0.0,
        lead_lag=0.0,
        microprice=mid,
        mid=mid,
    )


def test_microbar_builder_buckets_and_aggregates():
    builder = MicroBarBuilder(bar_interval_ms=500)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)

    ts1 = epoch + timedelta(milliseconds=100)
    ts2 = epoch + timedelta(milliseconds=200)
    ts3 = epoch + timedelta(milliseconds=499)
    ts4 = epoch + timedelta(milliseconds=500)

    assert builder.update(_frame(ts=ts1, mid=100.0, ofi_z=1.0, replenishment=0.2)) is None
    assert builder.update(_frame(ts=ts2, mid=101.0, ofi_z=-1.0, replenishment=0.4)) is None
    assert builder.update(_frame(ts=ts3, mid=99.0, ofi_z=0.5, replenishment=0.6)) is None

    bar0 = builder.update(_frame(ts=ts4, mid=102.0, ofi_z=-0.5, replenishment=1.0))
    assert bar0 is not None
    assert bar0.open == 100.0
    assert bar0.high == 101.0
    assert bar0.low == 99.0
    assert bar0.close == 99.0
    assert bar0.ts_start == ts1
    assert bar0.ts_end == ts3
    assert bar0.n == 3
    assert abs(bar0.ofi_mean - ((1.0 - 1.0 + 0.5) / 3.0)) < 1e-12
    assert abs(bar0.ofi_abs_mean - ((1.0 + 1.0 + 0.5) / 3.0)) < 1e-12
    assert abs(bar0.replenishment_mean - ((0.2 + 0.4 + 0.6) / 3.0)) < 1e-12

