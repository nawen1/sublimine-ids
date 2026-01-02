from datetime import datetime, timedelta, timezone

from sublimine.config import ThresholdsConfig
from sublimine.contracts.types import Venue
from sublimine.health.health import HealthMonitor


def _thresholds(**overrides) -> ThresholdsConfig:
    base = dict(
        window=5,
        depth_k=1,
        quantile_high=0.6,
        quantile_low=0.4,
        min_samples=2,
        signal_score_min=0.2,
        consensus_window_ms=750,
        max_stale_ms=2000,
    )
    base.update(overrides)
    return ThresholdsConfig(**base)


def test_health_stale_sets_score_zero() -> None:
    thresholds = _thresholds(max_stale_ms=1000, health_min_eps=0.0, max_mid_diff_bps=10_000.0)
    health = HealthMonitor(thresholds)
    ts0 = datetime(2023, 1, 1, tzinfo=timezone.utc)
    health.observe_trade(Venue.BYBIT, ts0, price=100.0)
    health.observe_trade(Venue.BINANCE, ts0, price=100.1)

    snap = health.snapshot("BTCUSDT", ts0 + timedelta(milliseconds=1500))

    assert snap.score_0_1 == 0.0
    assert "stale_BYBIT" in snap.reason_codes
    assert "stale_BINANCE" in snap.reason_codes


def test_health_queue_depth_high_blocks() -> None:
    thresholds = _thresholds(health_max_queue_depth=1, health_min_eps=0.0, max_mid_diff_bps=10_000.0)
    health = HealthMonitor(thresholds)
    ts0 = datetime(2023, 1, 1, tzinfo=timezone.utc)
    health.observe_trade(Venue.BYBIT, ts0, price=100.0)
    health.observe_trade(Venue.BINANCE, ts0, price=100.1)
    health.set_queue_depth(5)

    snap = health.snapshot("BTCUSDT", ts0)

    assert snap.score_0_1 == 0.0
    assert "queue_depth_high" in snap.reason_codes


def test_health_low_eps_penalty() -> None:
    thresholds = _thresholds(health_min_eps=5.0, health_eps_window_ms=1000, max_mid_diff_bps=10_000.0)
    health = HealthMonitor(thresholds)
    ts0 = datetime(2023, 1, 1, tzinfo=timezone.utc)
    health.observe_trade(Venue.BYBIT, ts0, price=100.0)
    for idx in range(5):
        health.observe_trade(Venue.BINANCE, ts0 + timedelta(milliseconds=100 * idx), price=100.0)

    snap = health.snapshot("BTCUSDT", ts0 + timedelta(milliseconds=900))

    assert snap.score_0_1 == 0.5
    assert "low_eps_BYBIT" in snap.reason_codes


def test_health_resync_rate_penalty() -> None:
    thresholds = _thresholds(
        health_min_eps=0.0,
        health_max_resync_per_min=0.5,
        health_rate_window_ms=60000,
        max_mid_diff_bps=10_000.0,
    )
    health = HealthMonitor(thresholds)
    ts0 = datetime(2023, 1, 1, tzinfo=timezone.utc)
    health.observe_trade(Venue.BYBIT, ts0, price=100.0)
    health.observe_trade(Venue.BINANCE, ts0, price=100.0)
    health.observe_resync(Venue.BYBIT, ts0)
    health.observe_resync(Venue.BYBIT, ts0 + timedelta(seconds=1))

    snap = health.snapshot("BTCUSDT", ts0 + timedelta(seconds=2))

    assert snap.score_0_1 == 0.6
    assert "resync_rate_high_BYBIT" in snap.reason_codes
