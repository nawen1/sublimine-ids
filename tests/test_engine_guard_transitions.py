from datetime import datetime, timedelta, timezone

from sublimine.config import ThresholdsConfig
from sublimine.health.health import DataQualitySnapshot
from sublimine.health.state import EngineGuard, EngineState


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


def _snapshot(ts_utc: datetime, score: float, reasons: list[str] | None = None) -> DataQualitySnapshot:
    return DataQualitySnapshot(
        ts_utc=ts_utc,
        symbol="BTCUSDT",
        per_venue={},
        queue_depth=0,
        mid_by_venue={},
        mid_diff_bps=None,
        score_0_1=score,
        reason_codes=list(reasons or []),
        meta={},
    )


def test_engine_guard_kill_latched() -> None:
    guard = EngineGuard(_thresholds())
    ts0 = datetime(2023, 1, 1, tzinfo=timezone.utc)

    event = guard.update(_snapshot(ts0, 0.2, ["missing_feed_BYBIT"]))

    assert event is not None
    assert event.state == EngineState.KILL.value
    assert guard.current_state == EngineState.KILL

    event2 = guard.update(_snapshot(ts0 + timedelta(seconds=1), 1.0))

    assert event2 is None
    assert guard.current_state == EngineState.KILL


def test_engine_guard_freeze_recover_window() -> None:
    thresholds = _thresholds(health_freeze_score=0.6, health_recover_score=0.9, health_recover_window_ms=5000)
    guard = EngineGuard(thresholds)
    ts0 = datetime(2023, 1, 1, tzinfo=timezone.utc)

    event = guard.update(_snapshot(ts0, 0.5))

    assert event is not None
    assert guard.current_state == EngineState.FREEZE

    event2 = guard.update(_snapshot(ts0 + timedelta(seconds=2), 0.95))

    assert event2 is None
    assert guard.current_state == EngineState.FREEZE

    event3 = guard.update(_snapshot(ts0 + timedelta(seconds=6), 0.95))

    assert event3 is not None
    assert guard.current_state == EngineState.RUN


def test_engine_guard_degraded_recover_score() -> None:
    guard = EngineGuard(_thresholds())
    ts0 = datetime(2023, 1, 1, tzinfo=timezone.utc)

    event = guard.update(_snapshot(ts0, 0.95, ["low_eps_BYBIT"]))

    assert event is not None
    assert guard.current_state == EngineState.DEGRADED

    event2 = guard.update(_snapshot(ts0 + timedelta(seconds=1), 0.89))

    assert event2 is None
    assert guard.current_state == EngineState.DEGRADED

    event3 = guard.update(_snapshot(ts0 + timedelta(seconds=2), 0.95))

    assert event3 is not None
    assert guard.current_state == EngineState.RUN
