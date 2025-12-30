from datetime import datetime, timezone

from sublimine.core.clock import FixedClock
from sublimine.core.ids import IdGenerator, run_id


def test_id_generator_increments():
    gen = IdGenerator("t_")
    assert gen.next_id() == "t_000001"
    assert gen.next_id() == "t_000002"


def test_run_id_uses_timestamp():
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert run_id(ts).startswith("run_20240101T000000")


def test_fixed_clock_returns_values():
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    clock = FixedClock(fixed_utc=ts, fixed_mono_ns=123)
    assert clock.utc_now() == ts
    assert clock.monotonic_ns() == 123
