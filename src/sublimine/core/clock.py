from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import time
from typing import Protocol


class Clock(Protocol):
    def utc_now(self) -> datetime:
        ...

    def monotonic_ns(self) -> int:
        ...


@dataclass(frozen=True)
class SystemClock:
    def utc_now(self) -> datetime:
        return datetime.now(timezone.utc)

    def monotonic_ns(self) -> int:
        return time.monotonic_ns()


@dataclass(frozen=True)
class FixedClock:
    fixed_utc: datetime
    fixed_mono_ns: int = 0

    def utc_now(self) -> datetime:
        return self.fixed_utc

    def monotonic_ns(self) -> int:
        return self.fixed_mono_ns


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def monotonic_ns() -> int:
    return time.monotonic_ns()
