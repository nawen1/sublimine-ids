from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from .clock import utc_now


@dataclass
class IdGenerator:
    prefix: str
    counter: int = 0

    def next_id(self) -> str:
        self.counter += 1
        return f"{self.prefix}{self.counter:06d}"


def run_id(ts_utc: datetime | None = None) -> str:
    ts = ts_utc or utc_now()
    return ts.astimezone(timezone.utc).strftime("run_%Y%m%dT%H%M%S%fZ")


_EVENT_GEN = IdGenerator("evt_")
_ORDER_GEN = IdGenerator("ord_")


def next_event_id() -> str:
    return _EVENT_GEN.next_id()


def next_order_id() -> str:
    return _ORDER_GEN.next_id()
