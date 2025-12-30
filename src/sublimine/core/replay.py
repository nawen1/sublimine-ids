from __future__ import annotations

from typing import Iterable

from sublimine.contracts.types import EventType
from sublimine.core.bus import EventBus
from sublimine.core.journal import iter_events


class ReplayEngine:
    def __init__(self, bus: EventBus, event_filter: set[EventType] | None = None) -> None:
        self._bus = bus
        self._event_filter = event_filter

    def run(self, path: str) -> None:
        for event_type, payload in iter_events(path):
            if self._event_filter is not None and event_type not in self._event_filter:
                continue
            self._bus.publish(event_type, payload)


def replay_events(bus: EventBus, events: Iterable[tuple[EventType, object]]) -> None:
    for event_type, payload in events:
        bus.publish(event_type, payload)
