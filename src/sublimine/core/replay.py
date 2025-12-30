from __future__ import annotations

from typing import Iterable

from sublimine.contracts.types import EventType
from sublimine.core.bus import EventBus
from sublimine.core.journal import iter_events


class ReplayEngine:
    def __init__(self, bus: EventBus) -> None:
        self._bus = bus

    def run(self, path: str) -> None:
        for event_type, payload in iter_events(path):
            self._bus.publish(event_type, payload)


def replay_events(bus: EventBus, events: Iterable[tuple[EventType, object]]) -> None:
    for event_type, payload in events:
        bus.publish(event_type, payload)
