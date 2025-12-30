from __future__ import annotations

from collections import defaultdict
from typing import Callable, DefaultDict

from sublimine.contracts.types import EventType

EventHandler = Callable[[object], None]


class EventBus:
    def __init__(self) -> None:
        self._subscribers: DefaultDict[EventType, list[EventHandler]] = defaultdict(list)

    def subscribe(self, event_type: EventType, handler: EventHandler) -> None:
        self._subscribers[event_type].append(handler)

    def publish(self, event_type: EventType, payload: object) -> None:
        for handler in list(self._subscribers.get(event_type, [])):
            handler(payload)
