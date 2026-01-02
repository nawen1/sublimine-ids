from __future__ import annotations

from dataclasses import dataclass
import queue
import threading
from typing import Callable, Iterable

from sublimine.contracts.types import EventType
from sublimine.core.bus import EventBus


EventSink = Callable[[EventType, object], None]


@dataclass(frozen=True)
class LiveEvent:
    event_type: EventType
    payload: object


class LiveRunner:
    def __init__(
        self,
        bus: EventBus,
        connectors: Iterable[object],
        on_tick: Callable[[], None] | None = None,
    ) -> None:
        self._bus = bus
        self._connectors = list(connectors)
        self._queue: queue.Queue[LiveEvent] = queue.Queue()
        self._stop_event = threading.Event()
        self._on_tick = on_tick

    @property
    def sink(self) -> EventSink:
        def _sink(event_type: EventType, payload: object) -> None:
            self._queue.put(LiveEvent(event_type=event_type, payload=payload))

        return _sink

    def run(self) -> None:
        for connector in self._connectors:
            connector.start(self.sink)
        try:
            while not self._stop_event.is_set():
                if self._on_tick is not None:
                    self._on_tick()
                try:
                    event = self._queue.get(timeout=0.5)
                except queue.Empty:
                    continue
                self._bus.publish(event.event_type, event.payload)
        finally:
            self.stop()

    def queue_depth(self) -> int:
        return self._queue.qsize()

    def stop(self) -> None:
        self._stop_event.set()
        for connector in self._connectors:
            connector.stop()
        for connector in self._connectors:
            connector.join()
