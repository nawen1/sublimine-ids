from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class RVEState(str, Enum):
    INIT = "INIT"
    WARMING = "WARMING"
    ACTIVE = "ACTIVE"
    HALT = "HALT"


@dataclass
class RVEStateMachine:
    state: RVEState = RVEState.INIT

    def on_event(self, event_type: str) -> None:
        if self.state == RVEState.INIT and event_type == "BOOK_SNAPSHOT":
            self.state = RVEState.WARMING
        elif self.state == RVEState.WARMING and event_type == "FEATURE":
            self.state = RVEState.ACTIVE
        elif event_type == "HALT":
            self.state = RVEState.HALT
