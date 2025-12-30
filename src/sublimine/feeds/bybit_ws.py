from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sublimine.contracts.types import BookDelta, BookLevel, BookSnapshot, Venue


def _parse_levels(raw_levels: list[list[Any]]) -> list[BookLevel]:
    levels: list[BookLevel] = []
    for price, size in raw_levels:
        levels.append(BookLevel(price=float(price), size=float(size)))
    return levels


def _ts_from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def parse_bybit_message(msg: dict) -> BookSnapshot | BookDelta | None:
    msg_type = msg.get("type")
    data = msg.get("data") or {}
    if msg_type not in {"snapshot", "delta"}:
        return None

    symbol = data.get("s")
    if symbol is None:
        return None

    ts_ms = msg.get("ts")
    if ts_ms is None:
        return None

    bids = _parse_levels(data.get("b", []))
    asks = _parse_levels(data.get("a", []))
    update_id = data.get("u")

    if msg_type == "snapshot":
        depth = int(data.get("depth", max(len(bids), len(asks))))
        return BookSnapshot(
            symbol=symbol,
            venue=Venue.BYBIT,
            ts_utc=_ts_from_ms(int(ts_ms)),
            bids=bids,
            asks=asks,
            depth=depth,
        )

    is_snapshot = bool(update_id == 1)
    return BookDelta(
        symbol=symbol,
        venue=Venue.BYBIT,
        ts_utc=_ts_from_ms(int(ts_ms)),
        bids=bids,
        asks=asks,
        is_snapshot=is_snapshot,
        update_id=int(update_id) if update_id is not None else None,
    )


@dataclass
class BybitConnector:
    def connect(self) -> None:
        raise NotImplementedError("Bybit websocket connection is disabled in phase 1")
