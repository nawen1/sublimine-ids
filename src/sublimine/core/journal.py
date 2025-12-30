from __future__ import annotations

import json
from dataclasses import fields, is_dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Iterable

from sublimine.contracts.types import (
    BookDelta,
    BookLevel,
    BookSnapshot,
    EventType,
    QuoteTick,
    SignalEvent,
    Side,
    TradeIntent,
    TradePrint,
    Venue,
)


def _encode_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if is_dataclass(value):
        return _encode_dataclass(value)
    if isinstance(value, list):
        return [_encode_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _encode_value(val) for key, val in value.items()}
    return value


def _encode_dataclass(obj: Any) -> dict:
    return {field.name: _encode_value(getattr(obj, field.name)) for field in fields(obj)}


def encode_record(event_type: EventType, payload: Any) -> dict:
    return {
        "event_type": event_type.value,
        "data": _encode_value(payload),
    }


def _decode_book_levels(raw: list[dict]) -> list[BookLevel]:
    return [BookLevel(price=float(item["price"]), size=float(item["size"])) for item in raw]


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


def decode_record(record: dict) -> tuple[EventType, Any]:
    event_type = EventType(record["event_type"])
    data = record.get("data", {})
    if event_type == EventType.BOOK_SNAPSHOT:
        payload = BookSnapshot(
            symbol=data["symbol"],
            venue=Venue(data["venue"]),
            ts_utc=_parse_datetime(data["ts_utc"]),
            bids=_decode_book_levels(data.get("bids", [])),
            asks=_decode_book_levels(data.get("asks", [])),
            depth=int(data["depth"]),
        )
    elif event_type == EventType.BOOK_DELTA:
        payload = BookDelta(
            symbol=data["symbol"],
            venue=Venue(data["venue"]),
            ts_utc=_parse_datetime(data["ts_utc"]),
            bids=_decode_book_levels(data.get("bids", [])),
            asks=_decode_book_levels(data.get("asks", [])),
            is_snapshot=bool(data.get("is_snapshot")),
            update_id=data.get("update_id"),
        )
    elif event_type == EventType.TRADE:
        payload = TradePrint(
            symbol=data["symbol"],
            venue=Venue(data["venue"]),
            ts_utc=_parse_datetime(data["ts_utc"]),
            price=float(data["price"]),
            size=float(data["size"]),
            aggressor_side=Side(data["aggressor_side"]),
        )
    elif event_type == EventType.QUOTE:
        payload = QuoteTick(
            symbol=data["symbol"],
            venue=Venue(data["venue"]),
            ts_utc=_parse_datetime(data["ts_utc"]),
            bid=float(data["bid"]),
            ask=float(data["ask"]),
            last=float(data["last"]),
        )
    elif event_type == EventType.EVENT_SIGNAL:
        payload = SignalEvent(
            event_name=data["event_name"],
            symbol=data["symbol"],
            venue=Venue(data["venue"]),
            ts_utc=_parse_datetime(data["ts_utc"]),
            score_0_1=float(data["score_0_1"]),
            reason_codes=list(data.get("reason_codes", [])),
            meta=dict(data.get("meta", {})),
        )
    elif event_type == EventType.FEATURE:
        payload = data
    else:
        payload = data
    return event_type, payload


class JournalWriter:
    def __init__(self, path: str) -> None:
        self._path = path

    def append(self, event_type: EventType, payload: Any) -> None:
        record = encode_record(event_type, payload)
        line = json.dumps(record, separators=(",", ":"))
        with open(self._path, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def iter_records(path: str) -> Iterable[dict]:
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def iter_events(path: str) -> Iterable[tuple[EventType, Any]]:
    for record in iter_records(path):
        yield decode_record(record)
