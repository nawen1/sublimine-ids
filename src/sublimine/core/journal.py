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
    DataQualitySnapshot,
    EngineStateEvent,
    EventType,
    QuoteTick,
    SignalEvent,
    Side,
    TradeIntent,
    TradePrint,
    Venue,
)
from sublimine.features.feature_engine import FeatureFrame


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


def _decode_feature_frame(data: dict) -> FeatureFrame:
    return FeatureFrame(
        symbol=data["symbol"],
        venue=Venue(data["venue"]),
        ts_utc=_parse_datetime(data["ts_utc"]),
        depth_near=float(data["depth_near"]),
        microprice_bias=float(data["microprice_bias"]),
        ofi_z=float(data["ofi_z"]),
        delta_size=float(data["delta_size"]),
        price_progress=float(data["price_progress"]),
        replenishment=float(data["replenishment"]),
        sweep_distance=float(data["sweep_distance"]),
        return_speed=float(data["return_speed"]),
        post_sweep_absorption=float(data["post_sweep_absorption"]),
        basis_z=float(data["basis_z"]),
        lead_lag=float(data["lead_lag"]),
        microprice=float(data["microprice"]),
        mid=float(data["mid"]),
    )


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
        payload = _decode_feature_frame(data)
    elif event_type == EventType.TRADE_INTENT:
        payload = TradeIntent(
            symbol=data["symbol"],
            direction=Side(data["direction"]),
            score=float(data["score"]),
            risk_frac=float(data["risk_frac"]),
            entry_plan=dict(data.get("entry_plan", {})),
            stop_plan=dict(data.get("stop_plan", {})),
            ts_utc=_parse_datetime(data["ts_utc"]),
            reason_codes=list(data.get("reason_codes", [])),
            meta=dict(data.get("meta", {})),
        )
    elif event_type == EventType.DATA_QUALITY:
        payload = DataQualitySnapshot(
            ts_utc=_parse_datetime(data["ts_utc"]),
            symbol=data["symbol"],
            per_venue=dict(data.get("per_venue", {})),
            queue_depth=int(data.get("queue_depth", 0)),
            mid_by_venue={key: float(val) for key, val in data.get("mid_by_venue", {}).items()},
            mid_diff_bps=float(data["mid_diff_bps"]) if data.get("mid_diff_bps") is not None else None,
            score_0_1=float(data.get("score_0_1", 0.0)),
            reason_codes=list(data.get("reason_codes", [])),
            meta=dict(data.get("meta", {})),
        )
    elif event_type == EventType.ENGINE_STATE:
        payload = EngineStateEvent(
            ts_utc=_parse_datetime(data["ts_utc"]),
            state=str(data.get("state", "")),
            prev_state=str(data.get("prev_state", "")),
            score_0_1=float(data.get("score_0_1", 0.0)),
            reason_codes=list(data.get("reason_codes", [])),
            meta=dict(data.get("meta", {})),
        )
    else:
        payload = data
    return event_type, payload


class JournalWriter:
    def __init__(self, path: str) -> None:
        self._path = path
        self._handle = open(self._path, "a", encoding="utf-8")

    def append(self, event_type: EventType, payload: Any) -> None:
        record = encode_record(event_type, payload)
        line = json.dumps(record, separators=(",", ":"))
        self._handle.write(line + "\n")
        self._handle.flush()

    def close(self) -> None:
        if self._handle:
            self._handle.close()
            self._handle = None


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
