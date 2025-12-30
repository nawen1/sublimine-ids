from datetime import datetime, timezone

from sublimine.contracts.types import BookSnapshot, BookLevel, Venue
from sublimine.feeds.binance_ws import BinanceBookSynchronizer, parse_binance_diff_event


def test_binance_snapshot_and_diff_sync():
    sync = BinanceBookSynchronizer(symbol="BTCUSDT", depth=2)

    buffered_msg = {
        "e": "depthUpdate",
        "E": 1700000000000,
        "s": "BTCUSDT",
        "U": 90,
        "u": 95,
        "b": [["100", "2"]],
        "a": [["101", "1"]],
    }
    buffered_event = parse_binance_diff_event(buffered_msg)
    sync.on_diff_event(buffered_event)

    snapshot = BookSnapshot(
        symbol="BTCUSDT",
        venue=Venue.BINANCE,
        ts_utc=datetime(2023, 1, 1, tzinfo=timezone.utc),
        bids=[BookLevel(100.0, 1.0), BookLevel(99.0, 1.0), BookLevel(98.0, 1.0)],
        asks=[BookLevel(101.0, 1.0), BookLevel(102.0, 1.0), BookLevel(103.0, 1.0)],
        depth=2,
    )
    sync.apply_snapshot(snapshot, last_update_id=100)
    assert sync.last_update_id == 100
    assert len(sync.book.bids) == 2
    assert len(sync.book.asks) == 2

    event_msg = {
        "e": "depthUpdate",
        "E": 1700000001000,
        "s": "BTCUSDT",
        "U": 95,
        "u": 105,
        "b": [["100.5", "2"], ["98", "1"]],
        "a": [["100.8", "1"], ["103", "1"]],
    }
    event = parse_binance_diff_event(event_msg)
    sync.on_diff_event(event)
    assert sync.last_update_id == 105
    assert sync.book.best_bid().price == 100.5
    assert sync.book.best_ask().price == 100.8
    assert len(sync.book.bids) == 2
    assert len(sync.book.asks) == 2

    event_msg2 = {
        "e": "depthUpdate",
        "E": 1700000002000,
        "s": "BTCUSDT",
        "U": 106,
        "u": 110,
        "b": [["100", "2"]],
        "a": [["100.8", "0"], ["104", "1"]],
    }
    event2 = parse_binance_diff_event(event_msg2)
    sync.on_diff_event(event2)
    assert sync.last_update_id == 110
    assert sync.book.best_ask().price == 101.0
    assert len(sync.book.asks) == 2


def test_binance_gap_sets_desync():
    sync = BinanceBookSynchronizer(symbol="BTCUSDT", depth=5)
    snapshot = BookSnapshot(
        symbol="BTCUSDT",
        venue=Venue.BINANCE,
        ts_utc=datetime(2023, 1, 1, tzinfo=timezone.utc),
        bids=[BookLevel(100.0, 1.0)],
        asks=[BookLevel(101.0, 1.0)],
        depth=5,
    )
    sync.apply_snapshot(snapshot, last_update_id=100)

    gap_msg = {
        "e": "depthUpdate",
        "E": 1700000003000,
        "s": "BTCUSDT",
        "U": 200,
        "u": 205,
        "b": [["100", "2"]],
        "a": [["101", "1"]],
    }
    gap_event = parse_binance_diff_event(gap_msg)
    sync.on_diff_event(gap_event)
    assert sync.desynced is True
    assert sync.needs_resync() is True
    sync.reset_for_resync()
    assert sync.needs_resync() is False
    assert sync.last_update_id is None


def test_binance_reset_clears_buffer():
    sync = BinanceBookSynchronizer(symbol="BTCUSDT", depth=2)
    buffered_msg = {
        "e": "depthUpdate",
        "E": 1700000000000,
        "s": "BTCUSDT",
        "U": 95,
        "u": 105,
        "b": [["101", "2"]],
        "a": [["102", "1"]],
    }
    buffered_event = parse_binance_diff_event(buffered_msg)
    sync.on_diff_event(buffered_event)

    sync.reset_for_resync()
    assert sync.last_update_id is None
    assert sync.needs_resync() is False

    snapshot = BookSnapshot(
        symbol="BTCUSDT",
        venue=Venue.BINANCE,
        ts_utc=datetime(2023, 1, 1, tzinfo=timezone.utc),
        bids=[BookLevel(100.0, 1.0)],
        asks=[BookLevel(101.0, 1.0)],
        depth=2,
    )
    sync.apply_snapshot(snapshot, last_update_id=100)
    assert sync.book.best_bid().price == 100.0
