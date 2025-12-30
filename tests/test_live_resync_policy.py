from datetime import datetime, timezone

from sublimine.contracts.types import BookLevel, BookSnapshot, Venue
from sublimine.feeds.binance_ws import BinanceBookSynchronizer, parse_binance_diff_event
from sublimine.feeds.ws_common import ReconnectPolicy


def test_reconnect_policy_backoff_and_reset():
    policy = ReconnectPolicy(base_delay=1.0, max_delay=5.0, factor=2.0)
    assert policy.next_delay() == 1.0
    assert policy.next_delay() == 2.0
    assert policy.next_delay() == 4.0
    assert policy.next_delay() == 5.0
    policy.reset()
    assert policy.next_delay() == 1.0


def test_binance_snapshot_applies_buffered_deltas():
    sync = BinanceBookSynchronizer(symbol="BTCUSDT", depth=2)
    buffered_msg = {
        "e": "depthUpdate",
        "E": 1700000000000,
        "s": "BTCUSDT",
        "U": 95,
        "u": 105,
        "b": [["100", "2"]],
        "a": [["101", "1"]],
    }
    buffered_event = parse_binance_diff_event(buffered_msg)
    sync.on_diff_event(buffered_event)

    snapshot = BookSnapshot(
        symbol="BTCUSDT",
        venue=Venue.BINANCE,
        ts_utc=datetime(2023, 1, 1, tzinfo=timezone.utc),
        bids=[BookLevel(100.0, 1.0), BookLevel(99.0, 1.0)],
        asks=[BookLevel(101.0, 1.0), BookLevel(102.0, 1.0)],
        depth=2,
    )
    applied = sync.apply_snapshot(snapshot, last_update_id=100)
    assert len(applied) == 1
    assert applied[0].update_id == 105
    assert sync.last_update_id == 105
