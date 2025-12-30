from sublimine.contracts.types import Venue
from sublimine.feeds.book import OrderBook
from sublimine.feeds.bybit_ws import parse_bybit_message


def test_bybit_snapshot_and_delta_apply():
    snapshot_msg = {
        "topic": "orderbook.50.BTCUSDT",
        "type": "snapshot",
        "ts": 1700000000000,
        "data": {
            "s": "BTCUSDT",
            "b": [["100", "1"], ["99", "2"], ["98", "1"]],
            "a": [["101", "1.5"], ["102", "1"], ["103", "1"]],
            "u": 1,
            "depth": 2,
        },
    }

    snapshot = parse_bybit_message(snapshot_msg)
    assert snapshot is not None
    book = OrderBook.empty("BTCUSDT", Venue.BYBIT, depth=2)
    book.apply_snapshot(snapshot)

    assert book.best_bid().price == 100.0
    assert book.best_ask().price == 101.0
    assert len(book.bids) == 2
    assert len(book.asks) == 2

    delta_msg = {
        "topic": "orderbook.50.BTCUSDT",
        "type": "delta",
        "ts": 1700000001000,
        "data": {
            "s": "BTCUSDT",
            "b": [["100.5", "1"], ["98", "3"]],
            "a": [["100.8", "1"], ["103", "2"]],
            "u": 2,
        },
    }

    delta = parse_bybit_message(delta_msg)
    assert delta is not None
    book.apply_delta(delta)

    assert book.best_bid().price == 100.5
    assert book.best_ask().price == 100.8
    assert len(book.bids) == 2
    assert len(book.asks) == 2


def test_bybit_u_equals_one_forces_snapshot():
    delta_msg = {
        "topic": "orderbook.50.BTCUSDT",
        "type": "delta",
        "ts": 1700000002000,
        "data": {"s": "BTCUSDT", "b": [["100", "1"]], "a": [["101", "1"]], "u": 1},
    }

    delta = parse_bybit_message(delta_msg)
    assert delta is not None
    assert delta.is_snapshot is True
