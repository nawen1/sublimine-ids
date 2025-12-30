from datetime import datetime, timezone

from sublimine.contracts.types import BookLevel, BookSnapshot, Venue
from sublimine.features.book_features import compute_book_features
from sublimine.features.ofi import OFIState
from sublimine.feeds.book import OrderBook


def test_microprice_and_bias():
    snapshot = BookSnapshot(
        symbol="BTCUSDT",
        venue=Venue.BYBIT,
        ts_utc=datetime(2023, 1, 1, tzinfo=timezone.utc),
        bids=[BookLevel(100.0, 2.0)],
        asks=[BookLevel(101.0, 1.0)],
        depth=1,
    )
    book = OrderBook.empty("BTCUSDT", Venue.BYBIT, depth=1)
    book.apply_snapshot(snapshot)

    features = compute_book_features(book, depth_k=1, ts_utc=snapshot.ts_utc)
    assert features is not None
    expected_microprice = (100.0 * 1.0 + 101.0 * 2.0) / 3.0
    assert abs(features.microprice - expected_microprice) < 1e-9
    assert features.microprice_bias > 0


def test_ofi_state():
    ofi = OFIState(window=5)
    bid1 = BookLevel(100.0, 2.0)
    ask1 = BookLevel(101.0, 1.0)
    assert ofi.update(bid1, ask1) == (0.0, 0.0)

    bid2 = BookLevel(100.0, 3.0)
    ask2 = BookLevel(101.0, 1.0)
    ofi_value, _ = ofi.update(bid2, ask2)
    assert ofi_value == 1.0
