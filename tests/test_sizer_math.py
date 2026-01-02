from __future__ import annotations

import pytest

from sublimine.exec.oms import size_lots


def test_size_lots_long_short_symmetry() -> None:
    args = dict(
        equity=10_000.0,
        risk_frac=0.01,
        tick_size=1.0,
        tick_value_per_lot=1.0,
        vol_min=0.1,
        vol_step=0.1,
    )
    long_lots = size_lots(entry_price=100.0, stop_price=95.0, **args)
    short_lots = size_lots(entry_price=100.0, stop_price=105.0, **args)
    assert long_lots == pytest.approx(short_lots)


def test_size_lots_rounds_down_to_step() -> None:
    lots = size_lots(
        equity=1000.0,
        risk_frac=0.01,
        entry_price=100.0,
        stop_price=99.3,
        tick_size=0.1,
        tick_value_per_lot=1.0,
        vol_min=0.0,
        vol_step=0.1,
    )
    assert lots == pytest.approx(1.4)


def test_size_lots_clamps_to_min() -> None:
    lots = size_lots(
        equity=1000.0,
        risk_frac=0.001,
        entry_price=100.0,
        stop_price=99.0,
        tick_size=1.0,
        tick_value_per_lot=1.0,
        vol_min=2.0,
        vol_step=1.0,
    )
    assert lots == pytest.approx(2.0)
