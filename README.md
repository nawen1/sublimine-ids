# SUBLIMINE IDS v2.1 (Shadow/Replay)

Production-grade repo skeleton for an event-driven trading engine built around microstructure events (E1..E4) and deterministic replay.

## Why leader markets for L2
CFDs do not expose a real limit order book. We therefore ingest real L2 + trades from market leaders (Bybit/Binance BTCUSDT) and use that microstructure to decide. Execution later routes to MT5 CFDs, but decision data always comes from leader markets.

## Setup

- Python 3.11+
- Install deps: `make install`

## Run (shadow + replay)

```
python -m sublimine.run --mode shadow --config config/sublimine.yaml --replay tests/data/replay.jsonl
```

## Tests

```
pytest -q
```

## Architecture (phase 1)

- `contracts/`: strict dataclasses/enums
- `feeds/`: Bybit/Binance parsing + order book apply
- `features/`: microprice, OFI, VPIN, spoof, iceberg, basis
- `events/`: rolling-quantile detectors (E1..E4)
- `core/`: event bus, replay, journal
- `strategy/`: BTC playbook skeleton
- `risk/`: phase ladder + gates
- `exec/`: MT5 adapter stub + router (shadow)

## Bybit/Binance book logic

- **Bybit**: snapshot and delta parsing, size=0 removes level, `u==1` forces snapshot overwrite.
- **Binance**: diff-depth sync algorithm (buffer until snapshot, drop `u < lastUpdateId`, first apply requires `U <= lastUpdateId <= u`, enforce continuity; gaps set desync).

## Event reason codes (initial)

- `depth_near_low`, `ofi_z_high`, `microprice_bias_high`
- `delta_high`, `price_progress_low`, `replenishment_high`
- `sweep_distance_high`, `return_speed_high`, `post_sweep_absorption_high`
- `basis_z_extreme`, `lead_lag_high`

## Next steps

- Live Bybit/Binance websockets + journaling for real-time shadow mode.
- IBKR L2 for NQ/GC futures and MT5 execution adapter implementation.
- Expand playbooks and risk gates after live replay validation.
