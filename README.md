# torrent-rs

## Run

- LOG-LEVELS: `TRACE`, `DEBUG`, `WARN`, `ERROR`, `INFO`
- Supported Exchanges: `binance`, `okx`

* For each supported exchange, open a new terminal window:

```
RUST_LOG=async_wss=<LOG-LEVEL>,<SUPPORTED-EXCHANGE>=<LOG-LEVEL> cargo r --bin <SUPPORTED-EXCHANGE>
e.x: RUST_LOG=async_wss=trace,binance=trace cargo r --bin binance
```

- Open a new terminal window:

```
RUST_LOG=async_wss=<LOG-LEVEL> cargo r --bin aggregator
```

## Benchmarks

- Note: the below results are operations done on one book (`btc-usdt-spot`) where in each scenario 100 samples are collected and the top three results are shown.

- To run:

```
cargo bench -- --nocapture
```

- Scenario 1: Snapshot 200 levels from binance into empty aggregator

```
time:   [77.294 µs 77.447 µs 77.569 µs]
change: [-3.4171% -2.5660% -1.7092%] (p = 0.00 < 0.05)
```

- Scenario 2: Snapshot 200 new levels from binance into book

```
time:   [79.391 µs 79.464 µs 79.535 µs]
change: [-7.9154% -7.2160% -6.5660%] (p = 0.00 < 0.05)
```

- Scenario 3: Snapshot 180 levels from Okx into book

```
time:   [87.742 µs 88.022 µs 88.320 µs]
change: [+917.85% +929.23% +941.06%] (p = 0.00 < 0.05)
```

- Scenario 4: Update 50 levels from Okx into book

```
time:   [8.4538 µs 8.5156 µs 8.6014 µs]
```

- Scenario 5: Update 70 levels from binance into book

```
time:   [14.939 µs 14.977 µs 15.023 µs]
```
