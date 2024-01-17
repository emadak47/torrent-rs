use async_wss::aggregator::{Aggregator, ZenohEvent};
use async_wss::flatbuffer::event_factory::{make_snapshot_event, make_update_event};
use async_wss::orderbook::l2::Level;
use async_wss::spsc::SPSCQueue;
use async_wss::utils::{CcyPair, Exchange, FlatbufferEvent};
use criterion::{criterion_group, criterion_main, Criterion};
use rand::Rng;

fn setup_data(num_levels: i32) -> (Vec<Level>, Vec<Level>, CcyPair) {
    // Setup data
    let mut asks = Vec::new();
    let mut bids = Vec::new();
    let mut rng = rand::thread_rng();

    for _ in 0..num_levels {
        let price = rng.gen_range(1..10000);
        let qty = rng.gen_range(1..1000);

        asks.push(Level { price, qty });
        bids.push(Level { price, qty });
    }

    let currency_pair = CcyPair {
        base: String::from("btc"),
        quote: String::from("usd"),
        product: String::from("spot"),
    };

    (bids, asks, currency_pair)
}

fn criterion_benchmark(c: &mut Criterion) {
    let (tx, _) = SPSCQueue::new::<FlatbufferEvent>(2000000);

    // Setup aggregator
    let mut aggregator = Aggregator::new(tx);

    /*
    Scenario 1 : aggregator inserting snapshot from binance with 200 levels
    */
    let (bids, asks, currency_pair) = setup_data(200);
    let event = make_snapshot_event(bids, asks, currency_pair, Exchange::BINANCE).unwrap();
    let zenoh_event = ZenohEvent {
        stream_id: 0,
        buff: event.buff,
    };

    c.bench_function("Scenario 1", |b| {
        b.iter(|| aggregator.process(zenoh_event.clone()))
    });

    /*
    Scenario 2:
        1. Aggregator with binance 200 levels already
        2. Binance snapshot event comes in with 200 levels
    */
    let (bids, asks, currency_pair) = setup_data(200);
    let event = make_snapshot_event(bids, asks, currency_pair, Exchange::BINANCE).unwrap();
    let zenoh_event = ZenohEvent {
        stream_id: 0,
        buff: event.buff,
    };

    c.bench_function("Scenario 2", |b| {
        b.iter(|| aggregator.process(zenoh_event.clone()))
    });

    /*
    Scenario 3:
        1. aggregator with binance 200 levels
        2. OKX snapshot event comes in with 180 levels
    */
    let (bids, asks, currency_pair) = setup_data(180);
    let event = make_snapshot_event(bids, asks, currency_pair, Exchange::BINANCE).unwrap();
    let zenoh_event = ZenohEvent {
        stream_id: 0,
        buff: event.buff,
    };

    c.bench_function("Scenario 3", |b| {
        b.iter(|| aggregator.process(zenoh_event.clone()))
    });

    /*
    Scenario 4: update events from Okx of 50 levels
    */
    let (bids, asks, currency_pair) = setup_data(50);
    let event = make_update_event(bids, asks, currency_pair, Exchange::BINANCE).unwrap();
    let zenoh_event = ZenohEvent {
        stream_id: 1,
        buff: event.buff,
    };

    c.bench_function("Scenario 4", |b| {
        b.iter(|| aggregator.process(zenoh_event.clone()))
    });

    /*
    Scenario 5: update events from binance of 70 levels
    */
    let (bids, asks, currency_pair) = setup_data(70);
    let event = make_update_event(bids, asks, currency_pair, Exchange::BINANCE).unwrap();
    let zenoh_event = ZenohEvent {
        stream_id: 1,
        buff: event.buff,
    };

    c.bench_function("Scenario 5", |b| {
        b.iter(|| aggregator.process(zenoh_event.clone()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
