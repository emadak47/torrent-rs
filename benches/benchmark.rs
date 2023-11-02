use criterion::{black_box, criterion_group, criterion_main, Criterion};
use async_wss::exchanges::binance::flatbuffer::event_factory::make_order_book_snapshot_event as make_binance_snapshot_event;
use async_wss::exchanges::okx::flatbuffer::event_factory::make_order_book_snapshot_event as make_okx_snapshot_event;
use async_wss::exchanges::okx::flatbuffer::event_factory::make_order_book_update_event as make_okx_update_event;
use async_wss::aggregator::ZenohEvent;
use async_wss::common::CcyPair;
use async_wss::aggregator_v2::AggBook;
use async_wss::aggregator::OrderBookAggregator;
use async_wss::orderbook::l2::Level;
use async_wss::common::FlatbufferEvent;
use async_wss::spsc::SPSCQueue;
use rand::Rng;

fn criterion_benchmark(c: &mut Criterion) {
    let (mut sender_prod, mut reciever_prod) = SPSCQueue::new::<FlatbufferEvent>(2000000);
    
    // aggregator v2
    let mut agg_book = AggBook::new();
    // aggregator
    let mut aggregator = OrderBookAggregator::new(sender_prod);
    
    // ************ Setup Data ************

    let mut asks = Vec::new();
    let mut bids = Vec::new();
    let num_levels = 55;

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

    /*
    Scenario 1 : 
        1. aggregator v2 and aggregator inserting snapshot to aggregator book as first exchange
    */

    let evnt = make_binance_snapshot_event(bids.clone(), asks.clone(), currency_pair.clone());
    
    let zenoh_event = ZenohEvent {
        streamid: 0, // binance snapshot
        buff: evnt.buff, // flatbuffers
    };    

    // benchmark for both aggregator and aggregator_v2 for their first snapshot event from any exchange
    c.bench_function("aggregator_v2_scenario_1", |b| b.iter(|| agg_book.snapshot_event(&zenoh_event.buff)));
    c.bench_function("aggregator_scenario_1", |b| b.iter(|| aggregator.run(&zenoh_event)));

    /*
    Scenario 2 : 
        1. Aggregator and Aggregator_v2 with binance 42 levels each
        2. Okx snapshot event comes in with 50 levels 
        3. Aggregator_v2 only iterates thru 50 levels to reset the okx book
        3. Aggregator iterates thru all 42+50 levels to reset the okx book 
    */

    let evnt = make_binance_snapshot_event(bids.clone(), asks.clone(), currency_pair.clone());

    let zenoh_event = ZenohEvent {
        streamid: 0, // binance snapshot
        buff: evnt.buff, // flatbuffers
    };   

    // fills the aggregator_v2 book with binance 42 levels bids and asks each
    agg_book.snapshot_event(&zenoh_event.buff);   
    // fills the aggregator book with binance 42 levels bids and asks each
    aggregator.run(&zenoh_event);

    asks.clear();
    bids.clear();

    for _ in 0..50 { // average levels in okx
        let price = rng.gen_range(1..10000);
        let qty = rng.gen_range(1..1000);

        asks.push(Level { price, qty });
        bids.push(Level { price, qty });
    }

    let evnt = make_okx_snapshot_event(bids.clone(), asks.clone(), currency_pair.clone());

    let zenoh_event = ZenohEvent {
        streamid: 0, // okx snapshot
        buff: evnt.buff, // flatbuffers
    };   

    // run the benchmark for aggregator and aggregator v2 for new okx snapshot event
    c.bench_function("aggregator_v2_scenario_2", |b| b.iter(|| agg_book.snapshot_event(&zenoh_event.buff)));
    c.bench_function("aggregator_scenario_2", |b| b.iter(|| aggregator.run(&zenoh_event)));

    /*
    Scenario 3 : 
        1. Aggregator and Aggregator_v2 for update events
        2. Average length of update bids and asks in binance and okx is 50 
    */

    let evnt = make_okx_update_event(bids.clone(), asks.clone(), currency_pair.clone());

    let zenoh_event = ZenohEvent {
        streamid: 1, // okx snapshot
        buff: evnt.buff, // flatbuffers
    };   

    // Run the aggregator and aggregator v2 benchmark for update event
    c.bench_function("aggregator_v2_scenario_3", |b| b.iter(|| agg_book.update_event(&zenoh_event.buff)));
    c.bench_function("aggregator_scenario_3", |b| b.iter(|| aggregator.run(&zenoh_event)));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);