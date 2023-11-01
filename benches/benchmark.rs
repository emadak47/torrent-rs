use criterion::{black_box, criterion_group, criterion_main, Criterion};
use async_wss::exchanges::binance::flatbuffer::event_factory::make_order_book_snapshot_event as make_binance_snapshot_event;
use async_wss::exchanges::okx::flatbuffer::event_factory::make_order_book_snapshot_event as make_okx_snapshot_event;
use async_wss::aggregator::ZenohEvent;
use async_wss::common::CcyPair;
use async_wss::aggregator_v2::AggBook;
use async_wss::aggregator::OrderBookAggregator;
use async_wss::orderbook::l2::Level;
use async_wss::common::FlatbufferEvent;
use async_wss::spsc::SPSCQueue;

fn criterion_benchmark(c: &mut Criterion) {
    let (mut sender_prod, mut reciever_prod) = SPSCQueue::new::<FlatbufferEvent>(20);
    
    // aggregator v2
    let mut agg_book = AggBook::new();
    // aggregator
    let mut aggregator = OrderBookAggregator::new(sender_prod);
    
    /*
    Scenario 1 : 
        1. aggregator v2 and aggregator inserting snapshot to aggregator book as first exchange
    */

    let asks: Vec<Level> = vec![
        Level { price: 10, qty: 100 },
        Level { price: 20, qty: 200 },
        Level { price: 60, qty: 300 },
        Level { price: 601, qty: 300 },
        Level { price: 602, qty: 300 },
        Level { price: 603, qty: 300 },
        Level { price: 604, qty: 300 },
        Level { price: 605, qty: 300 },
        Level { price: 606, qty: 300 },
        Level { price: 607, qty: 300 },
        Level { price: 608, qty: 300 },

    ];

    let bids: Vec<Level> = vec![
        Level { price: 10, qty: 100 },
        Level { price: 20, qty: 200 },
        Level { price: 60, qty: 300 },
        Level { price: 601, qty: 300 },
        Level { price: 602, qty: 300 },
        Level { price: 603, qty: 300 },
        Level { price: 604, qty: 300 },
        Level { price: 605, qty: 300 },
        Level { price: 606, qty: 300 },
        Level { price: 607, qty: 300 },
        Level { price: 608, qty: 300 },
    ];

    let currency_pair = CcyPair {
        base: String::from("btc"),
        quote: String::from("usd"),
        product: String::from("spot"),
    };  

    let evnt = make_binance_snapshot_event(bids, asks, currency_pair.clone());

    /********************************* Event 1 *********************************/
    
    let zenoh_event = ZenohEvent {
        streamid: 0, // binance snapshot
        buff: evnt.buff, // flatbuffers
    };    

    // benchmark for both aggregator and aggregator_v2 for their first snapshot event from any exchange
    c.bench_function("aggregator_v2", |b| b.iter(|| agg_book.snapshot_event(&zenoh_event.buff)));
    c.bench_function("aggregator", |b| b.iter(|| aggregator.run(&zenoh_event)));

    /*
    Scenario 2 : 
        1. Aggregator and Aggregator_v2 with binance 42 levels each
        2. Okx snapshot event comes in with 9 levels 
        3. Aggregator_v2 only iterates thru 9 levels to reset the okx book
        3. Aggregator iterates thru all 42 + 9 levels to reset the book 
    */
    
    let asks: Vec<Level> = vec![
        Level { price: 10, qty: 100 },
        Level { price: 20, qty: 200 },
        Level { price: 60, qty: 300 },
        Level { price: 601, qty: 300 },
        Level { price: 602, qty: 300 },
        Level { price: 603, qty: 300 },
        Level { price: 604, qty: 300 },
        Level { price: 605, qty: 300 },
        Level { price: 606, qty: 300 },
        Level { price: 607, qty: 300 },
        Level { price: 608, qty: 300 },
        Level { price: 609, qty: 300 },
        Level { price: 606, qty: 300 },
        Level { price: 605, qty: 300 },
        Level { price: 604, qty: 300 },
        Level { price: 602, qty: 300 },
        Level { price: 789, qty: 300 },
        Level { price: 654, qty: 300 },
        Level { price: 321, qty: 300 },
        Level { price: 987, qty: 300 },
        Level { price: 845, qty: 300 },
        Level { price: 986, qty: 300 },
        Level { price: 321, qty: 300 },
        Level { price: 741, qty: 300 },
        Level { price: 741, qty: 300 },
        Level { price: 741, qty: 300 },
        Level { price: 951, qty: 300 },
        Level { price: 231, qty: 300 },
        Level { price: 78, qty: 300 },
        Level { price: 2, qty: 300 },
        Level { price: 3, qty: 300 },
        Level { price: 65, qty: 300 },
        Level { price: 7, qty: 300 },
        Level { price: 96, qty: 300 },
        Level { price: 32, qty: 300 },
        Level { price: 4, qty: 300 },
        Level { price: 98, qty: 300 },
        Level { price: 45, qty: 300 },
        Level { price: 32, qty: 300 },
        Level { price: 9874, qty: 300 },
        Level { price: 9654, qty: 300 },
        Level { price: 3214, qty: 300 },
        Level { price: 9654, qty: 300 },
    ];

    let bids: Vec<Level> = vec![
        Level { price: 10, qty: 100 },
        Level { price: 20, qty: 200 },
        Level { price: 60, qty: 300 },
        Level { price: 601, qty: 300 },
        Level { price: 602, qty: 300 },
        Level { price: 603, qty: 300 },
        Level { price: 604, qty: 300 },
        Level { price: 605, qty: 300 },
        Level { price: 606, qty: 300 },
        Level { price: 607, qty: 300 },
        Level { price: 608, qty: 300 },
        Level { price: 609, qty: 300 },
        Level { price: 606, qty: 300 },
        Level { price: 605, qty: 300 },
        Level { price: 604, qty: 300 },
        Level { price: 602, qty: 300 },
        Level { price: 789, qty: 300 },
        Level { price: 654, qty: 300 },
        Level { price: 321, qty: 300 },
        Level { price: 987, qty: 300 },
        Level { price: 845, qty: 300 },
        Level { price: 986, qty: 300 },
        Level { price: 321, qty: 300 },
        Level { price: 741, qty: 300 },
        Level { price: 741, qty: 300 },
        Level { price: 741, qty: 300 },
        Level { price: 951, qty: 300 },
        Level { price: 231, qty: 300 },
        Level { price: 78, qty: 300 },
        Level { price: 2, qty: 300 },
        Level { price: 3, qty: 300 },
        Level { price: 65, qty: 300 },
        Level { price: 7, qty: 300 },
        Level { price: 96, qty: 300 },
        Level { price: 32, qty: 300 },
        Level { price: 4, qty: 300 },
        Level { price: 98, qty: 300 },
        Level { price: 45, qty: 300 },
        Level { price: 32, qty: 300 },
        Level { price: 9874, qty: 300 },
        Level { price: 9654, qty: 300 },
        Level { price: 3214, qty: 300 },
        Level { price: 9654, qty: 300 },
    ];


    let evnt = make_binance_snapshot_event(bids, asks, currency_pair.clone());

    let zenoh_event = ZenohEvent {
        streamid: 0, // okx snapshot
        buff: evnt.buff, // flatbuffers
    };   

    // fills the aggregator_v2 book with binance 42 levels bids and asks each
    agg_book.snapshot_event(&zenoh_event.buff);   
    // fills the aggregator book with binance 42 levels bids and asks each
    aggregator.run(&zenoh_event);

    let asks: Vec<Level> = vec![
        Level { price: 11, qty: 100 },
        Level { price: 22, qty: 200 },
        Level { price: 66, qty: 300 },
        Level { price: 601, qty: 300 },
        Level { price: 602, qty: 300 },
        Level { price: 9874, qty: 300 },
        Level { price: 9654, qty: 300 },
        Level { price: 3214, qty: 300 },
        Level { price: 9654, qty: 300 },
    ];

    let bids: Vec<Level> = vec![
        Level { price: 11, qty: 100 },
        Level { price: 22, qty: 200 },
        Level { price: 66, qty: 300 },
        Level { price: 601, qty: 300 },
        Level { price: 602, qty: 300 },
        Level { price: 9874, qty: 300 },
        Level { price: 9654, qty: 300 },
        Level { price: 3214, qty: 300 },
        Level { price: 9654, qty: 300 },
    ];

    let evnt = make_okx_snapshot_event(bids, asks, currency_pair.clone());

    let zenoh_event = ZenohEvent {
        streamid: 0, // okx snapshot
        buff: evnt.buff, // flatbuffers
    };   

    // run the benchmark for aggregator and aggregator v2 for new okx snapshot event
    c.bench_function("aggregator_v2_test", |b| b.iter(|| agg_book.snapshot_event(&zenoh_event.buff)));
    c.bench_function("aggregator_test", |b| b.iter(|| aggregator.run(&zenoh_event)));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);