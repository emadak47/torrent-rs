use criterion::{black_box, criterion_group, criterion_main, Criterion};
use async_wss::exchanges::binance::flatbuffer::event_factory::make_order_book_snapshot_event as make_binance_snapshot_event;
use async_wss::aggregator::ZenohEvent;
use async_wss::common::CcyPair;
use async_wss::aggregator_v2::AggBook;
use async_wss::aggregator::OrderBookAggregator;
use async_wss::orderbook::l2::Level;
use async_wss::common::FlatbufferEvent;
use async_wss::spsc::SPSCQueue;

fn criterion_benchmark(c: &mut Criterion) {
    let mut agg_book = AggBook::new();
    /********************************* Set-up book *********************************/
    let asks: Vec<Level> = vec![
        Level { price: 40, qty: 100 },
        Level { price: 50, qty: 200 },
        Level { price: 60, qty: 300 },
    ];

    let bids: Vec<Level> = vec![
        Level { price: 10, qty: 100 },
        Level { price: 20, qty: 200 },
        Level { price: 30, qty: 300 },
    ];

    let currency_pair = CcyPair {
        base: String::from("btc"),
        quote: String::from("usd"),
        product: String::from("spot"),
    };  

    let evnt = make_binance_snapshot_event(bids, asks, currency_pair.clone());

    /********************************* Event 1 *********************************/
    
    let zenoh_event = ZenohEvent {
        streamid: 0, // snapshot
        buff: evnt.buff, // flatbuffers
    };    


    let (mut sender_prod, mut reciever_prod) = SPSCQueue::new::<FlatbufferEvent>(20);
    let mut aggregator = OrderBookAggregator::new(sender_prod);

    let asks: Vec<Level> = vec![
        Level { price: 40, qty: 100 },
        Level { price: 50, qty: 200 },
        Level { price: 60, qty: 300 },
    ];

    let bids: Vec<Level> = vec![
        Level { price: 10, qty: 100 },
        Level { price: 20, qty: 200 },
        Level { price: 30, qty: 300 },
    ];

    let currency_pair = CcyPair {
        base: String::from("btc"),
        quote: String::from("usd"),
        product: String::from("spot"),
    };  

    let evnt = make_binance_snapshot_event(bids, asks, currency_pair.clone());

    let zenoh_event = ZenohEvent {
        streamid: 0, // snapshot
        buff: evnt.buff, // flatbuffers
    };   

    c.bench_function("aggregator_v2", |b| b.iter(|| agg_book.snapshot_event(&zenoh_event.buff)));
    c.bench_function("aggregator", |b| b.iter(|| aggregator.run(&zenoh_event)));


}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);