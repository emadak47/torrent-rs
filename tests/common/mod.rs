use async_wss::{
    aggregator::Aggregator,
    common::{scale, CcyPair, Exchange, FlatbufferEvent},
    flatbuffer::event_factory::{make_snapshot_event, make_update_event},
    orderbook::l2::Level,
    spsc::SPSCQueue,
};

pub static ASSET_CONSTANT_MULTIPLIER: f64 = 1e10;

pub enum Index {
    Standard,
    ReSnaphsot,
    ReUpdate,
}

pub enum Type {
    Snapshot,
    Update,
}

pub enum Source {
    Binance(Type),
    Okx(Type),
}

pub fn setup_levels(idx: Index) -> (Vec<Level>, Vec<Level>) {
    let bids;
    let asks;
    match idx {
        Index::Standard => {
            asks = vec![1, 3, 5]
                .into_iter()
                .map(|x| {
                    let num = scale(&*x.to_string()).unwrap();
                    Level {
                        price: num,
                        qty: num * 10,
                    }
                })
                .collect::<Vec<Level>>();
            bids = vec![2, 4, 6]
                .into_iter()
                .map(|x| {
                    let num = scale(&*x.to_string()).unwrap();
                    Level {
                        price: num,
                        qty: num * 10,
                    }
                })
                .collect::<Vec<Level>>();
        }
        Index::ReSnaphsot => {
            asks = vec![5, 7, 9, 11]
                .into_iter()
                .map(|x| {
                    let num = scale(&*x.to_string()).unwrap();
                    Level {
                        price: num,
                        qty: num * 10,
                    }
                })
                .collect::<Vec<Level>>();
            bids = vec![2, 8, 10, 12]
                .into_iter()
                .map(|x| {
                    let num = scale(&*x.to_string()).unwrap();
                    Level {
                        price: num,
                        qty: num * 10,
                    }
                })
                .collect::<Vec<Level>>();
        }
        Index::ReUpdate => {
            asks = vec![1, 3, 5, 7, 9]
                .into_iter()
                .map(|x| {
                    let num = scale(&*x.to_string()).unwrap();
                    if x != 5 {
                        Level {
                            price: num,
                            qty: num * 100,
                        }
                    } else {
                        Level { price: num, qty: 0 }
                    }
                })
                .collect::<Vec<Level>>();
            bids = vec![2, 4, 6, 8, 10]
                .into_iter()
                .map(|x| {
                    let num = scale(&*x.to_string()).unwrap();
                    if x != 6 {
                        Level {
                            price: num,
                            qty: num * 100,
                        }
                    } else {
                        Level { price: num, qty: 0 }
                    }
                })
                .collect::<Vec<Level>>();
        }
    }
    (bids, asks)
}

pub fn setup_aggregator() -> Aggregator {
    let (tx, _) = SPSCQueue::new::<FlatbufferEvent>(20);
    Aggregator::new(tx)
}

pub fn setup_flatbuffer(idx: Index, src: Source) -> FlatbufferEvent {
    let (bids, asks) = setup_levels(idx);

    let ccy_pair = CcyPair {
        base: String::from("btc"),
        quote: String::from("usdt"),
        product: String::from("spot"),
    };

    match src {
        Source::Binance(t) => match t {
            Type::Snapshot => make_snapshot_event(bids, asks, ccy_pair, Exchange::Binance).unwrap(),
            Type::Update => make_update_event(bids, asks, ccy_pair, Exchange::Binance).unwrap(),
        },
        Source::Okx(t) => match t {
            Type::Snapshot => make_snapshot_event(bids, asks, ccy_pair, Exchange::Okx).unwrap(),
            Type::Update => make_update_event(bids, asks, ccy_pair, Exchange::Okx).unwrap(),
        },
    }
}

pub fn setup(idx: Index, src: Source) -> (FlatbufferEvent, Aggregator) {
    let aggregator = setup_aggregator();
    let event = setup_flatbuffer(idx, src);

    (event, aggregator)
}
