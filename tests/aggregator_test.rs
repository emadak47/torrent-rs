use async_wss::{aggregator::ZenohEvent, utils::ASSET_CONSTANT_MULTIPLIER};

mod common;
use common::{scale, setup, setup_aggregator, setup_flatbuffer, Index, Source, Type};

#[test]
fn test_unknown_event() {
    let (event, mut aggregator) = setup(Index::Standard, Source::Binance(Type::Snapshot));
    let zenoh_event = ZenohEvent {
        stream_id: 3, // unknown
        buff: event.buff,
    };
    assert!(aggregator.process(zenoh_event).is_err());
}

#[test]
fn test_insert_book() {
    let (event, mut aggregator) = setup(Index::Standard, Source::Binance(Type::Snapshot));
    let zenoh_event = ZenohEvent {
        stream_id: 0, // snapshot
        buff: event.buff,
    };
    assert!(aggregator.process(zenoh_event).is_ok());
    assert_eq!(
        aggregator.list_books(),
        vec![&String::from("btc-usdt-spot")]
    );
}

#[test]
fn test_stats() {
    let (event, mut aggregator) = setup(Index::Standard, Source::Binance(Type::Snapshot));
    let zenoh_event = ZenohEvent {
        stream_id: 0, // snapshot
        buff: event.buff,
    };
    assert!(aggregator.process(zenoh_event).is_ok());

    let instrument = "btc-usdt-spot";
    let best_bid = aggregator.get_best_bid(instrument);
    assert!(best_bid.is_some_and(|x| x == (6.0 * ASSET_CONSTANT_MULTIPLIER) as u64));
    let best_ask = aggregator.get_best_ask(instrument);
    assert!(best_ask.is_some_and(|x| x == (1.0 * ASSET_CONSTANT_MULTIPLIER) as u64));
    let mid_price = aggregator.get_mid_price(instrument);
    assert!(mid_price.is_some_and(|x| x == (3.5 * ASSET_CONSTANT_MULTIPLIER) as u64));

    let exec_bid = aggregator.get_execution_bid(instrument, 80);
    assert!(exec_bid.is_some_and(|x| x == (5.5 * ASSET_CONSTANT_MULTIPLIER) as u64));
    let exec_bid = aggregator.get_execution_bid(instrument, 100);
    assert!(exec_bid.is_some_and(|x| x == (5.2 * ASSET_CONSTANT_MULTIPLIER) as u64));
    assert!(aggregator.get_execution_bid(instrument, 130).is_none());

    let exec_ask = aggregator.get_execution_ask(instrument, 50);
    assert!(exec_ask.is_some_and(|x| x == (3.0 * ASSET_CONSTANT_MULTIPLIER) as u64));
    let exec_ask = aggregator.get_execution_ask(instrument, 80);
    assert!(exec_ask.is_some_and(|x| x == (3.75 * ASSET_CONSTANT_MULTIPLIER) as u64));
    assert!(aggregator.get_execution_ask(instrument, 100).is_none());
}

#[test]
fn test_levels() {
    let (event, mut aggregator) = setup(Index::Standard, Source::Binance(Type::Snapshot));
    let zenoh_event = ZenohEvent {
        stream_id: 0, // snapshot
        buff: event.buff,
    };
    assert!(aggregator.process(zenoh_event).is_ok());

    let instrument = "btc-usdt-spot";
    let bid_levels = aggregator.list_bid_levels(instrument);
    assert!(bid_levels.is_some_and(|x| x
        == [2, 4, 6]
            .iter()
            .rev()
            .map(|i| scale(*i))
            .collect::<Vec<u64>>()
            .iter()
            .collect::<Vec<&u64>>()));

    let ask_levels = aggregator.list_ask_levels(instrument);
    assert!(ask_levels.is_some_and(|x| x
        == [1, 3, 5]
            .iter()
            .map(|i| scale(*i))
            .collect::<Vec<u64>>()
            .iter()
            .collect::<Vec<&u64>>()));
}

#[test]
fn test_reset_event() {
    let mut aggregator = setup_aggregator();

    // first snapshot
    let f_event = setup_flatbuffer(Index::Standard, Source::Binance(Type::Snapshot));
    let zenoh_event_one = ZenohEvent {
        stream_id: 0,
        buff: f_event.buff,
    };
    assert!(aggregator.process(zenoh_event_one).is_ok());

    // second snapshot
    let f_event = setup_flatbuffer(Index::ReSnaphsot, Source::Binance(Type::Snapshot));
    let zenoh_event_two = ZenohEvent {
        stream_id: 0,
        buff: f_event.buff,
    };
    assert!(aggregator.process(zenoh_event_two).is_ok());

    let instrument = "btc-usdt-spot";
    assert_eq!(aggregator.list_books(), vec![&String::from(instrument)]);

    let bid_levels = aggregator.list_bid_levels(instrument);
    assert!(bid_levels.is_some_and(|x| x
        == vec![2, 8, 10, 12]
            .into_iter()
            .rev()
            .map(scale)
            .collect::<Vec<u64>>()
            .iter()
            .collect::<Vec<&u64>>()));

    let ask_levels = aggregator.list_ask_levels(instrument);
    assert!(ask_levels.is_some_and(|x| x
        == vec![5, 7, 9, 11]
            .into_iter()
            .map(scale)
            .collect::<Vec<u64>>()
            .iter()
            .collect::<Vec<&u64>>()));

    let best_bid = aggregator.get_best_bid(instrument);
    assert!(best_bid.is_some_and(|x| x == (12.0 * ASSET_CONSTANT_MULTIPLIER) as u64));
    let best_ask = aggregator.get_best_ask(instrument);
    assert!(best_ask.is_some_and(|x| x == (5.0 * ASSET_CONSTANT_MULTIPLIER) as u64));
    let mid_price = aggregator.get_mid_price(instrument);
    assert!(mid_price.is_some_and(|x| x == (8.5 * ASSET_CONSTANT_MULTIPLIER) as u64));

    let exec_bid = aggregator.get_execution_bid(instrument, 310);
    assert!(exec_bid.is_some_and(|x| x == (10.0 * ASSET_CONSTANT_MULTIPLIER) as u64));
    assert!(aggregator.get_execution_bid(instrument, 400).is_none());

    let exec_ask = aggregator.get_execution_ask(instrument, 170);
    assert!(exec_ask.is_some_and(|x| x == (7.0 * ASSET_CONSTANT_MULTIPLIER) as u64));
    assert!(aggregator.get_execution_ask(instrument, 400).is_none());
}

#[test]
fn test_update_event() {
    let mut aggregator = setup_aggregator();

    // first snapshot
    let f_event = setup_flatbuffer(Index::Standard, Source::Binance(Type::Snapshot));
    let zenoh_event_one = ZenohEvent {
        stream_id: 0, // snapshot event
        buff: f_event.buff,
    };
    assert!(aggregator.process(zenoh_event_one).is_ok());

    // first update
    let f_event = setup_flatbuffer(Index::ReUpdate, Source::Binance(Type::Update));
    let zenoh_event_two = ZenohEvent {
        stream_id: 1, // update event
        buff: f_event.buff,
    };
    assert!(aggregator.process(zenoh_event_two).is_ok());

    let instrument = "btc-usdt-spot";
    assert_eq!(aggregator.list_books(), vec![&String::from(instrument)]);

    let bid_levels = aggregator.list_bid_levels(instrument);
    assert!(bid_levels.is_some_and(|x| x
        == vec![2, 4, 8, 10]
            .into_iter()
            .rev()
            .map(scale)
            .collect::<Vec<u64>>()
            .iter()
            .collect::<Vec<&u64>>()));

    let ask_levels = aggregator.list_ask_levels(instrument);
    assert!(ask_levels.is_some_and(|x| x
        == vec![1, 3, 7, 9]
            .into_iter()
            .map(scale)
            .collect::<Vec<u64>>()
            .iter()
            .collect::<Vec<&u64>>()));

    let best_bid = aggregator.get_best_bid(instrument);
    assert!(best_bid.is_some_and(|x| x == (10.0 * ASSET_CONSTANT_MULTIPLIER) as u64));
    let best_ask = aggregator.get_best_ask(instrument);
    assert!(best_ask.is_some_and(|x| x == (1.0 * ASSET_CONSTANT_MULTIPLIER) as u64));
    let mid_price = aggregator.get_mid_price(instrument);
    assert!(mid_price.is_some_and(|x| x == (5.5 * ASSET_CONSTANT_MULTIPLIER) as u64));

    let exec_bid = aggregator.get_execution_bid(instrument, 2000);
    assert!(exec_bid.is_some_and(|x| x == (8.6 * ASSET_CONSTANT_MULTIPLIER) as u64));
    assert!(aggregator.get_execution_bid(instrument, 3001).is_none());

    let exec_ask = aggregator.get_execution_ask(instrument, 1000);
    assert!(exec_ask.is_some_and(|x| x == (5.2 * ASSET_CONSTANT_MULTIPLIER) as u64));
    assert!(aggregator.get_execution_ask(instrument, 2501).is_none());
}

#[test]
fn test_mixed_snapshots() {
    let mut aggregator = setup_aggregator();

    // first snapshot - binance
    let f_event = setup_flatbuffer(Index::Standard, Source::Binance(Type::Snapshot));
    let zenoh_event_one = ZenohEvent {
        stream_id: 0, // snapshot event
        buff: f_event.buff,
    };
    assert!(aggregator.process(zenoh_event_one).is_ok());

    // second snapshot - okx
    let f_event = setup_flatbuffer(Index::ReSnaphsot, Source::Okx(Type::Snapshot));
    let zenoh_event_two = ZenohEvent {
        stream_id: 0, // snapshot event
        buff: f_event.buff,
    };
    assert!(aggregator.process(zenoh_event_two).is_ok());

    let instrument = "btc-usdt-spot";
    assert_eq!(aggregator.list_books(), vec![&String::from(instrument)]);

    let bid_levels = aggregator.list_bid_levels(instrument);
    assert!(bid_levels.is_some_and(|x| x
        == vec![2, 4, 6, 8, 10, 12]
            .into_iter()
            .rev()
            .map(scale)
            .collect::<Vec<u64>>()
            .iter()
            .collect::<Vec<&u64>>()));

    let ask_levels = aggregator.list_ask_levels(instrument);
    assert!(ask_levels.is_some_and(|x| x
        == vec![1, 3, 5, 7, 9, 11]
            .into_iter()
            .map(scale)
            .collect::<Vec<u64>>()
            .iter()
            .collect::<Vec<&u64>>()));

    let best_bid = aggregator.get_best_bid(instrument);
    assert!(best_bid.is_some_and(|x| x == (12.0 * ASSET_CONSTANT_MULTIPLIER) as u64));
    let best_ask = aggregator.get_best_ask(instrument);
    assert!(best_ask.is_some_and(|x| x == (1.0 * ASSET_CONSTANT_MULTIPLIER) as u64));
    let mid_price = aggregator.get_mid_price(instrument);
    assert!(mid_price.is_some_and(|x| x == (6.5 * ASSET_CONSTANT_MULTIPLIER) as u64));

    let exec_bid = aggregator.get_execution_bid(instrument, 400);
    assert!(exec_bid.is_some_and(|x| x == (9.0 * ASSET_CONSTANT_MULTIPLIER) as u64));
    assert!(aggregator.get_execution_bid(instrument, 440).is_some());
    assert!(aggregator.get_execution_bid(instrument, 441).is_none());

    let exec_ask = aggregator.get_execution_ask(instrument, 200);
    assert!(exec_ask.is_some_and(|x| x == (5.1 * ASSET_CONSTANT_MULTIPLIER) as u64));
    assert!(aggregator.get_execution_ask(instrument, 410).is_some());
    assert!(aggregator.get_execution_ask(instrument, 411).is_none());
}

#[test]
fn test_mixed_updates() {
    let mut aggregator = setup_aggregator();

    // first snapshot - binance
    let f_event = setup_flatbuffer(Index::Standard, Source::Binance(Type::Snapshot));
    let zenoh_event_one = ZenohEvent {
        stream_id: 0, // snapshot event
        buff: f_event.buff,
    };
    assert!(aggregator.process(zenoh_event_one).is_ok());

    // second snapshot - okx
    let f_event = setup_flatbuffer(Index::ReSnaphsot, Source::Okx(Type::Snapshot));
    let zenoh_event_two = ZenohEvent {
        stream_id: 0, // snapshot event
        buff: f_event.buff,
    };
    assert!(aggregator.process(zenoh_event_two).is_ok());

    // first update - binance
    let f_event = setup_flatbuffer(Index::ReUpdate, Source::Binance(Type::Update));
    let zenoh_event_three = ZenohEvent {
        stream_id: 1, // update event
        buff: f_event.buff,
    };
    assert!(aggregator.process(zenoh_event_three).is_ok());

    // second update - okx
    let f_event = setup_flatbuffer(Index::ReUpdate, Source::Okx(Type::Update));
    let zenoh_event_four = ZenohEvent {
        stream_id: 1, // update event
        buff: f_event.buff,
    };
    assert!(aggregator.process(zenoh_event_four).is_ok());

    let instrument = "btc-usdt-spot";
    assert_eq!(aggregator.list_books(), vec![&String::from(instrument)]);

    let bid_levels = aggregator.list_bid_levels(instrument);
    assert!(bid_levels.is_some_and(|x| x
        == vec![2, 4, 8, 10, 12]
            .into_iter()
            .rev()
            .map(scale)
            .collect::<Vec<u64>>()
            .iter()
            .collect::<Vec<&u64>>()));

    let ask_levels = aggregator.list_ask_levels(instrument);
    assert!(ask_levels.is_some_and(|x| x
        == vec![1, 3, 7, 9, 11]
            .into_iter()
            .map(scale)
            .collect::<Vec<u64>>()
            .iter()
            .collect::<Vec<&u64>>()));

    let best_bid = aggregator.get_best_bid(instrument);
    assert!(best_bid.is_some_and(|x| x == (12.0 * ASSET_CONSTANT_MULTIPLIER) as u64));
    let best_ask = aggregator.get_best_ask(instrument);
    assert!(best_ask.is_some_and(|x| x == (1.0 * ASSET_CONSTANT_MULTIPLIER) as u64));
    let mid_price = aggregator.get_mid_price(instrument);
    assert!(mid_price.is_some_and(|x| x == (6.5 * ASSET_CONSTANT_MULTIPLIER) as u64));

    let exec_bid = aggregator.get_execution_bid(instrument, 3200);
    assert!(exec_bid.is_some_and(|x| x == (9.4 * ASSET_CONSTANT_MULTIPLIER) as u64));
    assert!(aggregator.get_execution_bid(instrument, 4920).is_some());
    assert!(aggregator.get_execution_bid(instrument, 4921).is_none());

    let exec_ask = aggregator.get_execution_ask(instrument, 2000);
    assert!(exec_ask.is_some_and(|x| x == (5.2 * ASSET_CONSTANT_MULTIPLIER) as u64));
    assert!(aggregator.get_execution_ask(instrument, 4110).is_some());
    assert!(aggregator.get_execution_ask(instrument, 4111).is_none());

    let bid_liq = aggregator.get_bid_liquidity(instrument, 12);
    assert!(bid_liq.is_some_and(|x| x == (120.0 * ASSET_CONSTANT_MULTIPLIER) as u64));
    let bid_liq = aggregator.get_bid_liquidity(instrument, 2);
    assert!(bid_liq.is_some_and(|x| x == (400.0 * ASSET_CONSTANT_MULTIPLIER) as u64));
    assert!(aggregator.get_bid_liquidity(instrument, 6).is_none());

    let ask_liq = aggregator.get_ask_liquidity(instrument, 11);
    assert!(ask_liq.is_some_and(|x| x == (110.0 * ASSET_CONSTANT_MULTIPLIER) as u64));
    let ask_liq = aggregator.get_ask_liquidity(instrument, 7);
    assert!(ask_liq.is_some_and(|x| x == (1400.0 * ASSET_CONSTANT_MULTIPLIER) as u64));
    assert!(aggregator.get_bid_liquidity(instrument, 5).is_none());
}

#[test]
fn test_bids_till_bps() {
    let (event, mut aggregator) = setup(Index::Standard, Source::Binance(Type::Snapshot));
    let zenoh_event = ZenohEvent {
        stream_id: 0, // snapshot
        buff: event.buff,
    };
    assert!(aggregator.process(zenoh_event).is_ok());

    let instrument = "btc-usdt-spot";
    let till_25bps = aggregator.get_bid_qty_till(instrument, 25);
    assert!(till_25bps.is_some_and(|x| x == scale(100)));

    let till_50bps = aggregator.get_bid_qty_till(instrument, 50);
    assert!(till_50bps.is_some_and(|x| x == scale(100)));

    let till_75bps = aggregator.get_bid_qty_till(instrument, 75);
    assert!(till_75bps.is_some_and(|x| x == scale(100)));
}

#[test]
fn test_asks_till_bps() {
    let (event, mut aggregator) = setup(Index::Standard, Source::Binance(Type::Snapshot));
    let zenoh_event = ZenohEvent {
        stream_id: 0, // snapshot
        buff: event.buff,
    };
    assert!(aggregator.process(zenoh_event).is_ok());

    let instrument = "btc-usdt-spot";
    let till_25bps = aggregator.get_ask_qty_till(instrument, 25);
    assert!(till_25bps.is_some_and(|x| x == scale(40)));

    let till_50bps = aggregator.get_ask_qty_till(instrument, 50);
    assert!(till_50bps.is_some_and(|x| x == scale(40)));

    let till_75bps = aggregator.get_ask_qty_till(instrument, 75);
    assert!(till_75bps.is_some_and(|x| x == scale(40)));
}
