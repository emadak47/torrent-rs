use crate::{
    exchanges::binance::flatbuffer::{
        orderbook::atrimo::update_events::*, snapshot::atrimo::snapshot_events::*,
    },
    orderbook::l2::Level,
};
use flatbuffers;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::common::FlatbufferEvent;
use crate::common::CcyPair;

#[allow(dead_code)]
pub fn make_order_book_update_event(
    bids: Vec<Level>,
    asks: Vec<Level>,
    instrument: CcyPair,
) -> FlatbufferEvent {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(10240000);
    let exchange_name = builder.create_string("bybit");
    let instrument_name = builder.create_string(&instrument.to_string());

    let mut bid_offsets = Vec::new();
    let mut ask_offsets = Vec::new();

    for level in bids {
        bid_offsets.push(UpdateBidData::new(level.price, level.qty));
    }

    for level in asks {
        ask_offsets.push(UpdateAskData::new(level.price, level.qty));
    }
    let asksvector = builder.create_vector(&ask_offsets);
    let bidsvector = builder.create_vector(&bid_offsets);

    let update_data = UpdateData::create(
        &mut builder,
        &UpdateDataArgs {
            asks: Some(asksvector),
            bids: Some(bidsvector),
        },
    );
    let duration_since_epoch = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let timestamp_nanos = duration_since_epoch.as_micros();
    let update_event = UpdateEvent::create(
        &mut builder,
        &UpdateEventArgs {
            exchange: Some(exchange_name),
            instrument: Some(instrument_name),
            timestamp: timestamp_nanos as u64,
            update: Some(update_data),
        },
    );
    let update_event_message = UpdateEventMessage::create(
        &mut builder,
        &UpdateEventMessageArgs {
            update_event: Some(update_event),
            message_type: 1,
        },
    );
    finish_update_event_message_buffer(&mut builder, update_event_message);
    let buffer = builder.finished_data().to_vec();
    FlatbufferEvent {
        stream_id: 1,
        buff: buffer,
    }
}

#[allow(dead_code)]
pub fn make_order_book_snapshot_event(
    bids: Vec<Level>,
    asks: Vec<Level>,
    instrument: CcyPair,
) -> FlatbufferEvent {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(10240000);
    let exchange_name = builder.create_string("bybit");
    let instrument_name = builder.create_string(&instrument.to_string());

    let mut bid_offsets = Vec::new();
    let mut ask_offsets = Vec::new();

    for level in bids {
        bid_offsets.push(SnapshotBidData::new(level.price, level.qty));
    }

    for level in asks {
        ask_offsets.push(SnapshotAskData::new(level.price, level.qty));
    }

    let asksvector = builder.create_vector(&ask_offsets);
    let bidsvector = builder.create_vector(&bid_offsets);

    let snapshot_data = SnapshotData::create(
        &mut builder,
        &SnapshotDataArgs {
            asks: Some(asksvector),
            bids: Some(bidsvector),
        },
    );
    let snapshot_event = SnapshotEvent::create(
        &mut builder,
        &SnapshotEventArgs {
            exchange: Some(exchange_name),
            instrument: Some(instrument_name),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
            snapshot: Some(snapshot_data),
        },
    );
    let snapshot_event_message = SnapshotEventMessage::create(
        &mut builder,
        &SnapshotEventMessageArgs {
            snapshot_event: Some(snapshot_event),
            message_type: 0,
        },
    );

    finish_snapshot_event_message_buffer(&mut builder, snapshot_event_message);
    let buffer = builder.finished_data().to_vec();
    FlatbufferEvent {
        stream_id: 0,
        buff: buffer,
    }
}

#[allow(dead_code)]
pub fn make_order_book_snapshot_event_aggregator(
    bids: Vec<Level>,
    asks: Vec<Level>,
    instrument: &str,
) -> FlatbufferEvent {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
    let exchange_name = builder.create_string("");
    let instrument_name = builder.create_string(instrument);

    let mut bid_offsets = Vec::new();
    let mut ask_offsets = Vec::new();

    for level in bids {
        bid_offsets.push(SnapshotBidData::new(level.price, level.qty));
    }

    for level in asks {
        ask_offsets.push(SnapshotAskData::new(level.price, level.qty));
    }

    let asksvector = builder.create_vector(&ask_offsets);
    let bidsvector = builder.create_vector(&bid_offsets);

    let snapshot_data = SnapshotData::create(
        &mut builder,
        &SnapshotDataArgs {
            asks: Some(asksvector),
            bids: Some(bidsvector),
        },
    );
    let snapshot_event = SnapshotEvent::create(
        &mut builder,
        &SnapshotEventArgs {
            exchange: Some(exchange_name),
            instrument: Some(instrument_name),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
            snapshot: Some(snapshot_data),
        },
    );
    let snapshot_event_message = SnapshotEventMessage::create(
        &mut builder,
        &SnapshotEventMessageArgs {
            snapshot_event: Some(snapshot_event),
            message_type: 0,
        },
    );

    finish_snapshot_event_message_buffer(&mut builder, snapshot_event_message);
    let buffer = builder.finished_data().to_vec();
    FlatbufferEvent {
        stream_id: 0,
        buff: buffer,
    }
}

