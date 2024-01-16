use super::{
    orderbook::atrimo::update_events::{
        finish_update_event_message_buffer, UpdateAskData, UpdateBidData, UpdateData,
        UpdateDataArgs, UpdateEvent, UpdateEventArgs, UpdateEventMessage, UpdateEventMessageArgs,
    },
    snapshot::atrimo::snapshot_events::{
        finish_snapshot_event_message_buffer, SnapshotAskData, SnapshotBidData, SnapshotData,
        SnapshotDataArgs, SnapshotEvent, SnapshotEventArgs, SnapshotEventMessage,
        SnapshotEventMessageArgs,
    },
};
use crate::{
    orderbook::l2::Level,
    utils::{CcyPair, Exchange, FlatbufferEvent},
};
use failure::ResultExt;
use std::time::SystemTime;

pub fn make_update_event(
    bids: Vec<Level>,
    asks: Vec<Level>,
    ccy_pair: CcyPair,
    exchange: Exchange,
) -> Result<FlatbufferEvent, failure::Error> {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(10240000);
    let exchange_name = builder.create_string(exchange.to_string().as_str());
    let instrument_name = builder.create_string(ccy_pair.to_string().as_str());

    let mut bid_offsets = Vec::new();
    let mut ask_offsets = Vec::new();

    for level in bids {
        bid_offsets.push(UpdateBidData::new(level.price, level.qty));
    }

    for level in asks {
        ask_offsets.push(UpdateAskData::new(level.price, level.qty));
    }

    let asks_vector = builder.create_vector(&ask_offsets);
    let bids_vector = builder.create_vector(&bid_offsets);

    let update_data = UpdateData::create(
        &mut builder,
        &UpdateDataArgs {
            asks: Some(asks_vector),
            bids: Some(bids_vector),
        },
    );

    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .with_context(|e| format!("duration since epoch error {e}"))?
        .as_micros();

    let update_event = UpdateEvent::create(
        &mut builder,
        &UpdateEventArgs {
            exchange: Some(exchange_name),
            instrument: Some(instrument_name),
            timestamp: timestamp as u64,
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

    Ok(FlatbufferEvent {
        stream_id: 1,
        buff: buffer,
    })
}

pub fn make_snapshot_event(
    bids: Vec<Level>,
    asks: Vec<Level>,
    ccy_pair: CcyPair,
    exchange: Exchange,
) -> Result<FlatbufferEvent, failure::Error> {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(10240000);
    let exchange_name = builder.create_string(exchange.to_string().as_str());
    let instrument_name = builder.create_string(ccy_pair.to_string().as_str());

    let mut bid_offsets = Vec::new();
    let mut ask_offsets = Vec::new();

    for level in bids {
        bid_offsets.push(SnapshotBidData::new(level.price, level.qty));
    }

    for level in asks {
        ask_offsets.push(SnapshotAskData::new(level.price, level.qty));
    }

    let asks_vector = builder.create_vector(&ask_offsets);
    let bids_vector = builder.create_vector(&bid_offsets);

    let snapshot_data = SnapshotData::create(
        &mut builder,
        &SnapshotDataArgs {
            asks: Some(asks_vector),
            bids: Some(bids_vector),
        },
    );

    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .with_context(|e| format!("duration since epoch error {e}"))?
        .as_micros();

    let snapshot_event = SnapshotEvent::create(
        &mut builder,
        &SnapshotEventArgs {
            exchange: Some(exchange_name),
            instrument: Some(instrument_name),
            timestamp: timestamp as u64,
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

    Ok(FlatbufferEvent {
        stream_id: 0,
        buff: buffer,
    })
}

pub fn make_snapshot_aggregator(
    bids: Vec<Level>,
    asks: Vec<Level>,
    instrument: &str,
) -> Result<FlatbufferEvent, failure::Error> {
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

    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .with_context(|e| format!("duration since epoch error {e}"))?
        .as_micros();

    let snapshot_event = SnapshotEvent::create(
        &mut builder,
        &SnapshotEventArgs {
            exchange: Some(exchange_name),
            instrument: Some(instrument_name),
            timestamp: timestamp as u64,
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

    Ok(FlatbufferEvent {
        stream_id: 0,
        buff: buffer,
    })
}
