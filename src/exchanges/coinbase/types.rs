use serde::{Deserialize, Serialize};

use chrono;
pub type DateTime = chrono::DateTime<chrono::Utc>;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Deserialize, Debug)]
pub struct Snapshot{
    product_id: String,
    bids: Vec<Level2SnapshotRecord>,
    asks: Vec<Level2SnapshotRecord>,
}

#[derive(Deserialize, Debug)]
pub struct L2update{
    product_id: String,
    changes: Vec<Level2UpdateRecord>,
    time: DateTime,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Level2SnapshotRecord {
    pub price: String,
    pub size: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Level2UpdateRecord {
    pub side: OrderSide,
    pub price: String,
    pub size: String,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Event {
    Snapshot(Snapshot),
    L2update(L2update),
}