use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderBook {
    pub id: u64,
    pub current: i128,
    pub update: i128,
    pub bids: Vec<Vec<String>>,
    pub asks: Vec<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Level2Changes {
    pub t: u64,
    pub e: String,
    pub E: u64,
    pub s: String,
    pub U: u64,
    pub u: u64,
    pub b: Vec<Vec<String>>,
    pub a: Vec<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Level2 {
    pub time: u64,
    pub time_ms: u64,
    pub channel: String,
    pub event: String,
    pub result: Level2Changes,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Event {
    Level2 (Level2),
}