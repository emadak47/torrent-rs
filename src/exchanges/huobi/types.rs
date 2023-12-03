use serde::{Deserialize, Serialize};

#[derive(Serialize, Debug, Deserialize)]
pub struct InitResponse {
    pub id: String,
    pub status: Option<String>,
    pub subbed: Option<String>,
    pub ts: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OrderBook {
    pub ch: String,
    pub status: Option<String>,
    pub tick: Tick,
    pub ts: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Tick {
    pub bids: Vec<Bids>,
    pub asks: Vec<Asks>,
    pub mrid: Option<u32>,
    pub id: Option<u32>,
    pub ts: Option<u64>,
    pub version: Option<u32>,
    pub ch: Option<String>,
    pub event: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Bids {
    pub price: f64,
    pub qty: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Asks {
    pub price: f64,
    pub qty: f64,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Event {
    InitResponse(InitResponse),
    OrderBook(OrderBook),
}

