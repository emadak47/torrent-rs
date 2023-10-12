use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
enum Events {
    DepthOrderBook(DepthOrderBookEvent),
    PublicTrade(PublicTradeEvent),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Bids {
    pub price: String,
    pub qty: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Asks {
    pub price: String,
    pub qty: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DepthOrderBookEvent {
    #[serde(rename = "E")]
    pub event_time: u64,

    #[serde(rename = "U")]
    pub first_update_id: u64,

    #[serde(rename = "a")]
    pub asks: Option<Vec<Asks>>,

    #[serde(rename = "b")]
    pub bids: Option<Vec<Bids>>,

    #[serde(rename = "e")]
    pub event_type: String,

    #[serde(rename = "s")]
    pub symbol: String,

    #[serde(rename = "u")]
    pub final_update_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicTradeEvent {
    #[serde(rename = "e")]
    pub event_type: String,

    #[serde(rename = "E")]
    pub event_time: u64,

    #[serde(rename = "s")]
    pub symbol: String,

    #[serde(rename = "t")]
    pub trade_id: u64,

    #[serde(rename = "p")]
    pub price: String,

    #[serde(rename = "q")]
    pub qty: String,

    #[serde(rename = "b")]
    pub buyer_order_id: u64,

    #[serde(rename = "a")]
    pub seller_order_id: u64,

    #[serde(rename = "T")]
    pub trade_order_time: u64,

    #[serde(rename = "m")]
    pub is_buyer_maker: bool,

    #[serde(skip, rename = "M")]
    pub m_ignore: bool,
}
