use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Methods {
    Subscribe,
    Unsubscribe,
}

#[derive(Debug, Serialize)]
pub struct Subscription<'a, 'b, T> {
    method: Methods,
    params: Option<&'a T>,
    id: &'b usize,
}

impl<'a, 'b, T> Subscription<'a, 'b, T> {
    pub(crate) fn new(method: Methods, params: Option<&'a T>, id: &'b usize) -> Self {
        Self { method, params, id }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Event {
    SpotDepthOrderBook(SpotDepthOrderBookEvent),
    FuturesDepthOrderBook(FuturesDepthOrderBookEvent),
    SpotPublicTrade(SpotPublicTradeEvent),
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
pub struct SpotDepthOrderBookEvent {
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

#[derive(Debug, Serialize, Deserialize)]
pub struct FuturesDepthOrderBookEvent {
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

    #[serde(rename = "T")]
    pub transaction_time: u64,

    #[serde(rename = "pu")]
    pub final_update_id_in_last_stream: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SpotPublicTradeEvent {
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderBook {
    pub last_update_id: u64,
    pub bids: Vec<Bids>,
    pub asks: Vec<Asks>,
}
