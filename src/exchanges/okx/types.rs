use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Methods {
    Subscribe,
    Unsubscribe,
}
#[derive(Debug, Serialize)]
pub struct Subscription<'a, T> {
    op: Methods,
    args: Option<&'a Vec<T>>,
}

impl<'a, T> Subscription<'a, T> {
    pub(crate) fn new(op: Methods, args: Option<&'a Vec<T>>) -> Self {
        Self { op, args }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Event {
    InitResponse(InitResponse),
    DepthOrderBook(DepthOrderBookEvent),
    PublicTrade(PublicTradeEvent),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Args {
    #[serde(default = "String::default")]
    pub channel: String,
    #[serde(default = "String::default")]
    pub inst_id: String,
}

#[derive(Serialize, Debug, Deserialize)]
pub struct InitResponse {
    pub event: String,
    pub arg: Option<Args>,
    pub msg: Option<String>,
    pub code: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DepthOrderBookEvent {
    pub arg: Args,
    pub action: String,
    pub data: Vec<DepthOrderBookData>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DepthOrderBookData {
    pub asks: Vec<Vec<String>>,
    pub bids: Vec<Vec<String>>,
    pub ts: String,
    pub checksum: i64,
    pub prev_seq_id: i64,
    pub seq_id: i64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicTradeEvent {
    pub arg: Args,
    pub data: Vec<PublicTradeData>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicTradeData {
    inst_id: String,
    trade_id: String,
    px: String,
    sz: String,
    side: String,
    ts: String,
    count: String,
}
