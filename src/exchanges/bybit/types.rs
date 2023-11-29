use serde::{Deserialize, Serialize};

/// The base response which contains common fields of public channels.
#[derive(Deserialize, Debug)]
pub struct BasePublicResponse<Data> {
    /// Topic name.
    pub topic: String,
    /// Data type. `snapshot`, `delta`.
    #[serde(alias = "type")]
    pub type_: String,
    /// The timestamp (ms) that the system generates the data.
    pub ts: u64,
    /// The data vary on the topic.
    pub data: Data,
}

/// The (price, size) pair of orderbook.
#[derive(Deserialize, Debug)]
pub struct OrderbookItem(pub String, pub String);

/// The orderbook data.
#[derive(Deserialize, Debug)]
pub struct Orderbook{
    /// Symbol name.
    pub s: String,
    /// Bids. For `snapshot` stream, the element is sorted by price in descending order.
    pub b: Vec<OrderbookItem>,
    /// Asks. For `snapshot` stream, the element is sorted by price in ascending order.
    pub a: Vec<OrderbookItem>,
    /// Update ID. Is a sequence.
    /// Occasionally, you'll receive "u"=1, which is a snapshot data due to the restart of the service.
    /// So please overwrite your local orderbook.
    pub u: u64,
    /// Cross sequence. Option does not have this field.
    pub seq: Option<u64>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum Event {
    Orderbook(BasePublicResponse<Orderbook>),
}