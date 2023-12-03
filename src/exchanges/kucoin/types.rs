use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct APIData<T> {
    pub code: String,
    pub data: Option<Vec<T>>,
    pub msg: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct APIDatum<T> {
    pub code: String,
    pub data: Option<T>,
    pub msg: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceServers {
    pub instance_servers: Vec<InstanceServer>,
    pub token: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceServer {
    pub ping_interval: i32,
    pub endpoint: String,
    pub protocol: String,
    pub encrypt: bool,
    pub ping_timeout: i32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WSResp<T> {
    pub topic: String,
    pub r#type: String,
    pub data: T,
    pub subject: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Level2Changes {
    pub asks: Vec<Vec<String>>,
    pub bids: Vec<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Level2 {
    pub changes: Level2Changes,
    pub sequence_end: i64,
    pub sequence_start: i64,
    pub symbol: String,
    pub time: u64,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum Event {
    OrderBookMsg(WSResp<Level2>),
}

