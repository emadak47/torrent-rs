use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Methods {
    Subscribe,
    Unsubscribe,
}

#[derive(Debug, Serialize)]
pub struct Subscription {
    jsonrpc: String,
    r#type: String,
    method: Methods,
    params: Option<SubscriptionParams>,
    id: u64,
}

#[derive(Debug, Serialize)]
pub struct SubscriptionParams {
    topic: String,
    symbol: String,
}

impl Subscription {
    pub(crate) fn new(op: Methods, topic: &str, symbol: &str, id: u64) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            r#type: "command".to_string(),
            method: op,
            params: Some(SubscriptionParams { topic: topic.to_string(), symbol: symbol.to_string() }),
            id,
        }
    }
}