use crate::utils::{from_str, now, Result, TorrentError};
use crate::websocket::{MessageCallback, Wss};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum Message {
    Level2(Level2Message),
    Heartbeats(HeartbeatsMessage),
    Subscribe(SubscribeMessage),
}

#[derive(Deserialize, Debug)]
pub struct Level2Update {
    pub side: String,
    pub event_time: String,
    #[serde(deserialize_with = "from_str")]
    pub price_level: f64,
    #[serde(deserialize_with = "from_str")]
    pub new_quantity: f64,
}

#[derive(Deserialize, Debug)]
pub struct SubscribeUpdate {
    #[serde(default)]
    pub status: Vec<String>,
    #[serde(default)]
    pub ticker: Vec<String>,
    #[serde(default)]
    pub ticker_batch: Vec<String>,
    #[serde(default)]
    pub level2: Option<Vec<String>>,
    #[serde(default)]
    pub user: Option<Vec<String>>,
    #[serde(default)]
    pub market_trades: Option<Vec<String>>,
    #[serde(default)]
    pub heartbeats: Option<Vec<String>>,
}

#[derive(Deserialize, Debug)]
pub struct Level2Event {
    pub r#type: String,
    pub product_id: String,
    pub updates: Vec<Level2Update>,
}

#[derive(Deserialize, Debug)]
pub struct HeartbeatsEvent {
    pub current_time: String,
    pub heartbeat_counter: u64,
}

#[derive(Deserialize, Debug)]
pub struct SubscribeEvent {
    pub subscriptions: SubscribeUpdate,
}

#[derive(Deserialize, Debug)]
pub struct Level2Message {
    pub channel: String,
    pub client_id: String,
    pub timestamp: String,
    pub sequence_num: u64,
    pub events: Vec<Level2Event>,
}

#[derive(Deserialize, Debug)]
pub struct HeartbeatsMessage {
    pub channel: String,
    pub client_id: String,
    pub timestamp: String,
    pub sequence_num: u64,
    pub events: Vec<HeartbeatsEvent>,
}

#[derive(Deserialize, Debug)]
pub struct SubscribeMessage {
    pub channel: String,
    pub client_id: String,
    pub timestamp: String,
    pub sequence_id: u64,
    pub events: Vec<SubscribeEvent>,
}

#[derive(Serialize, Debug)]
struct LegacySubscription {
    pub r#type: String,
    pub product_ids: Vec<String>,
    pub channel: String,
    pub api_key: String,
    pub timestamp: String,
    pub signature: String,
}

#[derive(Serialize, Debug)]
struct JwtSubscription {
    pub r#type: String,
    pub product_ids: Vec<String>,
    pub channel: String,
    pub jwt: String,
    pub timestamp: String,
}

#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, Debug)]
pub enum Channel {
    LEVEL2,
    HEARTBEATS,
}

impl fmt::Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Channel::HEARTBEATS => write!(f, "heartbeats"),
            Channel::LEVEL2 => write!(f, "level2"),
        }
    }
}

#[derive(Debug)]
pub struct Coinbase {
    key_name: String,
    private_key: String,
    is_legacy: bool,
}

impl fmt::Display for Coinbase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Coinbase")
    }
}

impl Coinbase {
    pub const URL: &'static str = "wss://advanced-trade-ws.coinbase.com";

    pub fn new(key_name: String, private_key: String, is_legacy: bool) -> Self {
        Self {
            key_name,
            private_key,
            is_legacy,
        }
    }

    fn legacy_sign(
        &self,
        timestamp: &str,
        channel: &str,
        product_ids: &[String],
    ) -> Result<String> {
        let prehash = format!("{}{}{}", timestamp, channel, product_ids.join(","));
        let prehash_bytes = prehash.as_bytes();
        let private_key_bytes = self.private_key.as_bytes();
        let mut mac = match Hmac::<sha2::Sha256>::new_from_slice(private_key_bytes) {
            Ok(mac) => mac,
            Err(e) => return Err(TorrentError::BadParse(format!("Coinbase signature: {}", e))),
        };
        mac.update(prehash_bytes);
        let signature_bytes = mac.finalize().into_bytes();
        Ok(hex::encode(signature_bytes))
    }

    fn jwt_sign(&self, _timestamp: &str) -> Result<String> {
        let _private_key_bytes = self.private_key.as_bytes();
        unimplemented!()
    }
}

impl Wss for Coinbase {
    fn subscribe(&mut self, channel: String, topics: Vec<String>) -> Result<String> {
        let timestamp = now().to_string();
        let sub = if self.is_legacy {
            let signature = self.legacy_sign(timestamp.as_str(), channel.as_str(), &topics)?;
            let legacy_sub = LegacySubscription {
                r#type: "subscribe".to_string(),
                product_ids: topics,
                channel,
                api_key: self.key_name.clone(),
                timestamp,
                signature,
            };
            serde_json::to_string(&legacy_sub)
        } else {
            let jwt = self.jwt_sign(timestamp.as_str())?;
            let jwt_sub = JwtSubscription {
                r#type: "subscribe".to_string(),
                product_ids: topics,
                channel,
                jwt,
                timestamp,
            };
            serde_json::to_string(&jwt_sub)
        };

        match sub {
            Ok(s) => Ok(s),
            Err(e) => Err(TorrentError::BadParse(format!("serde parse error: {}", e))),
        }
    }
}

pub struct Manager;

impl MessageCallback<Message> for Manager {
    fn message_callback(&mut self, msg: Result<Message>) -> Result<()> {
        let msg = msg?;
        match msg {
            Message::Subscribe(_m) => {
                unimplemented!()
            }
            Message::Heartbeats(_m) => {
                unimplemented!()
            }
            Message::Level2(_m) => {
                unimplemented!()
            }
        }
        Ok(())
    }
}
