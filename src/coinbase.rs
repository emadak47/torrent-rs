use crate::{
    utils::{now, Result, TorrentError},
    websocket::Wss,
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use std::fmt;

#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, Debug)]
pub enum Channel {
    STATUS,
    LEVEL2,
    HEARTBEATS,
}

impl fmt::Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Channel::HEARTBEATS => write!(f, "heartbeats"),
            Channel::LEVEL2 => write!(f, "level2"),
            Channel::STATUS => write!(f, "status"),
        }
    }
}

pub struct Coinbase {
    api_key: String,
    private_key: String,
}

impl fmt::Display for Coinbase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Coinbase")
    }
}

impl Coinbase {
    pub const URL: &'static str = "wss://advanced-trade-ws.coinbase.com";

    pub fn new(api_key: String, private_key: String) -> Self {
        Self {
            api_key,
            private_key,
        }
    }

    pub fn sign_ws(
        &self,
        timestamp: &str,
        channel: &str,
        product_ids: Vec<String>,
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
}

impl Wss for Coinbase {
    fn subscribe(&self, channel: String, topics: Vec<String>) -> Result<String> {
        let timestamp = now().to_string();
        let _signature = self.sign_ws(timestamp.as_str(), channel.as_str(), topics.clone())?;
        Ok(String::from("Hello from Coinbase"))
    }
}
