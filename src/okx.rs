use crate::{utils::Result, websocket::Wss};
use std::fmt;

pub struct Okx {}

impl Okx {
    pub const URL: &'static str = "wss://ws.okx.com:8443/ws/v5/public";

    pub fn new() -> Self {
        Self {}
    }
}

impl fmt::Display for Okx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Okx")
    }
}

impl Wss for Okx {
    fn subscribe(&self, _channel: String, _topics: Vec<String>) -> Result<String> {
        Ok(String::from("Hello from Okx"))
    }
}
