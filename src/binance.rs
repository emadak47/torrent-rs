use crate::utils::{Result, TorrentError};
use crate::websocket::{MessageCallback, Wss};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum Message {
    Subscribe(SubscribeMessage),
    Depth(DepthMessage),
}

#[derive(Debug)]
pub struct LevelUpdate([f64; 2]);

impl Deref for LevelUpdate {
    type Target = [f64; 2];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'de> Deserialize<'de> for LevelUpdate {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_tuple(2, LevelUpdateVisitor)
    }
}

struct LevelUpdateVisitor;

impl<'de> serde::de::Visitor<'de> for LevelUpdateVisitor {
    type Value = LevelUpdate;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("Fixed-size array of 2 strings")
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut arr = [0.0; 2];
        let mut i = 0;

        while let Some(v) = seq.next_element::<String>()? {
            // v is a valid string but not parsable to f64
            let non_f64 = serde::de::Unexpected::Str(v.as_str());
            let element = v
                .parse()
                .map_err(|_| serde::de::Error::invalid_value(non_f64, &self))?;
            arr[i] = element;
            i += 1;
        }
        Ok(LevelUpdate(arr))
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeMessage {
    pub result: Option<String>,
    pub id: usize,
}

#[derive(Debug, Deserialize)]
pub struct DepthMessage {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "U")]
    pub first_update_id: u64,
    #[serde(rename = "u")]
    pub final_update_id: u64,
    #[serde(rename = "b")]
    pub bids: Vec<LevelUpdate>,
    #[serde(rename = "a")]
    pub asks: Vec<LevelUpdate>,
}

#[derive(Serialize, Debug)]
struct Subscription {
    method: String,
    params: Vec<String>,
    id: usize,
}

#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, Debug)]
pub enum Channel {
    DEPTH,
}

impl fmt::Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Channel::DEPTH => write!(f, "depth"),
        }
    }
}

#[derive(Debug, Default)]
pub struct Binance(usize);

impl Binance {
    pub const URL: &'static str = "wss://stream.binance.com:9443/ws";

    pub fn new() -> Self {
        Self(0)
    }
}

impl fmt::Display for Binance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Binance")
    }
}

impl Wss for Binance {
    fn subscribe(&mut self, channel: String, topics: Vec<String>) -> Result<String> {
        let params = topics
            .into_iter()
            .filter_map(|t| {
                let parts = t.split('-').collect::<Vec<&str>>();
                if parts.len() == 2 {
                    Some(parts.join(""))
                } else {
                    None
                }
            })
            .map(|sym| format!("{}@{}", sym.to_lowercase(), channel))
            .collect::<Vec<String>>();
        let sub = Subscription {
            method: "subscribe".to_uppercase(),
            params,
            id: self.0,
        };
        self.0 += 1;

        match serde_json::to_string(&sub) {
            Ok(s) => Ok(s),
            Err(e) => Err(TorrentError::BadParse(format!("serde parse error: {}", e))),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct RequestError {
    code: i16,
    msg: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DepthSnapshot {
    last_update_id: u64,
    bids: Vec<LevelUpdate>,
    asks: Vec<LevelUpdate>,
}

pub struct Manager;

impl MessageCallback<Message> for Manager {
    fn message_callback(&mut self, msg: Result<Message>) -> Result<()> {
        match msg? {
            Message::Subscribe(_m) => {
                unimplemented!()
            }
            Message::Depth(_m) => {
                unimplemented!()
            }
        }
    }
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "code: {}, msg: {}", self.code, self.msg)
    }
}

pub enum API {
    Spot(Spot),
}

pub enum Spot {
    Depth,
}

impl From<API> for String {
    fn from(item: API) -> Self {
        String::from(match item {
            API::Spot(route) => match route {
                Spot::Depth => "/api/v3/depth",
            },
        })
    }
}
