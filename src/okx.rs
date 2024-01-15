use crate::utils::{Exchange, Result, TorrentError};
use crate::websocket::{MessageCallback, Wss};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use std::ops::Deref;
use std::result;

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum Message {
    Subscribe(SubscribeMessage),
    Failure(FailureMessage),
    Books(BooksMessage),
}

#[derive(Debug)]
pub struct LevelUpdate([f64; 4]);

impl Deref for LevelUpdate {
    type Target = [f64; 4];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'de> Deserialize<'de> for LevelUpdate {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_tuple(4, LevelUpdateVisitor)
    }
}

struct LevelUpdateVisitor;

impl<'de> serde::de::Visitor<'de> for LevelUpdateVisitor {
    type Value = LevelUpdate;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("Fixed-size array of 4 strings")
    }

    fn visit_seq<A>(self, mut seq: A) -> result::Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut arr = [0.0; 4];
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
pub struct BooksUpdate {
    pub asks: Vec<LevelUpdate>,
    pub bids: Vec<LevelUpdate>,
    pub ts: String,
    pub checksum: i64,
    pub prev_seq_id: i64,
    pub seq_id: i64,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeMessage {
    pub event: String,
    pub arg: Arg,
    pub conn_id: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct FailureMessage {
    pub event: String,
    pub code: String,
    pub msg: String,
    pub conn_id: String,
}

#[derive(Deserialize, Debug)]
pub struct BooksMessage {
    pub action: String,
    pub arg: Arg,
    pub data: Vec<BooksUpdate>,
}

#[derive(Serialize, Debug)]
struct Subscription {
    pub op: String,
    pub args: Vec<Arg>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Arg {
    channel: String,
    inst_id: String,
    #[serde(default = "InstType::str_default")]
    inst_type: String,
}

impl Arg {
    fn new(channel: String, inst_id: String, inst_type: String) -> Self {
        Self {
            channel,
            inst_id,
            inst_type: inst_type.to_uppercase(),
        }
    }
}

#[allow(non_camel_case_types)]
#[derive(Default, Debug)]
pub enum InstType {
    #[default]
    SPOT,
    MARGIN,
    SWAP,
    FUTURES,
    OPTION,
    ANY,
}

impl InstType {
    fn str_default() -> String {
        let variant: InstType = Default::default();
        variant.to_string()
    }
}

impl Display for InstType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InstType::SPOT => write!(f, "SPOT"),
            InstType::MARGIN => write!(f, "MARGIN"),
            InstType::SWAP => write!(f, "SWAP"),
            InstType::FUTURES => write!(f, "FUTURES"),
            InstType::OPTION => write!(f, "OPTION"),
            InstType::ANY => write!(f, "ANY"),
        }
    }
}

#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, Debug)]
pub enum Channel {
    TRADES,
    BOOKS,
}

impl Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Channel::BOOKS => write!(f, "books"),
            Channel::TRADES => write!(f, "trades"),
        }
    }
}

#[derive(Default, Debug)]
pub struct Okx;

impl Display for Okx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Okx")
    }
}

impl Okx {
    pub const URL: &'static str = "wss://ws.okx.com:8443/ws/v5/public";

    pub fn new() -> Self {
        Self {}
    }
}

impl Wss for Okx {
    fn subscribe(&mut self, channel: String, topics: Vec<String>) -> Result<String> {
        let args = topics
            .into_iter()
            .map(|t| Arg::new(channel.clone(), t, "SPOT".to_string()))
            .collect::<Vec<Arg>>();
        let sub = Subscription {
            op: "subscribe".to_string(),
            args,
        };

        match serde_json::to_string(&sub) {
            Ok(s) => Ok(s),
            Err(e) => Err(TorrentError::BadParse(format!("serde parse error: {}", e))),
        }
    }

    fn to_enum(&self) -> Exchange {
        Exchange::OKX
    }
}

pub struct Manager;

impl MessageCallback<Message> for Manager {
    fn message_callback(&mut self, msg: Result<Message>) -> Result<()> {
        match msg? {
            Message::Failure(m) => {
                if m.msg.contains("Invalid request") {
                    // TODO: panic?
                } else if m.msg.contains("Connection refused") {
                    // TODO: Sleep then reconnect
                }
                unimplemented!()
            }
            Message::Subscribe(_m) => {
                unimplemented!()
            }
            Message::Books(_m) => {
                unimplemented!()
            }
        }
        Ok(())
    }
}
