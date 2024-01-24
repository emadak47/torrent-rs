use crate::aggregator::Transmitor;
use crate::flatbuffer::event_factory::{make_snapshot_event, make_update_event};
use crate::orderbook::l2::Level;
use crate::utils::{
    CcyPair, Exchange, Result, Symbol, TorrentError, ASSET_CONSTANT_MULTIPLIER, DATA_FEED,
};
use crate::websocket::{MessageCallback, Wss};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use std::ops::Deref;
use std::result;
use zenoh::prelude::sync::SyncResolve;
use zenoh::prelude::Encoding;

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum Message {
    Failure(FailureMessage),
    Books(OrderBookResp),
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
        let mut i: usize = 0;

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
pub struct FailureMessage {
    pub event: String,
    pub code: String,
    pub msg: String,
    pub conn_id: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Level2Changes {
    pub symbol: String,
    pub bids: Vec<LevelUpdate>, 
    pub asks: Vec<LevelUpdate>, 
    pub sequence_number_range: [u64; 2], 
    pub datetime: String,
    pub timestamp: String,
    pub published_at_timestamp: String, 
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct OrderBookResp {
    pub r#type: String,
    pub data_type: String,
    pub data: Level2Changes,
}

#[derive(Debug, Serialize)]
pub struct Subscription {
    jsonrpc: String,
    r#type: String,
    method: String,
    params: Option<SubscriptionParams>,
    id: u64,
}

#[derive(Debug, Serialize)]
pub struct SubscriptionParams {
    topic: String,
    symbol: String,
}

impl Subscription {
    pub(crate) fn new(op: String, topic: &str, symbol: &str, id: u64) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            r#type: "command".to_string(),
            method: op,
            params: Some(SubscriptionParams { topic: topic.to_string(), symbol: symbol.to_string() }),
            id,
        }
    }
}


#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, Debug)]
pub enum Channel {
    BOOKS,
}

impl Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Channel::BOOKS => write!(f, "l2Orderbook"),
        }
    }
}

#[derive(Default, Debug)]
pub struct Bullish;

impl Display for Bullish {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Bullish")
    }
}

impl Bullish {
    // TODO: later concatenate systematically
    pub const URL: &'static str = "wss://api.exchange.bullish.com/trading-api/v1/market-data/orderbook";

    pub fn new() -> Self {
        Self {}
    }
}

impl Wss for Bullish {
    fn subscribe(&mut self, channel: String, topics: Vec<String>) -> Result<String> {
        
        let sub = Subscription::new(
            "subscribe".to_string(),
            channel.as_str(),
            /* TODO : Doesnt look right */
            topics[0].as_str(),
            1,
        );
        
        match serde_json::to_string(&sub) {
            Ok(s) => Ok(s),
            Err(e) => Err(TorrentError::BadParse(format!("serde parse error: {}", e))),
        }
    }

    fn to_enum(&self) -> Exchange {
        Exchange::BULLISH
    }
}

#[derive(Debug)]
pub struct Manager {
    zenoh: zenoh::Session,
}

impl Default for Manager {
    fn default() -> Self {
        let config = zenoh::config::default();
        let session = zenoh::open(config)
            .res()
            .unwrap_or_else(|e| panic!("Couldn't open zenoh session: {e}"));
        Self { zenoh: session }
    }
}

impl Manager {
    pub fn new() -> Self {
        Default::default()
    }
}

impl MessageCallback<Message> for Manager {
    fn message_callback(&mut self, msg: Result<Message>) -> Result<()> {
        match msg? {
            Message::Failure(m) => {
                if m.msg.contains("Invalid request") {
                    return Err(TorrentError::BadRequest(format!("{:?}", m)));
                } else if m.msg.contains("Connection refused") {
                    // TODO: Sleep then reconnect
                    panic!("{:?}", m);
                }
                // TODO: handle it better
                panic!("{:?}", m);
            }
            Message::Books(update) => {
                let mut is_snapshot = false;
                let symbol = update.data.symbol;
                if update.r#type == "snapshot" {
                    is_snapshot = true;
                        let _ = self.transmit(symbol, update.data.bids, update.data.asks, is_snapshot);
                } else {
                    let _ = self.transmit(symbol, update.data.bids, update.data.asks, is_snapshot);
                }

            }
            _ => {}
        }
        Ok(())
    }
}

impl Transmitor<Vec<LevelUpdate>> for Manager {
    fn resolve_symbol(&self, symbol: &Symbol) -> Option<CcyPair> {
        let parts = symbol.split('-').collect::<Vec<&str>>();
        if parts.len() == 2 {
            Some(CcyPair {
                base: parts[0].to_string(),
                quote: parts[1].to_string(),
                product: "spot".to_string(),
            })
        } else {
            None
        }
    }

    fn standardise_updates(&self, updates: Vec<LevelUpdate>) -> Vec<Level> {
        updates
            .into_iter()
            .map(|update| {
                let update = *update;
                let update_0 = (update[0] * ASSET_CONSTANT_MULTIPLIER) as u64;
                let update_1 = (update[1] * ASSET_CONSTANT_MULTIPLIER) as u64;
                Level::new(update_0, update_1)
            })
            .collect()
    }

    fn transmit(
        &self,
        symbol: Symbol,
        bids: Vec<LevelUpdate>,
        asks: Vec<LevelUpdate>,
        is_snapshot: bool,
    ) -> Result<()> {
        let bids = self.standardise_updates(bids);
        let asks = self.standardise_updates(asks);
        let ccy_pair = self
            .resolve_symbol(&symbol)
            .unwrap_or_else(|| panic!("{symbol} is not supported for Bullish"));

        let (event, encoding) = if is_snapshot {
            let event = make_snapshot_event(bids, asks, ccy_pair, Exchange::BULLISH)
                .map_err(|e| TorrentError::BadZenoh(e.to_string()))?;
            let encoding = Encoding::APP_CUSTOM
                .with_suffix("snapshot_event")
                .map_err(|e| TorrentError::BadZenoh(e.to_string()))?;
            (event, encoding)
        } else {
            let event = make_update_event(bids, asks, ccy_pair, Exchange::BULLISH)
                .map_err(|e| TorrentError::BadZenoh(e.to_string()))?;
            let encoding = Encoding::APP_CUSTOM
                .with_suffix("update_event")
                .map_err(|e| TorrentError::BadZenoh(e.to_string()))?;
            (event, encoding)
        };

        let datafeed = zenoh::key_expr::keyexpr::new(DATA_FEED)
            .map_err(|e| TorrentError::BadZenoh(e.to_string()))?;
        self.zenoh
            .put(datafeed, event.buff)
            .encoding(encoding)
            .res()
            .map_err(|e| TorrentError::BadZenoh(e.to_string()))
    }
}
