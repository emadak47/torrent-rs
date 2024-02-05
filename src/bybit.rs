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
use regex::Regex;
use std::result;
use zenoh::prelude::sync::SyncResolve;
use zenoh::prelude::Encoding;

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum Message {
    Failure(FailureMessage),
    Books(BasePublicResponse<Orderbook>),
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
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
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

    fn visit_seq<A>(self, mut seq: A) -> result::Result<Self::Value, A::Error>
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
pub struct FailureMessage {
    pub event: String,
    pub code: String,
    pub msg: String,
    pub conn_id: String,
}

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

/// The orderbook data.
#[derive(Deserialize, Debug)]
pub struct Orderbook{
    /// Symbol name.
    pub s: String,
    /// Bids. For `snapshot` stream, the element is sorted by price in descending order.
    pub b: Vec<LevelUpdate>,
    /// Asks. For `snapshot` stream, the element is sorted by price in ascending order.
    pub a: Vec<LevelUpdate>,
    /// Update ID. Is a sequence.
    /// Occasionally, you'll receive "u"=1, which is a snapshot data due to the restart of the service.
    /// So please overwrite your local orderbook.
    pub u: u64,
    /// Cross sequence. Option does not have this field.
    pub seq: Option<u64>,
}

#[derive(Debug, Serialize)]
struct Sub {
    op: String,
    args: Vec<String>,
}

impl Sub {
    pub(crate) fn new(op: String, args: Vec<String>) -> Self {
        Self { op, args}
    }
}


#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, Debug)]
pub enum Channel {
    // (depth, symbol)
    Orderbook(u8, String),
}

impl Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Channel::Orderbook(depth, symbol) => write!(f, "orderbook.{}.{}", depth, symbol),
        }
    }
}

#[derive(Default, Debug)]
pub struct Bybit;

impl Display for Bybit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Bybit")
    }
}

impl Bybit {
    
    pub const URL: &'static str = "wss://stream.bybit.com/v5/public/linear";

    pub fn new() -> Self {
        Self {}
    }
}

impl Wss for Bybit {
    fn subscribe(&mut self, channel: String, topics: Vec<String>) -> Result<String> {
        
        let sub = Sub::new(
            "subscribe".to_string(),
            vec![channel],
        );

        match serde_json::to_string(&sub) {
            Ok(s) => Ok(s),
            Err(e) => Err(TorrentError::BadParse(format!("serde parse error: {}", e))),
        }
    }

    fn to_enum(&self) -> Exchange {
        Exchange::BYBIT
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
                let symbol = update.data.s;
                if update.type_ == "snapshot" {
                    is_snapshot = true;
                        let _ = self.transmit(symbol, update.data.b, update.data.a, is_snapshot);
                } else {
                    let _ = self.transmit(symbol, update.data.b, update.data.a, is_snapshot);
                }

            }
            _ => {}
        }
        Ok(())
    }
}

impl Transmitor<Vec<LevelUpdate>> for Manager {
    fn resolve_symbol(&self, symbol: &Symbol) -> Option<CcyPair> {
        // Create the regex pattern
        let re = Regex::new(r"^(\w+)(BTC|TRY|ETH|BNB|USDT|PAX|TUSD|USDC|XRP|USDS)$").unwrap();
        let symbol_str: &str = symbol;
        // Check for matches in the symbol
        if let Some(captures) = re.captures(symbol_str) {
            if let (Some(base), Some(quote)) = (captures.get(1), captures.get(2)) {
                return Some(CcyPair {
                    base: base.as_str().to_string(),
                    quote: quote.as_str().to_string(),
                    product: "futures".to_string(),
                });
            }
        }

        // If the quote is not found, panic with an error message
        panic!(
            "Quote not found in the symbol: {}, Please add before running again",
            symbol
        );
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
            .unwrap_or_else(|| panic!("{symbol} is not supported for Bybit"));

        let (event, encoding) = if is_snapshot {
            let event = make_snapshot_event(bids, asks, ccy_pair, Exchange::BYBIT)
                .map_err(|e| TorrentError::BadZenoh(e.to_string()))?;
            let encoding = Encoding::APP_CUSTOM
                .with_suffix("snapshot_event")
                .map_err(|e| TorrentError::BadZenoh(e.to_string()))?;
            (event, encoding)
        } else {
            let event = make_update_event(bids, asks, ccy_pair, Exchange::BYBIT)
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