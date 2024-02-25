use crate::aggregator::Transmitor;
use crate::flatbuffer::{make_snapshot_event, make_update_event};
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
    Subscribe(SubscribeMessage),
    Orderbook(PublicResponse<OrderbookEvent>),
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

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
pub struct SubscribeMessage {
    success: bool,
    ret_msg: String,
    conn_id: String,
    op: String,
    req_id: Option<String>,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
pub struct OrderbookEvent {
    /// Symbol name
    pub s: String,
    /// Bids. For `snapshot` stream, the element is sorted by price in descending order
    pub b: Vec<LevelUpdate>,
    /// Asks. For `snapshot` stream, the element is sorted by price in ascending order
    pub a: Vec<LevelUpdate>,
    /// Update ID, which is a sequence. Occasionally, you'll receive "u"=1, which is a
    /// snapshot data due to the restart of the service
    u: u64,
    /// Cross sequence.
    /// You can use this field to compare different levels orderbook data, and for the smaller seq,
    /// then it means the data is generated earlier
    pub seq: Option<u64>,
}

#[derive(Deserialize, Debug)]
pub struct PublicResponse<Data> {
    /// Topic name
    pub topic: String,
    /// Data type: `snapshot`, `delta`
    pub r#type: String,
    /// The timestamp in (ms) that the system generates the data
    pub ts: u64,
    /// Type to deserialise data from `Bybit` websocket stream
    pub data: Data,
    /// The timestamp from the match engine when this orderbook data is produced
    /// It can be correlated with T from public trade channel
    pub cts: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Subscription {
    req_id: String,
    op: String,
    args: Vec<String>,
}

#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, Debug)]
pub enum Depth {
    ONE,
    FIFTY,
    TWO_HUNDRED,
}

#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, Debug)]
pub enum Channel {
    ORDERBOOK(Depth),
}

impl Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Channel::ORDERBOOK(depth) => match depth {
                Depth::ONE => write!(f, "orderbook.1"),
                Depth::FIFTY => write!(f, "orderbook.50"),
                Depth::TWO_HUNDRED => write!(f, "orderbook.200"),
            },
        }
    }
}

#[derive(Default, Debug)]
pub struct Bybit(usize);

impl Display for Bybit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Bybit")
    }
}

impl Bybit {
    pub const URL: &'static str = "wss://stream.bybit.com/v5/public/spot";

    pub fn new() -> Self {
        Self(0)
    }
}

impl Wss for Bybit {
    fn subscribe(&mut self, channel: String, topics: Vec<String>) -> Result<String> {
        let args = topics
            .into_iter()
            .filter_map(|t| {
                let parts = t.split('-').collect::<Vec<&str>>();
                if parts.len() == 2 {
                    Some(parts.join(""))
                } else {
                    None
                }
            })
            .map(|sym| format!("{}.{}", channel, sym.to_uppercase()))
            .collect::<Vec<String>>();
        let sub = Subscription {
            req_id: self.0.to_string(),
            op: "subscribe".to_string(),
            args,
        };
        self.0 += 1;

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
            Message::Orderbook(update) => {
                let is_snapshot = update.r#type == "snapshot";
                let symbol = update.data.s;
                let _ = self.transmit(symbol, update.data.b, update.data.a, is_snapshot);
            }
            Message::Subscribe(_) => {}
        }
        Ok(())
    }
}

impl Transmitor<Vec<LevelUpdate>> for Manager {
    fn resolve_symbol(&self, symbol: &Symbol) -> Option<CcyPair> {
        let regex =
            regex::Regex::new(r"^(\w+)(BTC|TRY|ETH|BNB|USDT|PAX|TUSD|USDC|XRP|USDS)$").ok()?;
        let capture = regex.captures(symbol)?;
        let (base, quote) = (capture.get(1)?, capture.get(2)?);

        return Some(CcyPair {
            base: base.as_str().to_string(),
            quote: quote.as_str().to_string(),
            product: "spot".to_string(),
        });
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
