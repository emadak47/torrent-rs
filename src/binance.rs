use crate::aggregator::Transmitor;
use crate::flatbuffer::event_factory::{make_snapshot_event, make_update_event};
use crate::orderbook::l2::Level;
use crate::utils::{
    CcyPair, Exchange, Result, Symbol, TorrentError, ASSET_CONSTANT_MULTIPLIER, DATA_FEED,
};
use crate::websocket::{DepthCallback, Wss};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::ops::Deref;
use std::result;
use zenoh::prelude::sync::SyncResolve;
use zenoh::prelude::Encoding;

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

impl Display for Channel {
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

impl Display for Binance {
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
    fn to_enum(&self) -> Exchange {
        Exchange::BINANCE
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
    pub last_update_id: u64,
    pub bids: Vec<LevelUpdate>,
    pub asks: Vec<LevelUpdate>,
}

impl Display for RequestError {
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

#[derive(Debug)]
struct Metadata {
    small_u: u64,
    is_first_update: bool,
}

impl Metadata {
    fn new(small_u: u64, is_first_update: bool) -> Self {
        Self {
            small_u,
            is_first_update,
        }
    }
}

#[derive(Debug)]
pub struct Manager {
    zenoh: zenoh::Session,
    metadata_mp: HashMap<Symbol, Metadata>,
    snapshots_mp: HashMap<Symbol, Option<DepthSnapshot>>,
}

impl Default for Manager {
    fn default() -> Self {
        let config = zenoh::config::default();
        let session = zenoh::open(config)
            .res()
            .unwrap_or_else(|e| panic!("Couldn't open zenoh session: {e}"));
        Self {
            zenoh: session,
            metadata_mp: HashMap::default(),
            snapshots_mp: HashMap::default(),
        }
    }
}

impl Manager {
    pub fn new() -> Self {
        Default::default()
    }
}

impl DepthCallback<Message, DepthSnapshot> for Manager {
    const REST_URL: &'static str = "https://api.binance.com";

    fn depth_callback(
        &mut self,
        msg: Result<Message>,
        snapshots_mp: Option<HashMap<Symbol, DepthSnapshot>>,
    ) {
        if let Some(snapshots_mp) = snapshots_mp {
            for (symbol, snapshot) in snapshots_mp {
                let metadata = Metadata::new(0, true);
                self.metadata_mp.insert(symbol.clone(), metadata);
                self.snapshots_mp.insert(symbol, Some(snapshot));
            }
        }

        match msg {
            Ok(msg) => {
                // https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly
                if let Message::Depth(update) = msg {
                    let symbol = &update.symbol;
                    let small_u = &update.final_update_id;
                    let big_u = &update.first_update_id;

                    let metadata = self.metadata_mp.get_mut(symbol).unwrap_or_else(|| {
                        panic!("Received updates for {symbol} which was not subscribed to")
                    });
                    let is_first_update = metadata.is_first_update;
                    let previous_small_u = metadata.small_u;

                    let mut bids_buff = vec![];
                    let mut asks_buff = vec![];

                    if is_first_update {
                        // - `is_first_update` and `maybe_snapshot.is_some()` will be true together
                        // (only once; at first) and false together all the time
                        // - `maybe_snapshot` is placed inside the if block to avoid an additional
                        // unnecessary hashmap lookup (at the expense of readability)
                        let maybe_snapshot =
                            self.snapshots_mp.get_mut(symbol).unwrap_or_else(|| {
                                panic!("Received updates for {symbol} which was not subscribed to")
                            });

                        // 4. Drop (skip) event where u <= lastUpdateId
                        // 5. U <= lastUpdateId + 1 AND u >= lastUpdateId
                        #[allow(clippy::blocks_in_if_conditions)]
                        if maybe_snapshot.as_ref().is_some_and(|snapshot| {
                            let cond = snapshot.last_update_id + 1;
                            (*big_u <= cond) && (*small_u >= cond)
                        }) {
                            let snapshot = maybe_snapshot.take().unwrap();
                            bids_buff = snapshot.bids;
                            asks_buff = snapshot.asks;
                            *maybe_snapshot = None;
                            metadata.is_first_update = false;
                            metadata.small_u = *small_u;
                        }
                    // 6. New event's U == previous event's u + 1
                    } else if *big_u == previous_small_u + 1 {
                        bids_buff = update.bids;
                        asks_buff = update.asks;
                        metadata.small_u = *small_u;
                    }

                    if bids_buff.is_empty() && asks_buff.is_empty() {
                        // TODO: handle error
                        let _ = self.transmit(symbol, bids_buff, asks_buff);
                    }
                }
            }
            Err(e) => eprintln!("{:?}", e),
        }
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
        symbol: &Symbol,
        bids: Vec<LevelUpdate>,
        asks: Vec<LevelUpdate>,
    ) -> Result<()> {
        let bids = self.standardise_updates(bids);
        let asks = self.standardise_updates(asks);
        let ccy_pair = self
            .resolve_symbol(symbol)
            .unwrap_or_else(|| panic!("{symbol} is not supported for Binance"));

        let is_first_update = self
            .metadata_mp
            .get(symbol)
            .unwrap_or_else(|| panic!("{symbol} must exist in map if transmit was called"))
            .is_first_update;
        let (event, encoding) = if is_first_update {
            let event = make_snapshot_event(bids, asks, ccy_pair, Exchange::BINANCE)
                .map_err(|e| TorrentError::BadZenoh(e.to_string()))?;
            let encoding = Encoding::APP_CUSTOM
                .with_suffix("snapshot_event")
                .map_err(|e| TorrentError::BadZenoh(e.to_string()))?;
            (event, encoding)
        } else {
            let event = make_update_event(bids, asks, ccy_pair, Exchange::BINANCE)
                .map_err(|e| TorrentError::BadZenoh(e.to_string()))?;
            let encoding = Encoding::APP_CUSTOM
                .with_suffix("snapshot_event")
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
