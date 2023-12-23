use super::types::{DepthOrderBookEvent, Event};
use crate::common::{get_symbol_pair, scale, Exchange, Side, SymbolPair, DATA_FEED};
use crate::flatbuffer::event_factory::{make_snapshot_event, make_update_event};
use crate::orderbook::l2::{Level, OrderbookL2};
use failure::{Error, ResultExt};
use std::collections::HashMap;
use zenoh::{
    config::Config as ZenohConfig,
    key_expr::keyexpr,
    prelude::{sync::SyncResolve, Encoding},
};

/// Manages the state of all orderbooks that have been subscribed to
pub struct OkxFeedManager {
    /// maps each symbol to its orderbook
    books: HashMap<String, OrderbookL2>,
    /// receives updates from tx channel in [`super::spot::SpotWSClient`]
    rx: tokio::sync::mpsc::UnboundedReceiver<Event>,
    /// sends packets to aggregator
    zenoh_session: zenoh::Session,
}

impl OkxFeedManager {
    pub fn new(rx: tokio::sync::mpsc::UnboundedReceiver<Event>) -> Result<Self, zenoh::Error> {
        let conf = ZenohConfig::default();
        let zenoh_session = zenoh::open(conf).res()?;
        Ok(OkxFeedManager {
            books: HashMap::new(),
            rx,
            zenoh_session,
        })
    }

    /// Returns a mutable reference to the [`OrderbookL2`] corresponding to the symbol key
    fn get_mut<'a>(&mut self, symbol: impl Into<&'a str>) -> Option<&mut OrderbookL2> {
        self.books.get_mut(symbol.into())
    }

    /// Inserts a new [`OrderbookL2`] instance for the corresponding symbol
    /// Returns the old value, if any.
    fn insert<'a>(&mut self, symbol: impl Into<&'a str>) -> Option<OrderbookL2> {
        self.books
            .insert(symbol.into().to_string(), OrderbookL2::new())
    }

    /// Takes an update event and updates the [`OrderbookL2`] corresponding to the symbol key
    fn update(&mut self, update: DepthOrderBookEvent, is_first_update: bool) -> Result<(), Error> {
        let symbol = update.arg.inst_id.as_ref();

        // safe unwrap cuz method caller checks that book exists
        let book = self.get_mut(symbol).unwrap();

        let mut bids_flat: Vec<Level> = Vec::new();
        let mut asks_flat: Vec<Level> = Vec::new();

        for data in update.data {
            for ask in data.asks {
                let price = ask.get(0);
                let qty = ask.get(1);
                if price.is_some() && qty.is_some() {
                    let price = price.unwrap();
                    let qty = qty.unwrap();
                    let ask_price_u64 = scale(&**price)
                        .context(format!("failed to parse an ask price: {:?}", price))?;
                    let ask_qty_u64 =
                        scale(&**qty).context(format!("failed to parse an ask qty: {:?}", qty))?;

                    if ask_qty_u64 == 0 {
                        book.delete(Side::SELL, ask_price_u64);
                    } else {
                        book.add(Side::SELL, ask_price_u64, ask_qty_u64);
                    }
                    asks_flat.push(Level::new(ask_price_u64, ask_qty_u64));
                }
            }

            for bid in data.bids {
                let price = bid.get(0);
                let qty = bid.get(1);
                if price.is_some() && qty.is_some() {
                    let price = price.unwrap();
                    let qty = qty.unwrap();
                    let bid_price_u64 = scale(&**price)
                        .context(format!("failed to parse a bid price: {:?}", price))?;
                    let bid_qty_u64 =
                        scale(&**qty).context(format!("failed to parse a bid qty: {:?}", qty))?;

                    if bid_qty_u64 == 0 {
                        book.delete(Side::BUY, bid_price_u64);
                    } else {
                        book.add(Side::BUY, bid_price_u64, bid_qty_u64);
                    }

                    bids_flat.push(Level::new(bid_price_u64, bid_qty_u64));
                }
            }
        }

        let zenoh_datafeed = keyexpr::new(DATA_FEED).map_err(|e| {
            failure::err_msg(format!("failed to get zenoh key expression\n{:?}", e))
        })?;
        let pair = get_symbol_pair(SymbolPair::OkxFutures(symbol))
            .ok_or_else(|| failure::err_msg(format!("no supported pair for {:?}", symbol)))?;
        let event;
        let encoding;

        if is_first_update {
            event = make_snapshot_event(bids_flat, asks_flat, pair, Exchange::Okx)?;
            encoding = Encoding::APP_CUSTOM
                .with_suffix("snapshot_event")
                .map_err(|e| {
                    failure::err_msg(format!("failed to encode snapshot event\n{:?}", e))
                })?;
        } else {
            event = make_update_event(bids_flat, asks_flat, pair, Exchange::Okx)?;
            encoding = Encoding::APP_CUSTOM
                .with_suffix("update_event")
                .map_err(|e| failure::err_msg(format!("failed to encode update event\n{:?}", e)))?;
        }

        self.zenoh_session
            .put(zenoh_datafeed, event.buff)
            .encoding(encoding)
            .res()
            .map_err(|e| {
                failure::err_msg(format!("failed to send encoding for okx event\n{:?}", e))
            })
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        loop {
            match self.rx.recv().await {
                Some(event) => match event {
                    Event::DepthOrderBook(depth) => match depth.action.as_ref() {
                        "snapshot" => {
                            log::trace!("Inserting new okx orderbook {}", &depth.arg.inst_id);
                            self.insert(depth.arg.inst_id.as_ref());
                            self.update(depth, true)?;
                        }
                        "update" => {
                            self.update(depth, false)?;
                        }
                        _ => log::warn!("Received an unexpected action"),
                    },
                    Event::InitResponse(_) => continue,
                    Event::PublicTrade(_) => continue,
                },
                None => {
                    continue;
                }
            }
        }
    }
}
