use super::types::{OrderBookResp, Event};
use crate::common::{get_symbol_pair, scale, Exchange, Side, SymbolPair, DATA_FEED};
use crate::flatbuffer::event_factory::{make_snapshot_event, make_update_event};
use crate::orderbook::l2::{Level, OrderbookL2};
use crate::exchanges::bullish::ws::WSClient;
use failure::{Error, ResultExt};
use std::collections::HashMap;
use zenoh::{
    config::Config as ZenohConfig,
    key_expr::keyexpr,
    prelude::{sync::SyncResolve, Encoding},
};

#[derive(Debug)]
struct OrderbookMeta {
    /// stores the actual book
    book: OrderbookL2,
    /// stores the meta data status of the book for local maintenance  
    data: BullishOrderbookMetaData,
}

#[derive(Debug)]
struct BullishOrderbookMetaData {
    last_update_id: u64,
}

/// Manages the state of all orderbooks that have been subscribed to
pub struct BullishFeedManager {
    /// maps each symbol to its orderbook
    books: HashMap<String, OrderbookMeta>,
    /// receives updates from tx channel in [`super::spot::SpotWSClient`]
    rx: tokio::sync::mpsc::UnboundedReceiver<Event>,
    /// sends packets to aggregator
    zenoh_session: zenoh::Session,
}

impl BullishFeedManager {
    pub fn new(rx: tokio::sync::mpsc::UnboundedReceiver<Event>) -> Result<Self, zenoh::Error> {
        let conf = ZenohConfig::default();
        let zenoh_session = zenoh::open(conf).res()?;

        Ok(BullishFeedManager {
            books: HashMap::new(),
            rx,
            zenoh_session,
        })
    }
    
    /// get [`OrderbookL2`] by symbol
    fn get<'a>(&self, symbol: impl Into<&'a str>) -> Option<&OrderbookL2> {
        match self.books.get(symbol.into()) {
            Some(order_book_meta) => Some(&order_book_meta.book),
            None => None,
        }
    }

    /// Returns a mutable reference to the [`OrderbookMeta`] corresponding to the symbol key
    fn get_order_book_meta<'a>(
        &mut self,
        symbol: impl Into<&'a str>,
    ) -> Option<&mut OrderbookMeta> {
        self.books.get_mut(symbol.into())
    }

    /// Inserts a new [`OrderbookMeta`] instance for the corresponding symbol
    /// Returns the old value, if any
    fn insert<'a>(&mut self, symbol: impl Into<&'a str>) -> Option<OrderbookMeta> {
        self.books.insert(
            symbol.into().to_string(),
            OrderbookMeta {
                book: OrderbookL2::new(),
                data: BullishOrderbookMetaData {
                    last_update_id: 0,
                },
            },
        )
    }

    /// Takes an update event and updates the [`OrderbookL2`] corresponding to the symbol key
    fn update(&mut self, update: OrderBookResp, is_first_update: bool) -> Result<(), Error> {
        let symbol = update.data.symbol.as_ref();

        // safe unwrap cuz method caller checks that book exists
        let meta = self.get_order_book_meta(symbol).unwrap();

        if !is_first_update {
            if update.data.sequence_number_range[0] <= meta.data.last_update_id + 1 && meta.data.last_update_id  <= update.data.sequence_number_range[1] {
                meta.data.last_update_id = update.data.sequence_number_range[1];
            } else {
                // skip the update 
                return Ok(());
            }
        }

        let mut bids_flat: Vec<Level> = Vec::new();
        let mut asks_flat: Vec<Level> = Vec::new();

        if (!update.data.bids.is_empty()) {
            for i in (0..update.data.asks.len()).step_by(2) {
                let price = &update.data.bids[i];
                let qty = &update.data.bids[i + 1];
                if !price.is_empty() && !qty.is_empty() {

                    let ask_price_u64 = scale(&**price)
                        .context(format!("failed to parse an ask price: {:?}", price))?;
                    let ask_qty_u64 =
                        scale(&**qty).context(format!("failed to parse an ask qty: {:?}", qty))?;
                    if ask_qty_u64 == 0 {
                        meta.book.delete(Side::SELL, ask_price_u64);
                    } else {
                        meta.book.add(Side::SELL, ask_price_u64, ask_qty_u64);
                    }
                    asks_flat.push(Level::new(ask_price_u64, ask_qty_u64));
                }
            }
        }

        if (!update.data.asks.is_empty()) {
            for i in (0..update.data.asks.len()).step_by(2) {
                let price = &update.data.asks[i];
                let qty = &update.data.asks[i + 1];
                if !price.is_empty() && !qty.is_empty() {
                    let bid_price_u64 = scale(&**price)
                        .context(format!("failed to parse a bid price: {:?}", price))?;
                    let bid_qty_u64 =
                        scale(&**qty).context(format!("failed to parse a bid qty: {:?}", qty))?;
                    if bid_qty_u64 == 0 {
                        meta.book.delete(Side::BUY, bid_price_u64);
                    } else {
                        meta.book.add(Side::BUY, bid_price_u64, bid_qty_u64);
                    }

                    bids_flat.push(Level::new(bid_price_u64, bid_qty_u64));
                }
            }
        }

        let zenoh_datafeed = keyexpr::new(DATA_FEED).map_err(|e| {
            failure::err_msg(format!("failed to get zenoh key expression\n{:?}", e))
        })?;
        let pair = get_symbol_pair(SymbolPair::BullishFutures(symbol))
            .ok_or_else(|| failure::err_msg(format!("no supported pair for {:?}", symbol)))?;
        let event;
        let encoding;

        if is_first_update {
            event = make_snapshot_event(bids_flat, asks_flat, pair, Exchange::Bullish)?;
            encoding = Encoding::APP_CUSTOM
                .with_suffix("snapshot_event")
                .map_err(|e| {
                    failure::err_msg(format!("failed to encode snapshot event\n{:?}", e))
                })?;
        } else {
            event = make_update_event(bids_flat, asks_flat, pair, Exchange::Bullish)?;
            encoding = Encoding::APP_CUSTOM
                .with_suffix("update_event")
                .map_err(|e| failure::err_msg(format!("failed to encode update event\n{:?}", e)))?;
        }

        self.zenoh_session
            .put(zenoh_datafeed, event.buff)
            .encoding(encoding)
            .res()
            .map_err(|e| {
                failure::err_msg(format!("failed to send encoding for bullish event\n{:?}", e))
            })
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        loop {
            match self.rx.recv().await {
                Some(event) => match event {
                    Event::OrderBookMsg(depth) => match depth.r#type.as_ref() {
                        "snapshot" => {
                            log::trace!("Inserting new bullish orderbook {}", &depth.data.symbol);
                            self.insert(depth.data.symbol.as_ref());
                            self.update(depth, true)?;
                        }
                        "update" => {
                            self.update(depth, false)?;
                        }
                        _ => log::warn!("Received an unexpected action"),
                    },
                },
                None => {
                    continue;
                }
            }
        }
    }
}
