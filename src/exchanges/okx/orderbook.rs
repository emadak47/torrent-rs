use super::{
    types::{DepthOrderBookEvent, Event},
    utils::get_symbol_pair,
};
use crate::exchanges::okx::flatbuffer::event_factory::make_order_book_snapshot_event;
use crate::exchanges::okx::flatbuffer::event_factory::make_order_book_update_event;
use crate::common::{Side, ASSET_CONSTANT_MULTIPLIER};
use crate::orderbook::l2::OrderbookL2;
use zenoh::config::Config as ZenohConfig;
use zenoh::prelude::sync::*;
use failure::{Error, ResultExt};
use std::collections::HashMap;
use crate::orderbook::l2::Level;
use std::ops::Div;

pub struct OkxFeedManager {
    // Maintain books for all okx symbols
    books: HashMap<String, OrderbookL2>,
    rx: tokio::sync::mpsc::UnboundedReceiver<Event>,
    zenoh_session: zenoh::Session,
}

impl OkxFeedManager {
    pub fn new(rx: tokio::sync::mpsc::UnboundedReceiver<Event>) -> Self {
        // TODO : load the config location from env var
        let mut conf = ZenohConfig::default();
        let zenoh_session = zenoh::open(conf).res().unwrap();
        OkxFeedManager {
            books: HashMap::new(),
            rx,
            zenoh_session,
        }
    }

    /// Returns a mutable reference to the [`OrderbookL2`] corresponding to the symbol key
    fn get_mut(&mut self, symb: impl Into<String>) -> Option<&mut OrderbookL2> {
        self.books.get_mut(&symb.into())
    }

    /// compute mid price for an orderbook by symbol
    fn get_mid_price(&mut self, symbol: impl Into<String>) -> Option<u64> {
        if let Some(order_book) = self.get_mut(symbol.into()) {
            let best_bid = order_book.bids.first_entry();
            let best_ask = order_book.asks.last_entry();

            if best_ask.is_some() && best_bid.is_some() {
                let best_bid = best_bid.unwrap().key().clone();
                let best_ask = best_ask.unwrap().key().clone();

                let spread = best_bid.checked_add(best_ask);
                return if spread.is_some() {
                    Some(spread.unwrap().div(2))
                } else {
                    None
                };
            }
        }
        None
    }

    /// Inserts a new [`OrderbookL2`] instance for the corresponding symbol
    /// Returns the old value, if any.
    fn insert(&mut self, symbol: &str) -> Option<OrderbookL2> {
        self.books.insert(symbol.to_string(), OrderbookL2::new())
    }

    /// Takes an update event and updates the [`OrderbookL2`] corresponding to the symbol key
    fn update(&mut self, update: DepthOrderBookEvent, is_first_update: bool) -> Result<(), Error> {
        let symbol = &update.arg.inst_id;
        let symbol_pair = get_symbol_pair(symbol);
        let multiplier = ASSET_CONSTANT_MULTIPLIER[symbol_pair.quote.as_str()];

        let mut bids_flat: Vec<Level> = Vec::new();
        let mut asks_flat: Vec<Level> = Vec::new();

        // safe unwrap cuz method caller checks that book exists
        let book = self.get_mut(symbol).unwrap();
        let zenoh_datafeed = keyexpr::new("atrimo/datafeeds").unwrap();

        for data in update.data {
            for ask in data.asks {
                let price = ask.get(0);
                let qty = ask.get(1);
                if price.is_some() && qty.is_some() {
                    let ask_price_f64 = price.unwrap().parse::<f64>().context(format!(
                        "Failed to parse an ask price: {:?}",
                        price.unwrap()
                    ))?;
                    let ask_price_u64 = (ask_price_f64 * multiplier) as u64;
                    let ask_qty_f64 = qty
                        .unwrap()
                        .parse::<f64>()
                        .context(format!("Failed to parse an ask qty: {:?}", qty.unwrap()))?;
                    let ask_qty_u64 = (ask_qty_f64 * multiplier) as u64;

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
                    let bid_price_f64 = price.unwrap().parse::<f64>().context(format!(
                        "Failed to parse an bid price: {:?}",
                        price.unwrap()
                    ))?;
                    let bid_price_u64 = (bid_price_f64 * multiplier) as u64;
                    let bid_qty_f64 = qty
                        .unwrap()
                        .parse::<f64>()
                        .context(format!("Failed to parse an bid qty: {:?}", qty.unwrap()))?;
                    let bid_qty_u64 = (bid_qty_f64 * multiplier) as u64;

                    if bid_qty_u64 == 0 {
                        book.delete(Side::BUY, bid_price_u64);
                    } else {
                        book.add(Side::BUY, bid_price_u64, bid_qty_u64);
                    }

                    bids_flat.push(Level::new(bid_price_u64, bid_qty_u64));
                }
            }
        }
        // Producing
        if is_first_update {
            println!("=========> Sending snapshot");
            let evnt = make_order_book_snapshot_event(bids_flat, asks_flat, symbol_pair);
            self.zenoh_session.put(zenoh_datafeed, evnt.buff.clone()).encoding(Encoding::APP_CUSTOM.with_suffix("snapshot_event").unwrap()).res().unwrap();
        } else {
            println!("=========> Sending update");
            let evnt = make_order_book_update_event(bids_flat, asks_flat, symbol_pair);
            self.zenoh_session.put(zenoh_datafeed, evnt.buff.clone()).encoding(Encoding::APP_CUSTOM.with_suffix("update_event").unwrap()).res().unwrap();
        }
        Ok(())
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        loop {
            match self.rx.recv().await {
                Some(event) => match event {
                    Event::DepthOrderBook(d) => match d.action.as_ref() {
                        "snapshot" => {
                            log::trace!("Inserting new okx orderbook {}", &d.arg.inst_id);
                            self.insert(&d.arg.inst_id);
                            self.update(d, true)?;
                        }
                        "update" => {
                            // log::trace!("Updating okx orderbook book: {}", &d.arg.inst_id);
                            let mid_price = self.get_mid_price(&d.arg.inst_id);
                            log::debug!("Mid Price: {:?}", mid_price);

                            self.update(d, false)?;
                        }
                        _a => log::warn!("Received an unpredicted action: {}", _a),
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
