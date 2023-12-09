use zenoh::config::Config as ZenohConfig;
use zenoh::prelude::sync::*;
use failure::{Error, ResultExt};
use std::collections::HashMap;
use crate::orderbook::l2::Level;
use std::ops::Div;
use std::thread;

use crate::orderbook::l2::OrderbookL2;
use crate::exchanges::gate::types::Event;
use crate::common::ASSET_CONSTANT_MULTIPLIER;
use crate::exchanges::gate::utils::get_symbol_pair;
use crate::common::Side;

use super::types::Level2;
use crate::exchanges::gate::types::OrderBook;
use crate::exchanges::gate::client::Client;
use crate::exchanges::gate::api::API;
use crate::exchanges::gate::api::Spot;
use super::{super::config::Config};


struct OrderbookMeta {
    book: OrderbookL2,
    data: GateOrderbookMetaData,
}

struct GateOrderbookMetaData {
    last_update_id: u64,
    is_first_update: bool,
}

pub struct GateFeedManager {
    // Maintain books for all gate symbols
    books: HashMap<String, OrderbookMeta>,
    rx: tokio::sync::mpsc::UnboundedReceiver<Event>,
    zenoh_session: zenoh::Session,
}

impl GateFeedManager {
    pub fn new(rx: tokio::sync::mpsc::UnboundedReceiver<Event>) -> Self {
        // TODO : load the config location from env var
        let mut conf = ZenohConfig::default();
        let zenoh_session = zenoh::open(conf).res().unwrap();
        GateFeedManager {
            books: HashMap::new(),
            rx,
            zenoh_session,
        }
    }

    /// compute mid price for an orderbook by symbol
    fn get_mid_price(&mut self, symbol: impl Into<String>) -> Option<u64> {
        if let Some(order_book_meta) = self.get_order_book_meta(&symbol.into()) {
            let book = &mut order_book_meta.book;
            let best_bid = book.bids.first_entry();
            let best_ask = book.asks.last_entry();

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

    /// Returns a mutable reference to the [`OrderbookMeta`] corresponding to the symbol key
    fn get_order_book_meta(&mut self, symbol: impl Into<String>) -> Option<&mut OrderbookMeta> {
        self.books.get_mut(&symbol.into())
    }

    /// get [`OrderbookL2`] by symbol
    fn get(&self, symbol: impl Into<String>) -> Option<&OrderbookL2> {
        match self.books.get(&symbol.into()) {
            Some(order_book_meta) => Some(&order_book_meta.book),
            None => None,
        }
    }

    /// Inserts a new [`OrderbookMeta`] instance for the corresponding symbol
    /// Returns the old value, if any
    fn insert(&mut self, symbol: &str) -> Option<OrderbookMeta> {
        self.books.insert(
            symbol.to_string(),
            OrderbookMeta {
                book: OrderbookL2::new(),
                data: GateOrderbookMetaData {
                    last_update_id: 0,
                    is_first_update: true,
                },
            },
        )
    }

    /// Updates the [`OrderbookMeta`] of the corresponding symbol key  
    fn refresh(
        &mut self,
        symbol: String,
        book: OrderBook,
        is_first_update: bool,
    ) -> Result<(), Error> {
        let mut bids_flat: Vec<Level> = Vec::new();
        let mut asks_flat: Vec<Level> = Vec::new();

        let symbol_pair = get_symbol_pair(&symbol);
        let multiplier = ASSET_CONSTANT_MULTIPLIER[symbol_pair.quote.as_str()];
        // TODO: remove from the hot path
        let zenoh_datafeed = keyexpr::new("atrimo/datafeeds").unwrap();

        // safe unwrap cuz method caller checks that book exists
        let entry = self.get_order_book_meta(&symbol).unwrap();
        entry.data.is_first_update = is_first_update;
        entry.data.last_update_id = book.id;

        for ask in book.asks {
            let ask_price_f64 = ask
                .get(0).unwrap()
                .parse::<f64>()
                .context(format!("Failed to parse an ask price: {:?}", ask.get(0)))?;
            let ask_price_u64 = (ask_price_f64 * multiplier) as u64;
            let ask_qty_f64 = ask
                .get(1).unwrap()
                .parse::<f64>()
                .context(format!("Failed to parse an ask qty: {:?}", ask.get(1)))?;
            let ask_qty_u64 = (ask_qty_f64 * multiplier) as u64;

            if ask_qty_u64 == 0 {
                entry.book.delete(Side::SELL, ask_price_u64);
            } else {
                entry.book.add(Side::SELL, ask_price_u64, ask_qty_u64);
            }

            asks_flat.push(Level::new(ask_price_u64, ask_qty_u64));
        }

        for bid in book.bids {
            let bid_price_f64 = bid
                .get(0).unwrap()
                .parse::<f64>()
                .context(format!("Failed to parse an bid price: {:?}", bid.get(0)))?;
            let bid_price_u64 = (bid_price_f64 * multiplier) as u64;
            let bid_qty_f64 = bid
                .get(1).unwrap()
                .parse::<f64>()
                .context(format!("Failed to parse an bid qty: {:?}", bid.get(1)))?;
            let bid_qty_u64 = (bid_qty_f64 * multiplier) as u64;

            if bid_qty_u64 == 0 {
                entry.book.delete(Side::BUY, bid_price_u64);
            } else {
                entry.book.add(Side::BUY, bid_price_u64, bid_qty_u64);
            }

            bids_flat.push(Level::new(bid_price_u64, bid_qty_u64));
        }

        // if is_first_update {
        //     println!("=========> Sending snapshot");
        //     let evnt = make_order_book_snapshot_event(bids_flat, asks_flat, symbol_pair);
        //     self.zenoh_session.put(zenoh_datafeed, evnt.buff.clone()).encoding(Encoding::APP_CUSTOM.with_suffix("snapshot_event").unwrap()).res().unwrap();
        // } else {
        //     println!("=========> Sending update");
        //     let evnt = make_order_book_update_event(bids_flat, asks_flat, symbol_pair);
        //     self.zenoh_session.put(zenoh_datafeed, evnt.buff.clone()).encoding(Encoding::APP_CUSTOM.with_suffix("update_event").unwrap()).res().unwrap();
        // }
        Ok(())
    }


    // Takes an update event, and either `reset` or `refresh` of the [`OrderbookMeta`] corresponding to the symbol key.   
    async fn update(&mut self, update: Level2) -> Result<(), Error> {
        let symbol = update.result.s.clone();
        let entry = self.get_order_book_meta(&symbol).unwrap();
        
        // skip older messages
        if update.result.u < entry.data.last_update_id + 1 {
            return Ok(());
        }

        // some updates are lost reconstruct again
        if update.result.U > entry.data.last_update_id + 1 {
            self.reset(&symbol, None).await?;
            return Ok(());
        }

        if update.result.U <= entry.data.last_update_id + 1 && update.result.u >= entry.data.last_update_id + 1 {
            let order_book = OrderBook {
                id: update.result.u,
                current: 0,
                update: 0,
                bids: update.result.b,
                asks: update.result.a,
            };
            self.refresh(symbol, order_book, false)?;
            return Ok(());
        }

        Ok(())
    }

    /// Request a snapshot of the orderbook using Binance REST API  
    async fn reset(
        &mut self,
        symbol: &str,
        timeout: Option<std::time::Duration>,
    ) -> Result<(), Error> {
        let start = std::time::Instant::now();
        loop {
            let config = Config::gate();
            match Client::new("api_key", "secret_key", config.spot_rest_api_endpoint)
                .get::<OrderBook>(API::Spot(Spot::OrderBookSnapshot), Some(format!("currency_pair={}&limit=100&with_id=true", symbol)))
                .await
            {
                Ok(book) => {
                    self.refresh(symbol.to_string(), book, true)?;
                    return Ok(());
                }

                Err(e) => {
                    log::error!(
                        "Error requesting binance orderbook snapshot {}",
                        e.to_string()
                    );

                    if let Some(to) = timeout {
                        if start.elapsed() <= to {
                            thread::sleep(std::time::Duration::from_secs(1));
                        } else {
                            // TODO: complete re-request snapshot
                            // return Err(Error::from(e).context("Failed to get a binance orderbook snapshot"))?;
                        }
                    } else {
                        if start.elapsed() > std::time::Duration::from_secs(30) {
                            log::warn!("Still can't get binance orderbook snapshot");
                        }
                    }
                }
            }
        }
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        let mut symbol = String::new();
        loop {
            match self.rx.recv().await {
                Some(event) => match event {
                    Event::Level2(d) => {
                        symbol = d.result.s.clone();
                        if let Some(book) = self.get(&symbol) {
                            if book.is_empty() {
                                log::trace!("Resetting binance orderbook book: {}", &symbol);
                                self.reset(&symbol, None).await?;
                            } else {
                                // log::trace!("Updating binance orderbook book: {}", &symbol);
                                self.update(d).await?;
                            }
                        } else {
                            log::trace!("Inserting new binance orderbook {}", &symbol);
                            self.insert(&symbol);
                            self.reset(&symbol, None).await?;
                            continue;
                        }

                        let mid_price = self.get_mid_price(symbol);
                        println!("mid price {:?}", mid_price);
                    }
                },
                None => {
                    continue;
                }
            }
        }
    }
}