use super::{
    super::config::Config,
    api::{Spot, API},
    client::Client as BinanceClient,
    types::{DepthOrderBookEvent, Event, OrderBook},
    utils::get_symbol_pair,
};
use crate::common::{Side, ASSET_CONSTANT_MULTIPLIER};
use crate::orderbook::l2::OrderbookL2;
use failure::{Error, ResultExt};
use std::{collections::HashMap, ops::Div, thread};

/// BinanceFuturesUsd: How To Manage A Local OrderBook Correctly: Step 6:
/// "While listening to the stream, each new event's U should be equal to the
///  previous event's u+1, otherwise initialize the process from step 3."
///
/// See docs: <https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly>
fn validate_next_update(update: &DepthOrderBookEvent, last_update_id: u64) -> bool {
    let expected_next_id = last_update_id + 1;
    if update.first_update_id == expected_next_id {
        // do not skip the update
        return false;
    } else {
        // request snapshot
        return true;
    }
}

fn validate_first_update(update: &DepthOrderBookEvent, last_update_id: u64) -> bool {
    let expected_next_id = last_update_id + 1;
    if update.first_update_id <= expected_next_id && update.final_update_id >= expected_next_id {
        // not skip the update
        return false;
    } else {
        // request snapshot
        return true;
    }
}

/// Binance orderbook metadata for local orderbook maintenance
struct OrderbookMeta {
    book: OrderbookL2,
    data: BinanceOrderbookMetaData,
}

struct BinanceOrderbookMetaData {
    last_update_id: u64,
    is_first_update: bool,
}

pub struct BinanceFeedManager {
    // Maintain books for all binance symbols
    books: HashMap<String, OrderbookMeta>,
    rx: tokio::sync::mpsc::UnboundedReceiver<Event>,
}

impl BinanceFeedManager {
    pub fn new(rx: tokio::sync::mpsc::UnboundedReceiver<Event>) -> Self {
        BinanceFeedManager {
            books: HashMap::new(),
            rx,
        }
    }

    /// get [`OrderbookL2`] by symbol
    fn get(&self, symbol: impl Into<String>) -> Option<&OrderbookL2> {
        match self.books.get(&symbol.into()) {
            Some(order_book_meta) => Some(&order_book_meta.book),
            None => None,
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
    fn get_order_book_meta(&mut self, symbol: &str) -> Option<&mut OrderbookMeta> {
        self.books.get_mut(symbol)
    }

    /// Inserts a new [`OrderbookMeta`] instance for the corresponding symbol
    /// Returns the old value, if any
    fn insert(&mut self, symbol: &str) -> Option<OrderbookMeta> {
        self.books.insert(
            symbol.to_string(),
            OrderbookMeta {
                book: OrderbookL2::new(),
                data: BinanceOrderbookMetaData {
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
        let symbol_pair = get_symbol_pair(&symbol);
        let multiplier = ASSET_CONSTANT_MULTIPLIER[symbol_pair.quote.as_str()];

        // safe unwrap cuz method caller checks that book exists
        let entry = self.get_order_book_meta(&symbol).unwrap();
        entry.data.is_first_update = is_first_update;
        entry.data.last_update_id = book.last_update_id;

        for ask in book.asks {
            let ask_price_f64 = ask
                .price
                .parse::<f64>()
                .context(format!("Failed to parse an ask price: {:?}", ask.price))?;
            let ask_price_u64 = (ask_price_f64 * multiplier) as u64;
            let ask_qty_f64 = ask
                .qty
                .parse::<f64>()
                .context(format!("Failed to parse an ask qty: {:?}", ask.qty))?;
            let ask_qty_u64 = (ask_qty_f64 * multiplier) as u64;

            if ask_qty_u64 == 0 {
                entry.book.delete(Side::SELL, ask_price_u64);
            } else {
                entry.book.add(Side::SELL, ask_price_u64, ask_qty_u64);
            }
        }

        for bid in book.bids {
            let bid_price_f64 = bid
                .price
                .parse::<f64>()
                .context(format!("Failed to parse an bid price: {:?}", bid.price))?;
            let bid_price_u64 = (bid_price_f64 * multiplier) as u64;
            let bid_qty_f64 = bid
                .qty
                .parse::<f64>()
                .context(format!("Failed to parse an bid qty: {:?}", bid.qty))?;
            let bid_qty_u64 = (bid_qty_f64 * multiplier) as u64;

            if bid_qty_u64 == 0 {
                entry.book.delete(Side::BUY, bid_price_u64);
            } else {
                entry.book.add(Side::BUY, bid_price_u64, bid_qty_u64);
            }
        }

        Ok(())
    }

    /// Takes an update event, and either `reset` or `refresh` of the [`OrderbookMeta`] corresponding to the symbol key.   
    async fn update(&mut self, update: DepthOrderBookEvent) -> Result<(), Error> {
        let symbol = update.symbol.clone();
        let entry = self.get_order_book_meta(&symbol).unwrap();

        if update.final_update_id <= entry.data.last_update_id {
            return Ok(());
        }

        if entry.data.is_first_update {
            if validate_first_update(&update, entry.data.last_update_id) {
                self.reset(&symbol, None).await?;
            } else {
                let asks = match update.asks {
                    Some(a) => a,
                    None => vec![],
                };
                let bids = match update.bids {
                    Some(b) => b,
                    None => vec![],
                };
                let book = OrderBook {
                    bids,
                    asks,
                    last_update_id: update.final_update_id,
                };
                self.refresh(symbol, book, false)?;
            }
        } else {
            if validate_next_update(&update, entry.data.last_update_id) {
                // * request new snapshot
                self.reset(&symbol, None).await?;
            } else {
                let asks = match update.asks {
                    Some(a) => a,
                    None => vec![],
                };
                let bids = match update.bids {
                    Some(b) => b,
                    None => vec![],
                };
                let book = OrderBook {
                    bids,
                    asks,
                    last_update_id: update.final_update_id,
                };
                self.refresh(symbol, book, false)?;
            }
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
            let config = Config::binance();
            match BinanceClient::new("api_key", "secret_key", config.spot_rest_api_endpoint)
                .get(API::Spot(Spot::Depth), Some(format!("symbol={}", symbol)))
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
        loop {
            match self.rx.recv().await {
                Some(event) => match event {
                    Event::DepthOrderBook(depth) => {
                        let symbol = depth.symbol.clone();

                        if let Some(book) = self.get(&symbol) {
                            if book.is_empty() {
                                log::trace!("Resetting binance orderbook book: {}", &symbol);
                                self.reset(&symbol, None).await?;
                            } else {
                                self.update(depth).await?;
                            }
                        } else {
                            log::trace!("Inserting new binance orderbook {}", &symbol);
                            self.insert(&depth.symbol);
                            continue;
                        }
                        let mid_price = self.get_mid_price(&symbol);
                        log::info!("Mid Price for {}: {:?}", symbol, mid_price);
                    }
                    Event::PublicTrade(_trade) => {}
                },
                None => {
                    continue;
                }
            }
        }
    }
}
