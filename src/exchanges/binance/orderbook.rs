use super::{
    api::{Client as BinanceClient, Futures, Spot, API},
    types::{Event, FuturesDepthOrderBookEvent, OrderBook, SpotDepthOrderBookEvent},
};
use crate::common::{get_symbol_pair, scale, Exchange, Side, SymbolPair, DATA_FEED};
use crate::exchanges::Config;
use crate::flatbuffer::event_factory::{make_snapshot_event, make_update_event};
use crate::orderbook::l2::{Level, OrderbookL2};
use failure::{Error, ResultExt};
use std::{env, collections::HashMap, thread};
use zenoh::{
    config::Config as ZenohConfig,
    key_expr::keyexpr,
    prelude::{sync::SyncResolve, Encoding},
};

// ==========================================================================================
// SPOT
// https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly
fn validate_next_update_spot(first_update_id: &u64, last_update_id: &u64) -> bool {
    let expected_next_id = *last_update_id + 1;
    if *first_update_id == expected_next_id {
        // do not skip the update
        false
    } else {
        // request snapshot
        true
    }
}

fn validate_first_update_spot(
    first_update_id: &u64,
    final_update_id: &u64,
    last_update_id: &u64,
) -> bool {
    let expected_next_id = *last_update_id + 1;
    if *first_update_id <= expected_next_id && *final_update_id >= expected_next_id {
        // not skip the update
        false
    } else {
        // request snapshot
        true
    }
}
// ==========================================================================================

// ==========================================================================================
// Futures
// https://binance-docs.github.io/apidocs/futures/en/#how-to-manage-a-local-order-book-correctly
fn validate_next_update_futures(
    final_update_id_in_last_stream: &u64,
    last_update_id: &u64,
) -> bool {
    if *final_update_id_in_last_stream == *last_update_id {
        // do not skip the update
        false
    } else {
        // request snapshot
        true
    }
}

fn validate_first_update_futures(
    first_update_id: &u64,
    final_update_id: &u64,
    last_update_id: &u64,
) -> bool {
    if *first_update_id <= *last_update_id && *final_update_id >= *last_update_id {
        // not skip the update
        false
    } else {
        // request snapshot
        true
    }
}
// ==========================================================================================

/// Binance orderbook metadata
#[derive(Debug)]
struct OrderbookMeta {
    /// stores the actual book
    book: OrderbookL2,
    /// stores the meta data status of the book for local maintenance  
    data: BinanceOrderbookMetaData,
}

#[derive(Debug)]
struct BinanceOrderbookMetaData {
    last_update_id: u64,
    is_first_update: bool,
}

/// Manages the state of all orderbooks that have been subscribed to
pub struct BinanceFeedManager {
    /// maps each symbol to its orderbook
    books: HashMap<String, OrderbookMeta>,
    /// receives updates from tx channel in [`super::spot::SpotWSClient`]
    rx: tokio::sync::mpsc::UnboundedReceiver<Event>,
    /// sends packets to aggregator
    zenoh_session: zenoh::Session,
}

impl BinanceFeedManager {
    pub fn new(rx: tokio::sync::mpsc::UnboundedReceiver<Event>) -> Result<Self, zenoh::Error> {
        let file_location = env::var("ZENOH_CONFIG_DATAFEED").expect("ZENOH_CONFIG_DATAFEED not set");
        let conf = ZenohConfig::from_file(file_location).unwrap();
        let zenoh_session = zenoh::open(conf).res()?;
        Ok(BinanceFeedManager {
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
                data: BinanceOrderbookMetaData {
                    last_update_id: 0,
                    is_first_update: true,
                },
            },
        )
    }

    /// Updates the [`OrderbookMeta`] of the corresponding symbol key  
    fn refresh<'a>(
        &mut self,
        symbol: impl Into<&'a str>,
        book: OrderBook,
        is_first_update: bool,
        is_futures: bool,
    ) -> Result<(), Error> {
        let symbol = symbol.into();

        // safe unwrap cuz method caller checks that book exists
        let entry = self.get_order_book_meta(symbol).unwrap();
        entry.data.is_first_update = is_first_update;
        entry.data.last_update_id = book.last_update_id;

        let mut bids_flat: Vec<Level> = Vec::new();
        let mut asks_flat: Vec<Level> = Vec::new();

        for ask in book.asks {
            let price = ask.price;
            let qty = ask.qty;
            let ask_price_u64 =
                scale(&*price).context(format!("failed to parse an ask price: {:?}", price))?;
            let ask_qty_u64 =
                scale(&*qty).context(format!("failed to parse an ask qty: {:?}", qty))?;

            if ask_qty_u64 == 0 {
                entry.book.delete(Side::SELL, ask_price_u64);
            } else {
                entry.book.add(Side::SELL, ask_price_u64, ask_qty_u64);
            }

            asks_flat.push(Level::new(ask_price_u64, ask_qty_u64));
        }

        for bid in book.bids {
            let price = bid.price;
            let qty = bid.qty;
            let bid_price_u64 =
                scale(&*price).context(format!("failed to parse a bid price: {:?}", price))?;
            let bid_qty_u64 =
                scale(&*qty).context(format!("failed to parse a bid qty: {:?}", qty))?;

            if bid_qty_u64 == 0 {
                entry.book.delete(Side::BUY, bid_price_u64);
            } else {
                entry.book.add(Side::BUY, bid_price_u64, bid_qty_u64);
            }

            bids_flat.push(Level::new(bid_price_u64, bid_qty_u64));
        }

        let zenoh_datafeed = keyexpr::new(DATA_FEED).map_err(|e| {
            failure::err_msg(format!("failed to get zenoh key expression\n{:?}", e))
        })?;
        let symbol_pair = if is_futures {
            SymbolPair::BinanceFutures(symbol)
        } else {
            SymbolPair::BinanceSpot(symbol)
        };

        let pair = get_symbol_pair(symbol_pair)
            .ok_or_else(|| failure::err_msg(format!("no supported pair for {:?}", symbol)))?;
        let event;
        let encoding;

        if is_first_update {
            event = make_snapshot_event(bids_flat, asks_flat, pair, Exchange::Binance)?;
            encoding = Encoding::APP_CUSTOM
                .with_suffix("snapshot_event")
                .map_err(|e| {
                    failure::err_msg(format!("failed to encode snapshot event\n{:?}", e))
                })?;
        } else {
            event = make_update_event(bids_flat, asks_flat, pair, Exchange::Binance)?;
            encoding = Encoding::APP_CUSTOM
                .with_suffix("update_event")
                .map_err(|e| failure::err_msg(format!("failed to encode update event\n{:?}", e)))?;
        }

        self.zenoh_session
            .put(zenoh_datafeed, event.buff)
            .encoding(encoding)
            .res()
            .map_err(|e| {
                failure::err_msg(format!(
                    "failed to send encoding for binance event\n{:?}",
                    e
                ))
            })
    }

    /// Takes a spot update event, and either `reset` or `refresh` of the [`OrderbookMeta`] corresponding to the symbol key.
    async fn update_spot(&mut self, update: SpotDepthOrderBookEvent) -> Result<(), Error> {
        let symbol = update.symbol.as_ref();
        // safe unwrap cuz method caller checked for symbol existence
        let entry = self.get_order_book_meta(symbol).unwrap();

        if update.final_update_id <= entry.data.last_update_id {
            return Ok(());
        }

        if entry.data.is_first_update {
            if validate_first_update_spot(
                &update.first_update_id,
                &update.final_update_id,
                &entry.data.last_update_id,
            ) {
                self.reset(symbol, None, false).await?;
            } else {
                let book = OrderBook {
                    bids: update.bids.unwrap_or_default(),
                    asks: update.asks.unwrap_or_default(),
                    last_update_id: update.final_update_id,
                };
                self.refresh(symbol, book, false, false)?;
            }
        } else if validate_next_update_spot(&update.first_update_id, &entry.data.last_update_id) {
            self.reset(symbol, None, false).await?;
        } else {
            let book = OrderBook {
                bids: update.bids.unwrap_or_default(),
                asks: update.asks.unwrap_or_default(),
                last_update_id: update.final_update_id,
            };
            self.refresh(symbol, book, false, false)?;
        }

        Ok(())
    }

    /// Takes a futures update event, and either `reset` or `refresh` of the [`OrderbookMeta`] corresponding to the symbol key.
    async fn update_futures(&mut self, update: FuturesDepthOrderBookEvent) -> Result<(), Error> {
        let symbol_orig = update.symbol.as_ref();
        let symbol = format!("{}FUTURES", update.symbol);
        // safe unwrap cuz method caller checked for symbol existence
        let entry = self.get_order_book_meta(symbol.as_str()).unwrap();

        if update.final_update_id < entry.data.last_update_id {
            return Ok(());
        }

        if entry.data.is_first_update {
            if validate_first_update_futures(
                &update.first_update_id,
                &update.final_update_id,
                &entry.data.last_update_id,
            ) {
                self.reset(symbol_orig, None, true).await?;
            } else {
                let book = OrderBook {
                    bids: update.bids.unwrap_or_default(),
                    asks: update.asks.unwrap_or_default(),
                    last_update_id: update.final_update_id,
                };
                self.refresh(symbol.as_str(), book, false, true)?;
            }
        } else if validate_next_update_futures(
            &update.final_update_id_in_last_stream,
            &entry.data.last_update_id,
        ) {
            self.reset(symbol_orig, None, true).await?;
        } else {
            let book = OrderBook {
                bids: update.bids.unwrap_or_default(),
                asks: update.asks.unwrap_or_default(),
                last_update_id: update.final_update_id,
            };
            self.refresh(symbol.as_str(), book, false, true)?;
        }

        Ok(())
    }

    /// Request a snapshot of the orderbook using Binance REST API  
    async fn reset(
        &mut self,
        symbol: &str,
        timeout: Option<std::time::Duration>,
        is_futures: bool,
    ) -> Result<(), Error> {
        let start = std::time::Instant::now();
        loop {
            let config = Config::binance();
            let params = format!("symbol={}", symbol);
            let res = if is_futures {
                BinanceClient::new(config.futures_rest_api_endpoint)
                    .get(API::Futures(Futures::Depth), Some(params))
                    .await
            } else {
                BinanceClient::new(config.spot_rest_api_endpoint)
                    .get(API::Spot(Spot::Depth), Some(params))
                    .await
            };

            match res {
                Ok(book) => {
                    if is_futures {
                        self.refresh(
                            format!("{}FUTURES", symbol).as_str(),
                            book,
                            true,
                            is_futures,
                        )?;
                    } else {
                        self.refresh(symbol, book, true, is_futures)?;
                    }
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
                    } else if start.elapsed() > std::time::Duration::from_secs(30) {
                        log::warn!("Still can't get binance orderbook snapshot");
                    }
                }
            }
        }
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        loop {
            match self.rx.recv().await {
                Some(event) => match event {
                    Event::SpotDepthOrderBook(depth) => {
                        let symbol = depth.symbol.as_ref();

                        if let Some(book) = self.get(symbol) {
                            if book.is_empty() {
                                log::trace!("Resetting binance orderbook book: {}", symbol);
                                self.reset(symbol, None, false).await?;
                            } else {
                                self.update_spot(depth).await?;
                            }
                        } else {
                            log::trace!("Inserting new binance orderbook {}", symbol);
                            self.insert(symbol);
                            self.reset(symbol, None, false).await?;
                            continue;
                        }
                    }
                    Event::FuturesDepthOrderBook(depth) => {
                        let symbol = format!("{}FUTURES", depth.symbol);

                        if let Some(book) = self.get(&*symbol) {
                            if book.is_empty() {
                                log::trace!("Resetting binance orderbook book: {}", symbol);
                                self.reset(&depth.symbol, None, true).await?;
                            } else {
                                self.update_futures(depth).await?;
                            }
                        } else {
                            log::trace!("Inserting new binance orderbook {}", symbol);
                            self.insert(&*symbol);
                            self.reset(&depth.symbol, None, true).await?;
                            continue;
                        }
                    }
                    _ => {}
                },
                None => {
                    continue;
                }
            }
        }
    }
}
