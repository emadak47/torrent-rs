use crate::common::{FlatbufferEvent, ZenohEvent};
use crate::flatbuffer::{
    event_factory::make_snapshot_aggregator,
    orderbook::atrimo::update_events::{
        root_as_update_event_message, UpdateAskData, UpdateBidData,
    },
    snapshot::atrimo::snapshot_events::{
        root_as_snapshot_event_message, SnapshotAskData, SnapshotBidData,
    },
};
use crate::orderbook::l2::Level;
use crate::spsc::Producer;
use flatbuffers::Vector;
use std::{
    collections::{BTreeMap, HashMap},
    ops::Div,
};

type Exchange = String;
type ExchangeQty = u64;
type Price = u64;
type Symbol = String;
type BookSide = BTreeMap<Price, Metadata>;

struct Metadata {
    total_qty: u64,
    ex_qty_mp: HashMap<Exchange, ExchangeQty>,
}

impl Metadata {
    fn new() -> Self {
        Metadata {
            total_qty: 0,
            ex_qty_mp: HashMap::new(),
        }
    }
}

#[derive(Default)]
pub struct Book {
    bid_book: BookSide,
    ask_book: BookSide,
}

impl Book {
    pub fn new() -> Self {
        Book {
            bid_book: BookSide::new(),
            ask_book: BookSide::new(),
        }
    }
}

/// Holds and maintains an aggregated orderbook for a number of symbols.
/// Sends [`PricingEvent`] to an external service as a [`FlatbufferEvent`]
pub struct Aggregator {
    /// holds all symbols and their corresponding aggregated orderbook
    books: HashMap<Symbol, Book>,
    /// produces events to an spsc data-structure to be consumed by an external service
    q: Producer<FlatbufferEvent>,
    /// flag to denote if an incoming update has not been processed correctly
    in_sync: bool,
}

impl Aggregator {
    pub fn new(q: Producer<FlatbufferEvent>) -> Self {
        Self {
            books: HashMap::new(),
            q,
            in_sync: true,
        }
    }

    /// clears the orderbook for the supplied `exchange` and `instrument`
    /// and appends new `bids` and `asks`
    fn reset(
        &mut self,
        instrument: &str,
        exchange: &str,
        bids: Vector<SnapshotBidData>,
        asks: Vector<SnapshotAskData>,
    ) -> Result<(), failure::Error> {
        log::info!("Snapshot event: Exchange `{exchange}` | Symbol `{instrument}`");

        let mut bids_to_remove = Vec::new();
        let mut asks_to_remove = Vec::new();

        if let Some(book) = self.books.get_mut(instrument) {
            for (price, metadata) in book.bid_book.iter_mut() {
                if let Some(qty) = metadata.ex_qty_mp.get_mut(exchange) {
                    if *qty != 0 {
                        metadata.total_qty -= *qty;
                        *qty = 0;
                        if metadata.total_qty == 0 {
                            bids_to_remove.push(*price);
                        }
                    }
                }
            }

            for (price, metadata) in book.ask_book.iter_mut() {
                if let Some(qty) = metadata.ex_qty_mp.get_mut(exchange) {
                    if *qty != 0 {
                        metadata.total_qty -= *qty;
                        *qty = 0;
                        if metadata.total_qty == 0 {
                            asks_to_remove.push(*price);
                        }
                    }
                }
            }
        } else {
            self.books.insert(instrument.to_string(), Book::new());
        }

        // remove price levels with total qty 0
        if !asks_to_remove.is_empty() || !bids_to_remove.is_empty() {
            let book = self
                .books
                .get_mut(instrument)
                .expect("len must be 0 if book didn't exist");
            for a in asks_to_remove {
                book.ask_book.remove(&a);
            }
            for b in bids_to_remove {
                book.bid_book.remove(&b);
            }
        }

        let book = self
            .books
            .get_mut(instrument)
            .expect("book must haved existed or just been inserted");

        // insert bids
        for bid in bids {
            let price = bid.price();
            let qty = bid.qty();
            if let Some(metadata) = book.bid_book.get_mut(&price) {
                if let Some(exchange_qty) = metadata.ex_qty_mp.get_mut(exchange) {
                    metadata.total_qty -= *exchange_qty;
                }
                metadata.total_qty += qty;
                metadata.ex_qty_mp.insert(exchange.to_string(), qty);
            } else {
                let mut metadata = Metadata::new();
                metadata.total_qty = qty;
                metadata.ex_qty_mp.insert(exchange.to_string(), qty);
                book.bid_book.insert(price, metadata);
            }
        }

        // insert asks
        for ask in asks {
            let price = ask.price();
            let qty = ask.qty();
            if let Some(metadata) = book.ask_book.get_mut(&price) {
                if let Some(exchange_qty) = metadata.ex_qty_mp.get_mut(exchange) {
                    metadata.total_qty -= *exchange_qty;
                }
                metadata.total_qty += qty;
                metadata.ex_qty_mp.insert(exchange.to_string(), qty);
            } else {
                let mut metadata = Metadata::new();
                metadata.total_qty = qty;
                metadata.ex_qty_mp.insert(exchange.to_string(), qty);
                book.ask_book.insert(price, metadata);
            }
        }

        Ok(())
    }

    /// update the orderbook for the supplied `exchange` and `instrument`
    /// with data in `bids` and `asks`
    fn update(
        &mut self,
        instrument: &str,
        exchange: &str,
        bids: Vector<UpdateBidData>,
        asks: Vector<UpdateAskData>,
    ) -> Result<(), failure::Error> {
        log::info!("Update event: Exchange `{exchange}` | Symbol `{instrument}`");

        let book = self
            .books
            .get_mut(instrument)
            .ok_or_else(|| failure::err_msg("book must haved existed or just been insertd"))?;

        for bid in bids {
            let price = bid.price();
            let qty = bid.qty();
            if let Some(metadata) = book.bid_book.get_mut(&price) {
                if let Some(exchange_qty) = metadata.ex_qty_mp.get(exchange) {
                    metadata.total_qty -= *exchange_qty;
                }
                if qty == 0 {
                    metadata.ex_qty_mp.remove(exchange);
                } else {
                    metadata.total_qty += qty;
                    metadata.ex_qty_mp.insert(exchange.to_string(), qty);
                }
            } else {
                let mut metadata = Metadata::new();
                metadata.total_qty = qty;
                metadata.ex_qty_mp.insert(exchange.to_string(), qty);
                book.bid_book.insert(price, metadata);
            }
        }

        for ask in asks {
            let price = ask.price();
            let qty = ask.qty();
            if let Some(metadata) = book.ask_book.get_mut(&price) {
                if let Some(exchange_qty) = metadata.ex_qty_mp.get(exchange) {
                    metadata.total_qty -= *exchange_qty;
                }
                if qty == 0 {
                    metadata.ex_qty_mp.remove(exchange);
                } else {
                    metadata.total_qty += qty;
                    metadata.ex_qty_mp.insert(exchange.to_string(), qty);
                }
            } else {
                let mut metadata = Metadata::new();
                metadata.total_qty = qty;
                metadata.ex_qty_mp.insert(exchange.to_string(), qty);
                book.ask_book.insert(price, metadata);
            }
        }

        Ok(())
    }

    /// decodes the data's buffer and either resets or updates the corresponding orderbook
    /// based on the data's stream id
    /// # Note
    ///   - stream id 0 corresponds to a snapshot event (i.e. triggers reset)
    ///   - stream id 1 corresponds to an update event (i.e. triggers update)
    ///   - stream id 2 corresponds to a pricing event
    pub fn process(&mut self, data: ZenohEvent) -> Result<(), failure::Error> {
        match data.stream_id {
            0 => {
                // snapshot
                // if the instrument and exchange are parsed successfully, and bids and asks aren't,
                // then we can just clear the bid and ask books without inserting a snapshot (i.e. insert empty bids & asks)
                // This just means that our aggregated orderbook will not have any of that particular exchange's levels,
                // which prevents it having an incorrect state at any point in time.
                let event = root_as_snapshot_event_message(&data.buff)?
                    .snapshot_event()
                    .ok_or_else(|| {
                        self.in_sync = false;
                        failure::err_msg("failed to parse snapshot event")
                    })?;
                let instrument = event.instrument().ok_or_else(|| {
                    self.in_sync = false;
                    failure::err_msg("failed to parse instrument")
                })?;
                let exchange = event.exchange().ok_or_else(|| {
                    self.in_sync = false;
                    failure::err_msg("failed to parse exchange")
                })?;
                let data = event
                    .snapshot()
                    .ok_or_else(|| failure::err_msg("failed to parse snapshot data"));
                let (bids, asks) = data.map_or_else(
                    |e| {
                        log::error!("{e}");
                        (Vector::default(), Vector::default())
                    },
                    |data| {
                        let b = data.bids();
                        let a = data.asks();
                        (b.unwrap_or_default(), a.unwrap_or_default())
                    },
                );

                self.reset(instrument, exchange, bids, asks)?;
            }
            1 => {
                // update
                // if instrument, exchange, bids, or asks are parsed unsucessfully, then the whole update
                // will be dissmised. Thus, the aggregator will have an out of sync state
                let event = root_as_update_event_message(&data.buff)?
                    .update_event()
                    .ok_or_else(|| {
                        self.in_sync = false;
                        failure::err_msg("failed to parse update event")
                    })?;
                let instrument = event.instrument().ok_or_else(|| {
                    self.in_sync = false;
                    failure::err_msg("failed to parse instrument")
                })?;
                let exchange = event.exchange().ok_or_else(|| {
                    self.in_sync = false;
                    failure::err_msg("failed to parse instrument")
                })?;
                let data = event
                    .update()
                    .ok_or_else(|| failure::err_msg("failed to parse update data"))?;

                let bids = data
                    .bids()
                    .ok_or_else(|| failure::err_msg("failed to parse bids for update"))?;
                let asks = data
                    .asks()
                    .ok_or_else(|| failure::err_msg("failed to parse asks for update"))?;

                self.update(instrument, exchange, bids, asks)?;
            }
            2 => {
                // Pricing Details
                self.make_snapshot_event("BTC-USDT-spot");
            }
            _ => return Err(failure::err_msg("unexpected stream id event")),
        }

        Ok(())
    }

    /*
     ************************************************************
     *********            Utility Methods              **********
     ************************************************************
     */
    pub fn list_books(&self) -> Vec<&String> {
        self.books.keys().collect::<Vec<&String>>()
    }

    /// Returns all bid price levels for the given `instrument` if it exists.
    pub fn list_bid_levels<'a>(&self, instrument: impl Into<&'a str>) -> Option<Vec<&u64>> {
        let book = self.books.get(instrument.into())?;
        let bid_levels = book.bid_book.keys().collect::<Vec<&u64>>();
        Some(bid_levels)
    }

    /// Returns all ask price levels for the given `instrument` if it exists.
    pub fn list_ask_levels<'a>(&self, instrument: impl Into<&'a str>) -> Option<Vec<&u64>> {
        let book = self.books.get(instrument.into())?;
        let ask_levels = book.ask_book.keys().collect::<Vec<&u64>>();
        Some(ask_levels)
    }

    /// Returns best bid price for the given `instrument` if it exists.
    pub fn get_best_bid<'a>(&self, instrument: impl Into<&'a str>) -> Option<u64> {
        let book = self.books.get(instrument.into())?;
        let best_bid = book.bid_book.last_key_value()?;
        Some(*best_bid.0)
    }

    /// Returns best ask price for the given `instrument` if it exists.
    pub fn get_best_ask<'a>(&self, instrument: impl Into<&'a str>) -> Option<u64> {
        let book = self.books.get(instrument.into())?;
        let best_ask = book.ask_book.first_key_value()?;
        Some(*best_ask.0)
    }

    /// Returns mid price for the given `instrument` if it exists.
    pub fn get_mid_price<'a>(&self, instrument: impl Into<&'a str>) -> Option<u64> {
        let instrument = instrument.into();
        let best_bid = self.get_best_bid(instrument)?;
        let best_ask = self.get_best_ask(instrument)?;
        let spread = best_bid.checked_add(best_ask)?;
        Some(spread.div(2))
    }

    /// Returns the average execution bid price for the given `qty`
    /// # Note
    /// this methods incurs a small performance cost. This is because the prices and
    /// sizes in the bid orderbook have to be scaled down to avoid overflowing.
    pub fn get_execution_bid<'a>(
        &self,
        instrument: impl Into<&'a str>,
        qty: impl Into<f64>,
    ) -> Option<u64> {
        let qty_f64 = Into::<f64>::into(qty);
        let instrument = instrument.into();
        let book = self.books.get(instrument)?;

        let multiplier = 1e10;

        let mut cum_qty = 0.0;
        let mut avg_price = 0.0;
        for (price, meta) in book.bid_book.iter().rev() {
            let level_qty = (meta.total_qty as f64).div(multiplier);
            let price = (*price as f64).div(multiplier);
            if cum_qty + level_qty < qty_f64 {
                cum_qty += level_qty;
                avg_price += price * level_qty;
            } else {
                let rem_qty = qty_f64 - cum_qty;
                cum_qty += rem_qty;
                avg_price += price * rem_qty;
                break;
            }
        }

        if cum_qty < qty_f64 {
            return None;
        }

        Some((avg_price.div(cum_qty) * multiplier) as u64)
    }

    /// Returns the average execution ask price for the given `qty`
    /// # Note
    /// this methods incurs a small performance cost. This is because the prices and
    /// sizes in the bid orderbook have to be scaled down to avoid overflowing.
    pub fn get_execution_ask<'a>(
        &self,
        instrument: impl Into<&'a str>,
        qty: impl Into<f64>,
    ) -> Option<u64> {
        let qty_f64 = Into::<f64>::into(qty);
        let instrument = instrument.into();
        let book = self.books.get(instrument)?;

        let multiplier = 1e10;

        let mut cum_qty = 0.0;
        let mut avg_price = 0.0;
        for (price, meta) in book.ask_book.iter() {
            let level_qty = (meta.total_qty as f64).div(multiplier);
            let price = (*price as f64).div(multiplier);

            if cum_qty + level_qty < qty_f64 {
                cum_qty += level_qty;
                avg_price += price * level_qty;
            } else {
                let rem_qty = qty_f64 - cum_qty;
                cum_qty += rem_qty;
                avg_price += price * rem_qty;
                break;
            }
        }

        if cum_qty < qty_f64 {
            return None;
        }

        Some((avg_price.div(qty_f64) * multiplier) as u64)
    }

    /// Returns the total bid liquidity in the orderbook of the given `instrument`
    /// up to the number of `bps` away from mid price
    /// # Arguments
    ///
    /// * `instrument` - the symbol for which liquidity will be retrieved
    /// * `bps` - a whole number of basis points below mid price (i.e. mid_price * (1 - 0.0001 * bps)
    pub fn get_bid_qty_till<'a>(
        &self,
        instrument: impl Into<&'a str>,
        bps: impl Into<f64>,
    ) -> Option<u64> {
        let instrument = instrument.into();
        let bps: f64 = bps.into();
        let book = self.books.get(instrument)?;
        let mid_price = self.get_mid_price(instrument)?;
        let factor = (1.0_f64) - (bps / 10000_f64);
        let stoppage_price = (mid_price as f64 * factor) as u64;
        let cum_qty = book
            .bid_book
            .iter()
            .rev()
            .take_while(|&bid| bid.0 > &stoppage_price)
            .map(|bid| bid.1.total_qty)
            .sum();

        Some(cum_qty)
    }

    /// Returns the total ask liquidity in the orderbook of the given `instrument`
    /// up to the number of `bps` away from mid price
    /// # Arguments
    ///
    /// * `instrument` - the symbol for which liquidity will be retrieved
    /// * `bps` - a whole number of basis points above mid price (i.e. mid_price * (1 + 0.0001 * bps)
    pub fn get_ask_qty_till<'a>(
        &self,
        instrument: impl Into<&'a str>,
        bps: impl Into<f64>,
    ) -> Option<u64> {
        let instrument = instrument.into();
        let bps: f64 = bps.into();
        let book = self.books.get(instrument)?;
        let mid_price = self.get_mid_price(instrument)?;
        let factor = (1.0_f64) + (bps / 10000_f64);
        let stoppage_price = (mid_price as f64 * factor) as u64;
        let cum_qty = book
            .ask_book
            .iter()
            .take_while(|&ask| ask.0 < &stoppage_price)
            .map(|ask| ask.1.total_qty)
            .sum();

        Some(cum_qty)
    }

    /*
     ************************************************************
     *********        Methods For Extneral Use         **********
     ************************************************************
     */

    fn make_snapshot_event(&mut self, instrument: &str) {
        let mut bids_flat: Vec<Level> = Vec::new();
        let mut asks_flat: Vec<Level> = Vec::new();

        if let Some(book) = self.books.get_mut(instrument) {
            for (price, metadata) in book.bid_book.iter().rev() {
                bids_flat.push(Level::new(*price, metadata.total_qty));
            }
            for (price, metadata) in book.ask_book.iter() {
                asks_flat.push(Level::new(*price, metadata.total_qty));
            }
            let event = make_snapshot_aggregator(bids_flat, asks_flat, instrument);
            if event.is_ok() {
                if self.in_sync {
                    self.q.push(event.unwrap());
                } else {
                    log::error!("aggregator out of sync");
                }
            } else {
                log::error!("error making aggregator snapshot\n{:?}", event);
            }
        }
    }
}
