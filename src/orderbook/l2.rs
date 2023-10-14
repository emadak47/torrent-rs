use crate::common::Side;
use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};

/// Defines an ordebook's L2 level
#[derive(Debug, PartialEq, Eq)]
pub struct Level {
    pub price: u64,
    pub qty: u64,
}

impl Level {
    pub fn new(price: u64, qty: u64) -> Self {
        Level { price, qty }
    }
}

impl Deref for Level {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.qty
    }
}
impl DerefMut for Level {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.qty
    }
}

/// Standard L2 orderbook. Book sides are implemented using [`BTreeMap`]
#[derive(Debug)]
pub struct OrderbookL2 {
    pub bids: BTreeMap<u64, Level>,
    pub asks: BTreeMap<u64, Level>,
}

impl OrderbookL2 {
    /// Define a new orderbook template for an exchange's currency pair
    ///
    /// ```rust
    /// # use torrent::orderbook::l2::OrderbookL2;
    /// let mut orderbook = OrderbookL2::new();
    /// assert!(orderbook.is_empty());
    ///
    /// let level = orderbook.add(Side::BUY, 1, 5);
    /// assert_eq!(level.price, 1);
    /// assert_eq!(level.qty, 5);
    ///
    /// orderbook.add(Side::SELL, 10, 50);
    /// assert_eq!(orderbook.asks.len(), 1);
    ///
    /// orderbook.add(Side::BUY, 2, 8);
    /// assert_eq!(orderbook.bids.len(), 2);
    ///
    /// let level = orderbook.add(Side::BUY, 1, 4);
    /// assert_eq!(level.price, 1);
    /// assert_eq!(level.qty, 4);
    ///
    /// let level = orderbook.delete(Side::SELL, 10);
    /// assert_eq!(level, Some(Level::new(10, 50)));
    ///
    /// let level = orderbook.delete(Side::SELL, 10);
    /// assert_eq!(level, None);
    ///
    /// orderbook.clear();
    /// assert!(orderbook.is_empty());
    /// ```
    pub fn new() -> Self {
        OrderbookL2 {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    /// Check if the book empty (i.e. both sides are empty)
    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }

    /// Modify an exisiting [`Level`] in one `side` of the orderbook if its price
    /// corresponds to the given price. Otherwise, add a new [`Level`]
    /// Returns a mutable reference to that value
    pub fn add(&mut self, side: Side, price: u64, qty: u64) -> &mut Level {
        // qty can never be < 0 due to type limits
        match side {
            Side::BUY => self
                .bids
                .entry(price)
                .and_modify(|lvl| **lvl = qty)
                .or_insert(Level::new(price, qty)),
            Side::SELL => self
                .asks
                .entry(price)
                .and_modify(|lvl| **lvl = qty)
                .or_insert(Level::new(price, qty)),
        }
    }

    /// Delete a level from the orderbook, if exsits
    pub fn delete(&mut self, side: Side, price: u64) -> Option<Level> {
        match side {
            Side::BUY => self.bids.remove(&price),
            Side::SELL => self.asks.remove(&price),
        }
    }

    /// Clear both sides of the orderbook
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    /// Get mid price
    pub fn get_mid_price(&mut self) -> Option<u64> {
        let best_bid = self.bids.first_entry();
        let best_ask = self.asks.last_entry();

        if best_bid.is_some() && best_ask.is_some() {
            let best_bid = best_bid.unwrap().key().clone();
            let best_ask = best_ask.unwrap().key().clone();

            return best_bid.checked_add(best_ask);
        }

        None
    }
}
