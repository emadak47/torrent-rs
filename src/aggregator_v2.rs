use ::phf::phf_map;
use std::collections::{BTreeMap, HashMap, HashSet};

static EXCH_MP: phf::Map<&'static str, u8> = phf_map! {
    "BINANCE" => 1,
    "OKX" => 2,
};

pub struct FlatbufferUpdate {
    pub bids: Vec<(u64, u64)>, // [(price1, qty1), ...]
    pub asks: Vec<(u64, u64)>,
}

pub struct FlatbufferEvent {
    pub kind: String,
    pub source: String,
    pub updates: FlatbufferUpdate,
}

#[derive(Debug)]
pub struct Meta {
    pub tot_qty: u64,
    pub qty_mp: HashMap<u8, u64>, // exchange identidier => exchange specific qty
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            tot_qty: 0,
            qty_mp: HashMap::new(),
        }
    }
}

impl Meta {
    pub fn new(tot_qty: u64, qty_mp: HashMap<u8, u64>) -> Self {
        Self { tot_qty, qty_mp }
    }
}

#[derive(Debug)]
pub struct AggBook {
    pub bids: BTreeMap<u64, Meta>,
    pub asks: BTreeMap<u64, Meta>,

    pub b_prices: HashMap<u8, HashSet<u64>>, // exchange identifier => set of price levels
    pub a_prices: HashMap<u8, HashSet<u64>>,
}

impl AggBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),

            b_prices: HashMap::new(),
            a_prices: HashMap::new(),
        }
    }

    pub fn update(&mut self, event: FlatbufferEvent) {
        let identifier = EXCH_MP[&event.source];

        /***********************************  Bids  *************************************/
        for bid in &event.updates.bids {
            match self.bids.get_mut(&bid.0) {
                Some(meta) => {
                    // 1. add incoming qty
                    meta.tot_qty = (meta.tot_qty).checked_add(bid.1).expect(&format!(
                        "u64 addition `{}` to `{}` for exchange `{}`",
                        &bid.1,
                        &(meta.tot_qty),
                        &event.source,
                    ));

                    // 2. if there is some old qty of this exchange at this price level, deduct it.
                    if let Some(curr_qty) = meta.qty_mp.get(&identifier) {
                        meta.tot_qty = (meta.tot_qty).checked_sub(*curr_qty).expect(&format!(
                            "u64 substraction `{}` to `{}` for exchange `{}`",
                            &bid.1,
                            &(meta.tot_qty),
                            &event.source,
                        ));

                        // 3. remove this price level if tot_qty is 0
                        // meta.tot_qty can only be 0 at this point iff
                        // - the tot_qty is fully contributed by this exchange
                        // - and the incoming update qty is 0
                        if meta.tot_qty == 0 {
                            self.bids.remove(&bid.0);
                            if let Some(set) = self.b_prices.get_mut(&identifier) {
                                set.remove(&bid.0);
                            }
                            continue;
                        }
                    }

                    // 4. update (or insert new) exchange qty
                    if bid.1 != 0 {
                        // `insert` replaces value if key exists, otherwise inserts new.
                        meta.qty_mp.insert(identifier, bid.1);
                    } else {
                        meta.qty_mp.remove(&identifier);
                    }

                    // 5. update exchange's price list
                    if bid.1 != 0 {
                        if let Some(set) = self.b_prices.get_mut(&identifier) {
                            set.insert(bid.0);
                        } else {
                            self.b_prices.insert(identifier, HashSet::from([bid.0]));
                        }
                    } else if bid.1 == 0 && self.b_prices.get(&identifier).is_some() {
                        self.b_prices
                            .get_mut(&identifier)
                            .expect("is some")
                            .remove(&bid.0);
                    }
                }
                None => {
                    // create a new price level with tot_qty from this incoming update
                    let meta = Meta::new(bid.1, HashMap::from([(identifier, bid.1)]));
                    self.bids.insert(bid.0, meta);

                    // update exchange's price list
                    if let Some(set) = self.b_prices.get_mut(&identifier) {
                        set.insert(bid.0);
                    } else {
                        self.b_prices.insert(identifier, HashSet::from([bid.0]));
                    }
                }
            }
        }

        /***********************************  Asks  *************************************/
        for ask in &event.updates.asks {
            match self.asks.get_mut(&ask.0) {
                Some(meta) => {
                    // 1. add incoming qty
                    meta.tot_qty = (meta.tot_qty).checked_add(ask.1).expect(&format!(
                        "u64 addition `{}` to `{}` for exchange `{}`",
                        &ask.1,
                        &(meta.tot_qty),
                        &event.source,
                    ));

                    // 2. if there is some old qty of this exchange at this price level, deduct it.
                    if let Some(curr_qty) = meta.qty_mp.get(&identifier) {
                        meta.tot_qty = (meta.tot_qty).checked_sub(*curr_qty).expect(&format!(
                            "u64 substraction `{}` to `{}` for exchange `{}`",
                            &ask.1,
                            &(meta.tot_qty),
                            &event.source,
                        ));

                        // 3. remove this price level if tot_qty is 0
                        // meta.tot_qty can only be 0 at this point iff
                        // - the tot_qty is fully contributed by this exchange
                        // - and the incoming update qty is 0
                        if meta.tot_qty == 0 {
                            self.asks.remove(&ask.0);
                            if let Some(set) = self.a_prices.get_mut(&identifier) {
                                set.remove(&ask.0);
                            }
                            continue;
                        }
                    }

                    // 4. update (or insert new) exchange qty
                    if ask.1 != 0 {
                        // `insert` replaces value if key exists, otherwise inserts new.
                        meta.qty_mp.insert(identifier, ask.1);
                    } else {
                        meta.qty_mp.remove(&identifier);
                    }

                    // 5. update exchange's price list
                    if ask.1 != 0 {
                        if let Some(set) = self.a_prices.get_mut(&identifier) {
                            set.insert(ask.0);
                        } else {
                            self.b_prices.insert(identifier, HashSet::from([ask.0]));
                        }
                    } else if ask.1 == 0 && self.a_prices.get(&identifier).is_some() {
                        self.a_prices
                            .get_mut(&identifier)
                            .expect("is some")
                            .remove(&ask.0);
                    }
                }
                None => {
                    // create a new price level with tot_qty from this incoming update
                    let meta = Meta::new(ask.1, HashMap::from([(identifier, ask.1)]));
                    self.asks.insert(ask.0, meta);

                    // update exchange's price list
                    if let Some(set) = self.a_prices.get_mut(&identifier) {
                        set.insert(ask.0);
                    } else {
                        self.a_prices.insert(identifier, HashSet::from([ask.0]));
                    }
                }
            }
        }

        if event.kind == "snapshot" {
            /***********************************  Bids  *************************************/
            // at this point, `price_set.len()` >= `incoming_prices.len()`
            let bid_price_set = self
                .b_prices
                .get(&identifier)
                .expect("must have length > 0 from updates above");

            let incoming_bid_prices = HashSet::<u64>::from_iter(
                event
                    .updates
                    .bids
                    .into_iter()
                    .filter_map(|(p, q)| if q != 0 { Some(p) } else { None })
                    .collect::<Vec<u64>>(),
            );

            let bid_prices_to_remove: HashSet<_> =
                bid_price_set.difference(&incoming_bid_prices).collect();
            for price in bid_prices_to_remove {
                let meta = self
                    .bids
                    .get_mut(price)
                    .expect(&format!("price level `{}` must exist in tree", price));

                let curr_qty = meta.qty_mp.get(&identifier).expect(&format!(
                    "price level `{}` for exchange `{}` must exist",
                    price, &identifier
                ));

                meta.tot_qty = (meta.tot_qty)
                    .checked_sub(*curr_qty)
                    .expect("u64 substraction `{}` to `{}` for exchange `{}`");

                meta.qty_mp.remove(&identifier);

                if meta.tot_qty == 0 {
                    // remove this price level if tot_qty is 0
                    // meta.tot_qty can only be 0 at this point iff
                    // - the tot_qty is fully contributed by this exchange
                    if meta.tot_qty == 0 {
                        self.bids.remove(price);

                        // no need to remove price level from `self.b_prices` here
                        // because it will be reset at the end
                    }
                }
            }

            self.b_prices.insert(identifier, incoming_bid_prices);

            /***********************************  Asks  *************************************/
            // at this point, `price_set.len()` >= `incoming_prices.len()`
            let ask_price_set = self
                .a_prices
                .get(&identifier)
                .expect("must have length > 0 from updates above");

            let incoming_ask_prices = HashSet::<u64>::from_iter(
                event
                    .updates
                    .asks
                    .into_iter()
                    .filter_map(|(p, q)| if q != 0 { Some(p) } else { None })
                    .collect::<Vec<u64>>(),
            );

            let ask_prices_to_remove: HashSet<_> =
                ask_price_set.difference(&incoming_ask_prices).collect();
            for price in ask_prices_to_remove {
                let meta = self
                    .asks
                    .get_mut(price)
                    .expect(&format!("price level `{}` must exist in tree", price));

                let curr_qty = meta.qty_mp.get(&identifier).expect(&format!(
                    "price level `{}` for exchange `{}` must exist",
                    price, &identifier
                ));

                meta.tot_qty = (meta.tot_qty)
                    .checked_sub(*curr_qty)
                    .expect("u64 substraction `{}` to `{}` for exchange `{}`");

                meta.qty_mp.remove(&identifier);

                if meta.tot_qty == 0 {
                    // remove this price level if tot_qty is 0
                    // meta.tot_qty can only be 0 at this point iff
                    // - the tot_qty is fully contributed by this exchange
                    if meta.tot_qty == 0 {
                        self.asks.remove(price);

                        // no need to remove price level from `self.b_prices` here
                        // because it will be reset at the end
                    }
                }
            }

            self.a_prices.insert(identifier, incoming_ask_prices);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_updates() {
        /********************************* Set-up book *********************************/
        let mut agg_book = AggBook {
            bids: BTreeMap::from([
                (10, Meta::new(5, HashMap::from([(1, 3), (2, 2)]))),
                (9, Meta::new(8, HashMap::from([(1, 8)]))),
                (8, Meta::new(10, HashMap::from([(1, 7), (2, 3)]))),
                (7, Meta::new(15, HashMap::from([(2, 15)]))),
            ]),

            asks: BTreeMap::new(),

            b_prices: HashMap::from([
                (1, HashSet::from([10, 9, 8])),
                (2, HashSet::from([10, 8, 7])),
            ]),
            a_prices: HashMap::new(),
        };

        /********************************* Event 1 *********************************/
        let update_event = FlatbufferEvent {
            kind: "update".to_string(),
            source: "BINANCE".to_string(),
            updates: FlatbufferUpdate {
                bids: vec![(10, 0), (11, 3)],
                asks: vec![],
            },
        };
        agg_book.update(update_event);

        let level = agg_book.bids.get(&11).unwrap();
        assert_eq!(level.tot_qty, 3);
        assert_eq!(level.qty_mp.get(&1).unwrap(), &3);

        let level = agg_book.bids.get(&10).unwrap();
        assert_eq!(level.tot_qty, 2);
        assert!(level.qty_mp.get(&1).is_none());
        assert_eq!(level.qty_mp.get(&2).unwrap(), &2);

        let mut v = agg_book
            .b_prices
            .iter()
            .find_map(|(k, v)| if k == &1 { Some(v) } else { None })
            .unwrap()
            .iter()
            .collect::<Vec<&u64>>();
        v.sort();
        assert_eq!(v, Vec::from([&8, &9, &11]));

        /********************************* Event 2 *********************************/
        let update_event = FlatbufferEvent {
            kind: "update".to_string(),
            source: "BINANCE".to_string(),
            updates: FlatbufferUpdate {
                bids: vec![(7, 2), (9, 0), (8, 13)],
                asks: vec![],
            },
        };
        agg_book.update(update_event);

        let level = agg_book.bids.get(&7).unwrap();
        assert_eq!(level.tot_qty, 17);
        assert_eq!(level.qty_mp.get(&1).unwrap(), &2);

        let level = agg_book.bids.get(&9);
        assert!(level.is_none());

        let level = agg_book.bids.get(&8).unwrap();
        assert_eq!(level.tot_qty, 16);
        assert_eq!(level.qty_mp.get(&1).unwrap(), &13);

        let mut v = agg_book
            .b_prices
            .iter()
            .find_map(|(k, v)| if k == &1 { Some(v) } else { None })
            .unwrap()
            .iter()
            .collect::<Vec<&u64>>();
        v.sort();
        assert_eq!(v, Vec::from([&7, &8, &11]));

        let mut v = agg_book
            .b_prices
            .iter()
            .find_map(|(k, v)| if k == &2 { Some(v) } else { None })
            .unwrap()
            .iter()
            .collect::<Vec<&u64>>();
        v.sort();
        assert_eq!(v, Vec::from([&7, &8, &10]));
    }

    #[test]
    fn test_snapshot() {
        /********************************* Set-up book *********************************/
        let mut agg_book = AggBook {
            bids: BTreeMap::from([
                (10, Meta::new(5, HashMap::from([(1, 3), (2, 2)]))),
                (9, Meta::new(8, HashMap::from([(1, 8)]))),
                (8, Meta::new(10, HashMap::from([(1, 7), (2, 3)]))),
                (7, Meta::new(15, HashMap::from([(2, 15)]))),
            ]),

            asks: BTreeMap::new(),

            b_prices: HashMap::from([
                (1, HashSet::from([10, 9, 8])),
                (2, HashSet::from([10, 8, 7])),
            ]),
            a_prices: HashMap::new(),
        };

        /********************************* Event 1 *********************************/
        let snapshot_event = FlatbufferEvent {
            kind: "snapshot".to_string(),
            source: "BINANCE".to_string(),
            updates: FlatbufferUpdate {
                bids: vec![(10, 2), (11, 3), (8, 6)],
                asks: vec![],
            },
        };
        agg_book.update(snapshot_event);

        let level = agg_book.bids.get(&11).unwrap();
        assert_eq!(level.tot_qty, 3);
        assert_eq!(level.qty_mp.get(&1).unwrap(), &3);
        assert_eq!(level.qty_mp.len(), 1);

        let level = agg_book.bids.get(&10).unwrap();
        assert_eq!(level.tot_qty, 4);
        assert_eq!(level.qty_mp.get(&1).unwrap(), &2);
        assert_eq!(level.qty_mp.get(&2).unwrap(), &2);

        let level = agg_book.bids.get(&8).unwrap();
        assert_eq!(level.tot_qty, 9);
        assert_eq!(level.qty_mp.get(&1).unwrap(), &6);
        assert_eq!(level.qty_mp.get(&2).unwrap(), &3);

        let level = agg_book.bids.get(&9);
        assert!(level.is_none());

        let level = agg_book.bids.get(&7).unwrap();
        assert_eq!(level.tot_qty, 15);
        assert!(level.qty_mp.get(&1).is_none());
        assert_eq!(level.qty_mp.get(&2).unwrap(), &15);

        let mut v = agg_book
            .b_prices
            .iter()
            .find_map(|(k, v)| if k == &1 { Some(v) } else { None })
            .unwrap()
            .iter()
            .collect::<Vec<&u64>>();
        v.sort();
        assert_eq!(v, Vec::from([&8, &10, &11]));
        /*******************************************************************************/
    }
}
