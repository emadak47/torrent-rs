use ::phf::phf_map;
use std::collections::{BTreeMap, HashMap, HashSet};
use crate::exchanges::binance::flatbuffer::snapshot::atrimo::snapshot_events::root_as_snapshot_event_message;
use crate::exchanges::binance::flatbuffer::orderbook::atrimo::update_events::root_as_update_event_message;
use crate::exchanges::binance::flatbuffer::event_factory::make_order_book_snapshot_event as make_binance_snapshot_event;
use crate::exchanges::okx::flatbuffer::event_factory::make_order_book_snapshot_event as make_okx_snapshot_event;
use crate::exchanges::binance::flatbuffer::event_factory::make_order_book_update_event as make_binance_update_event;
use crate::exchanges::okx::flatbuffer::event_factory::make_order_book_update_event as make_okx_update_event;
use crate::aggregator::ZenohEvent;
use std::time::{Instant, Duration};
use crate::common::CcyPair;
use crate::orderbook::l2::Level;

static EXCH_MP: phf::Map<&'static str, u8> = phf_map! {
    "binance" => 1,
    "okx" => 2,
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

type AggregatedBook = BTreeMap<u64, Meta>;
type Symbol = String;

pub struct Book {
    bids: AggregatedBook,
    asks: AggregatedBook,
    pub b_prices: HashMap<u8, HashSet<u64>>, // exchange identifier => set of price levels
    pub a_prices: HashMap<u8, HashSet<u64>>,
}

impl Book {
    pub fn new() -> Self {
        Book {
            bids:  AggregatedBook::new(),
            asks:  AggregatedBook::new(),
            b_prices:  HashMap::new(),
            a_prices:  HashMap::new(),            
        }
    }
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

pub struct AggBook {
    pub books: HashMap<Symbol, Book>,
}

impl AggBook {
    pub fn new() -> Self {
        Self {
            books: HashMap::new(),
        }
    }

    pub fn update_event(&mut self, flatbuffers_data: Vec<u8>) {

        let buffer: Vec<u8> = Vec::new();
        let update = root_as_update_event_message(&flatbuffers_data);
        let UpdateEvent_ = update.expect("UNABLE TO PARSE UPDATE EVENT").update_event();
        let instrument = UpdateEvent_.expect("UNABLE TO PARSE INSTRUMENT").instrument();
        let exchange = UpdateEvent_.expect("UNABLE TO PARSE EXCHANGE").exchange().unwrap().to_string();
        let update_ = UpdateEvent_.expect("UNABLE TO PARSE UPDATE").update(); 
        let bids_ = update_.expect("UNABLE TO PARSE UPDATE BIDS").bids();
        let asks_ = update_.expect("UNABLE TO PARSE UPDATE ASKS").asks();
        
        let identifier = EXCH_MP[&exchange];
        let book = self.books.get_mut(&instrument.unwrap().to_string()).unwrap(); 

        /***********************************  Bids  *************************************/
        for bid in bids_ {
            for i in 0..bid.len() {
                match book.bids.get_mut(&bid.get(i).price()) {
                    Some(meta) => {
                        // 1. add incoming qty
                        meta.tot_qty = (meta.tot_qty).checked_add(bid.get(i).qty()).expect(&format!(
                            "u64 addition `{}` to `{}` for exchange `{}`",
                            &bid.get(i).qty(),
                            &(meta.tot_qty),
                            &exchange,
                        ));

                        // 2. if there is some old qty of this exchange at this price level, deduct it.
                        if let Some(curr_qty) = meta.qty_mp.get(&identifier) {
                            meta.tot_qty = (meta.tot_qty).checked_sub(*curr_qty).expect(&format!(
                                "u64 substraction `{}` to `{}` for exchange `{}`",
                                &bid.get(i).qty(),
                                &(meta.tot_qty),
                                &exchange,
                            ));

                            // 3. remove this price level if tot_qty is 0
                            // meta.tot_qty can only be 0 at this point iff
                            // - the tot_qty is fully contributed by this exchange
                            // - and the incoming update qty is 0
                            if meta.tot_qty == 0 {
                                book.bids.remove(&bid.get(i).price());
                                if let Some(set) = book.b_prices.get_mut(&identifier) {
                                    set.remove(&bid.get(i).price());
                                }
                                continue;
                            }
                        }

                        // 4. update (or insert new) exchange qty
                        if bid.get(i).qty() != 0 {
                            // `insert` replaces value if key exists, otherwise inserts new.
                            meta.qty_mp.insert(identifier, bid.get(i).qty());
                        } else {
                            meta.qty_mp.remove(&identifier);
                        }

                        // 5. update exchange's price list
                        if bid.get(i).qty() != 0 {
                            if let Some(set) = book.b_prices.get_mut(&identifier) {
                                set.insert(bid.get(i).price());
                            } else {
                                book.b_prices.insert(identifier, HashSet::from([bid.get(i).price()]));
                            }
                        } else if bid.get(i).qty() == 0 && book.b_prices.get(&identifier).is_some() {
                            book.b_prices
                                .get_mut(&identifier)
                                .expect("is some")
                                .remove(&bid.get(i).price());
                        }
                    }
                    None => {
                        // create a new price level with tot_qty from this incoming update
                        let meta = Meta::new(bid.get(i).qty(), HashMap::from([(identifier, bid.get(i).qty())]));
                        book.bids.insert(bid.get(i).price(), meta);

                        // update exchange's price list
                        if let Some(set) = book.b_prices.get_mut(&identifier) {
                            set.insert(bid.get(i).price());
                        } else {
                            book.b_prices.insert(identifier, HashSet::from([bid.get(i).price()]));
                        }
                    }
                }
            }
        }

        /***********************************  Asks  *************************************/
        for ask in asks_ {
            for i in 0..ask.len() {
                match book.asks.get_mut(&ask.get(i).price()) {
                    Some(meta) => {
                        // 1. add incoming qty
                        meta.tot_qty = (meta.tot_qty).checked_add(ask.get(i).qty()).expect(&format!(
                            "u64 addition `{}` to `{}` for exchange `{}`",
                            &ask.get(i).qty(),
                            &(meta.tot_qty),
                            &exchange,
                        ));

                        // 2. if there is some old qty of this exchange at this price level, deduct it.
                        if let Some(curr_qty) = meta.qty_mp.get(&identifier) {
                            meta.tot_qty = (meta.tot_qty).checked_sub(*curr_qty).expect(&format!(
                                "u64 substraction `{}` to `{}` for exchange `{}`",
                                &ask.get(i).qty(),
                                &(meta.tot_qty),
                                &exchange,
                            ));

                            // 3. remove this price level if tot_qty is 0
                            // meta.tot_qty can only be 0 at this point iff
                            // - the tot_qty is fully contributed by this exchange
                            // - and the incoming update qty is 0
                            if meta.tot_qty == 0 {
                                book.asks.remove(&ask.get(i).price());
                                if let Some(set) = book.a_prices.get_mut(&identifier) {
                                    set.remove(&ask.get(i).price());
                                }
                                continue;
                            }
                        }

                        // 4. update (or insert new) exchange qty
                        if ask.get(i).qty() != 0 {
                            // `insert` replaces value if key exists, otherwise inserts new.
                            meta.qty_mp.insert(identifier, ask.get(i).qty());
                        } else {
                            meta.qty_mp.remove(&identifier);
                        }

                        // 5. update exchange's price list
                        if ask.get(i).qty() != 0 {
                            if let Some(set) = book.a_prices.get_mut(&identifier) {
                                set.insert(ask.get(i).price());
                            } else {
                                book.a_prices.insert(identifier, HashSet::from([ask.get(i).price()]));
                            }
                        } else if ask.get(i).qty() == 0 && book.a_prices.get(&identifier).is_some() {
                            book.a_prices
                                .get_mut(&identifier)
                                .expect("is some")
                                .remove(&ask.get(i).price());
                        }
                    }
                    None => {
                        // create a new price level with tot_qty from this incoming update
                        let meta = Meta::new(ask.get(i).qty(), HashMap::from([(identifier, ask.get(i).qty())]));
                        book.asks.insert(ask.get(i).price(), meta);

                        // update exchange's price list
                        if let Some(set) = book.a_prices.get_mut(&identifier) {
                            set.insert(ask.get(i).price());
                        } else {
                            book.a_prices.insert(identifier, HashSet::from([ask.get(i).price()]));
                        }
                    }
                }
            }
        }
    }

    pub fn snapshot_event(&mut self, flatbuffers_data: Vec<u8>) {

        let buffer: Vec<u8> = Vec::new();
        let snapshot = root_as_snapshot_event_message(&flatbuffers_data);
        let SnapshotEvent_ = snapshot.expect("UNABLE TO PARSE SNAPSHOT EVENT").snapshot_event();
        let instrument = SnapshotEvent_.expect("UNABLE TO PARSE INSTRUMENT").instrument();
        let exchange = SnapshotEvent_.expect("UNABLE TO PARSE EXCHANGE").exchange().unwrap().to_string();
        let Snapshot_ = SnapshotEvent_.expect("UNABLE TO PARSE SNAPSHOT").snapshot(); 
        let bids_ = Snapshot_.expect("UNABLE TO PARSE SNAPSHOT BIDS").bids();
        let asks_ = Snapshot_.expect("UNABLE TO PARSE SNAPSHOT ASKS").asks();

        let identifier = EXCH_MP[&exchange];
        self.books.entry(instrument.unwrap().to_string()).or_insert_with(Book::new);
        let book = self.books.get_mut(&instrument.unwrap().to_string()).unwrap(); 

        /***********************************  Update Bids  *************************************/

        for bid in bids_ {
            for i in 0..bid.len() {
                match book.bids.get_mut(&bid.get(i).price()) {
                    Some(meta) => {
                        // 1. add incoming qty
                        meta.tot_qty = (meta.tot_qty).checked_add(bid.get(i).qty()).expect(&format!(
                            "u64 addition `{}` to `{}` for exchange `{}`",
                            &bid.get(i).qty(),
                            &(meta.tot_qty),
                            &exchange,
                        ));

                        // 2. if there is some old qty of this exchange at this price level, deduct it.
                        if let Some(curr_qty) = meta.qty_mp.get(&identifier) {
                            meta.tot_qty = (meta.tot_qty).checked_sub(*curr_qty).expect(&format!(
                                "u64 substraction `{}` to `{}` for exchange `{}`",
                                &bid.get(i).qty(),
                                &(meta.tot_qty),
                                &exchange,
                            ));

                            // 3. remove this price level if tot_qty is 0
                            // meta.tot_qty can only be 0 at this point iff
                            // - the tot_qty is fully contributed by this exchange
                            // - and the incoming update qty is 0
                            if meta.tot_qty == 0 {
                                book.bids.remove(&bid.get(i).price());
                                if let Some(set) = book.b_prices.get_mut(&identifier) {
                                    set.remove(&bid.get(i).price());
                                }
                                continue;
                            }
                        }

                        // 4. update (or insert new) exchange qty
                        if bid.get(i).qty() != 0 {
                            // `insert` replaces value if key exists, otherwise inserts new.
                            meta.qty_mp.insert(identifier, bid.get(i).qty());
                        } else {
                            meta.qty_mp.remove(&identifier);
                        }

                        // 5. update exchange's price list
                        if bid.get(i).qty() != 0 {
                            if let Some(set) = book.b_prices.get_mut(&identifier) {
                                set.insert(bid.get(i).price());
                            } else {
                                book.b_prices.insert(identifier, HashSet::from([bid.get(i).price()]));
                            }
                        } else if bid.get(i).qty() == 0 && book.b_prices.get(&identifier).is_some() {
                            book.b_prices
                                .get_mut(&identifier)
                                .expect("is some")
                                .remove(&bid.get(i).price());
                        }
                    }
                    None => {
                        // create a new price level with tot_qty from this incoming update
                        let meta = Meta::new(bid.get(i).qty(), HashMap::from([(identifier, bid.get(i).qty())]));
                        book.bids.insert(bid.get(i).price(), meta);

                        // update exchange's price list
                        if let Some(set) = book.b_prices.get_mut(&identifier) {
                            set.insert(bid.get(i).price());
                        } else {
                            book.b_prices.insert(identifier, HashSet::from([bid.get(i).price()]));
                        }
                    }
                }
            }
        }
        /***********************************  Update Asks  *************************************/
        for ask in asks_ {
            for i in 0..ask.len() {
                match book.asks.get_mut(&ask.get(i).price()) {
                    Some(meta) => {
                        // 1. add incoming qty
                        meta.tot_qty = (meta.tot_qty).checked_add(ask.get(i).qty()).expect(&format!(
                            "u64 addition `{}` to `{}` for exchange `{}`",
                            &ask.get(i).qty(),
                            &(meta.tot_qty),
                            &exchange,
                        ));

                        // 2. if there is some old qty of this exchange at this price level, deduct it.
                        if let Some(curr_qty) = meta.qty_mp.get(&identifier) {
                            meta.tot_qty = (meta.tot_qty).checked_sub(*curr_qty).expect(&format!(
                                "u64 substraction `{}` to `{}` for exchange `{}`",
                                &ask.get(i).qty(),
                                &(meta.tot_qty),
                                &exchange,
                            ));

                            // 3. remove this price level if tot_qty is 0
                            // meta.tot_qty can only be 0 at this point iff
                            // - the tot_qty is fully contributed by this exchange
                            // - and the incoming update qty is 0
                            if meta.tot_qty == 0 {
                                book.asks.remove(&ask.get(i).price());
                                if let Some(set) = book.a_prices.get_mut(&identifier) {
                                    set.remove(&ask.get(i).price());
                                }
                                continue;
                            }
                        }

                        // 4. update (or insert new) exchange qty
                        if ask.get(i).qty() != 0 {
                            // `insert` replaces value if key exists, otherwise inserts new.
                            meta.qty_mp.insert(identifier, ask.get(i).qty());
                        } else {
                            meta.qty_mp.remove(&identifier);
                        }

                        // 5. update exchange's price list
                        if ask.get(i).qty() != 0 {
                            if let Some(set) = book.a_prices.get_mut(&identifier) {
                                set.insert(ask.get(i).price());
                            } else {
                                book.a_prices.insert(identifier, HashSet::from([ask.get(i).price()]));
                            }
                        } else if ask.get(i).qty() == 0 && book.a_prices.get(&identifier).is_some() {
                            book.a_prices
                                .get_mut(&identifier)
                                .expect("is some")
                                .remove(&ask.get(i).price());
                        }
                    }
                    None => {
                        // create a new price level with tot_qty from this incoming update
                        let meta = Meta::new(ask.get(i).qty(), HashMap::from([(identifier, ask.get(i).qty())]));
                        book.asks.insert(ask.get(i).price(), meta);

                        // update exchange's price list
                        if let Some(set) = book.a_prices.get_mut(&identifier) {
                            set.insert(ask.get(i).price());
                        } else {
                            book.a_prices.insert(identifier, HashSet::from([ask.get(i).price()]));
                        }
                    }
                }
            }
        }

        /***********************************  Bids  *************************************/
        // at this point, `price_set.len()` >= `incoming_prices.len()`
        let bid_price_set = book
            .b_prices
            .get(&identifier)
            .expect("must have length > 0 from updates above");
        
        let mut incoming_bid_prices: HashSet<u64> = HashSet::new();
        
        for bid in &bids_ {
            for i in 0..bid.len() {
                let price = bid.get(i).price();
                if bid.get(i).qty() != 0 {
                    incoming_bid_prices.insert(price);
                }
            }
        }

        let bid_prices_to_remove: HashSet<_> =
            bid_price_set.difference(&incoming_bid_prices).collect();
        for price in bid_prices_to_remove {
            let meta = book
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
                    book.bids.remove(price);

                    // no need to remove price level from `self.b_prices` here
                    // because it will be reset at the end
                }
            }
        }

        book.b_prices.insert(identifier, incoming_bid_prices);

        /***********************************  Asks  *************************************/
        // at this point, `price_set.len()` >= `incoming_prices.len()`
        let ask_price_set = book
            .a_prices
            .get(&identifier)
            .expect("must have length > 0 from updates above");

        let mut incoming_ask_prices: HashSet<u64> = HashSet::new();

        for ask in &asks_ {
            for i in 0..ask.len() {
                let price = ask.get(i).price();
                if ask.get(i).qty() != 0 {
                    incoming_ask_prices.insert(price);
                }
            }
        }

        let ask_prices_to_remove: HashSet<_> =
            ask_price_set.difference(&incoming_ask_prices).collect();
        for price in ask_prices_to_remove {
            let meta = book
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
                    book.asks.remove(price);

                    // no need to remove price level from `self.b_prices` here
                    // because it will be reset at the end
                }
            }
        }

        book.a_prices.insert(identifier, incoming_ask_prices);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_updates() {
        let mut agg_book = AggBook::new();
        /********************************* Set-up book *********************************/
        let asks: Vec<Level> = vec![
            Level { price: 40, qty: 100 },
            Level { price: 50, qty: 200 },
            Level { price: 60, qty: 300 },
        ];
    
        let bids: Vec<Level> = vec![
            Level { price: 10, qty: 100 },
            Level { price: 20, qty: 200 },
            Level { price: 30, qty: 300 },
        ];

        let currency_pair = CcyPair {
            base: String::from("btc"),
            quote: String::from("usd"),
            product: String::from("spot"),
        };  

        let evnt = make_binance_snapshot_event(bids, asks, currency_pair.clone());

        /********************************* Event 1 *********************************/
        
        let zenoh_event = ZenohEvent {
            streamid: 0, // snapshot
            buff: evnt.buff, // flatbuffers
        };    

        agg_book.snapshot_event(zenoh_event.buff);

        let book = agg_book.books.get_mut(&currency_pair.to_string()).unwrap(); 

        let level = book.bids.get(&30).unwrap();
        assert_eq!(level.tot_qty, 300);
        assert_eq!(level.qty_mp.get(&1).unwrap(), &300);

        let level = book.bids.get(&20).unwrap();
        assert_eq!(level.tot_qty, 200);
        assert!(!level.qty_mp.get(&1).is_none());
        assert_eq!(level.qty_mp.get(&1).unwrap(), &200);

        let level = book.bids.get(&10).unwrap();
        assert_eq!(level.tot_qty, 100);
        assert!(!level.qty_mp.get(&1).is_none());
        assert_eq!(level.qty_mp.get(&1).unwrap(), &100);

        let level = book.asks.get(&60).unwrap();
        assert_eq!(level.tot_qty, 300);
        assert_eq!(level.qty_mp.get(&1).unwrap(), &300);

        let level = book.asks.get(&50).unwrap();
        assert_eq!(level.tot_qty, 200);
        assert!(!level.qty_mp.get(&1).is_none());
        assert_eq!(level.qty_mp.get(&1).unwrap(), &200);

        let level = book.asks.get(&40).unwrap();
        assert_eq!(level.tot_qty, 100);
        assert!(!level.qty_mp.get(&1).is_none());
        assert_eq!(level.qty_mp.get(&1).unwrap(), &100);

        /********************************* Event 2 *********************************/
        let asks: Vec<Level> = vec![
            Level { price: 40, qty: 1500 }, // updated best ask quantity
            Level { price: 50, qty: 2000 },
            Level { price: 60, qty: 3000 },
        ];
    
        let bids: Vec<Level> = vec![
            Level { price: 10, qty: 1000 },
            Level { price: 20, qty: 2000 },
            Level { price: 30, qty: 1500 }, // updated best bid quantity
        ];
    
        let currency_pair = CcyPair {
            base: String::from("btc"),
            quote: String::from("usd"),
            product: String::from("spot"),
        };  

        let evnt = make_binance_update_event(bids, asks, currency_pair.clone());

        let zenoh_event = ZenohEvent {
            streamid: 1, // update
            buff: evnt.buff, // flatbuffers
        };   

        agg_book.update_event(zenoh_event.buff);

        let book = agg_book.books.get_mut(&currency_pair.to_string()).unwrap(); 

        let level = book.bids.get(&30).unwrap();
        assert_eq!(level.tot_qty, 1500);
        assert_eq!(level.qty_mp.get(&1).unwrap(), &1500);

        let level = book.bids.get(&20).unwrap();
        assert_eq!(level.tot_qty, 2000);
        assert!(!level.qty_mp.get(&1).is_none());
        assert_eq!(level.qty_mp.get(&1).unwrap(), &2000);

        let level = book.bids.get(&10).unwrap();
        assert_eq!(level.tot_qty, 1000);
        assert!(!level.qty_mp.get(&1).is_none());
        assert_eq!(level.qty_mp.get(&1).unwrap(), &1000);

        let level = book.asks.get(&60).unwrap();
        assert_eq!(level.tot_qty, 3000);
        assert_eq!(level.qty_mp.get(&1).unwrap(), &3000);

        let level = book.asks.get(&50).unwrap();
        assert_eq!(level.tot_qty, 2000);
        assert!(!level.qty_mp.get(&1).is_none());
        assert_eq!(level.qty_mp.get(&1).unwrap(), &2000);

        let level = book.asks.get(&40).unwrap();
        assert_eq!(level.tot_qty, 1500);
        assert!(!level.qty_mp.get(&1).is_none());
        assert_eq!(level.qty_mp.get(&1).unwrap(), &1500);

        let mut v = book
            .b_prices
            .iter()
            .find_map(|(k, v)| if k == &1 { Some(v) } else { None })
            .unwrap()
            .iter()
            .collect::<Vec<&u64>>();
        v.sort();
        assert_eq!(v, Vec::from([&10, &20, &30]));

        let mut v = book
            .a_prices
            .iter()
            .find_map(|(k, v)| if k == &1 { Some(v) } else { None })
            .unwrap()
            .iter()
            .collect::<Vec<&u64>>();
        v.sort();

        assert_eq!(v, Vec::from([&40, &50, &60]));
    }

    #[test]
    fn test_snapshot() {
        /********************************* Set-up book *********************************/
        let mut agg_book = AggBook::new();
        let asks: Vec<Level> = vec![
            Level { price: 40, qty: 100 },
            Level { price: 50, qty: 200 },
            Level { price: 60, qty: 300 },
        ];
    
        let bids: Vec<Level> = vec![
            Level { price: 10, qty: 100 },
            Level { price: 20, qty: 200 },
            Level { price: 30, qty: 300 },
        ];

        let currency_pair = CcyPair {
            base: String::from("btc"),
            quote: String::from("usd"),
            product: String::from("spot"),
        };  

        let evnt = make_binance_snapshot_event(bids, asks, currency_pair.clone());

        /********************************* Event 1 *********************************/
        
        let zenoh_event = ZenohEvent {
            streamid: 0, // snapshot
            buff: evnt.buff, // flatbuffers
        };    

        agg_book.snapshot_event(zenoh_event.buff);

        let book = agg_book.books.get_mut(&currency_pair.to_string()).unwrap(); 

        let level = book.bids.get(&30).unwrap();
        assert_eq!(level.tot_qty, 300);
        assert_eq!(level.qty_mp.get(&1).unwrap(), &300);

        let level = book.bids.get(&20).unwrap();
        assert_eq!(level.tot_qty, 200);
        assert!(!level.qty_mp.get(&1).is_none());
        assert_eq!(level.qty_mp.get(&1).unwrap(), &200);

        let level = book.bids.get(&10).unwrap();
        assert_eq!(level.tot_qty, 100);
        assert!(!level.qty_mp.get(&1).is_none());
        assert_eq!(level.qty_mp.get(&1).unwrap(), &100);

        let level = book.asks.get(&60).unwrap();
        assert_eq!(level.tot_qty, 300);
        assert_eq!(level.qty_mp.get(&1).unwrap(), &300);

        let level = book.asks.get(&50).unwrap();
        assert_eq!(level.tot_qty, 200);
        assert!(!level.qty_mp.get(&1).is_none());
        assert_eq!(level.qty_mp.get(&1).unwrap(), &200);

        let level = book.asks.get(&40).unwrap();
        assert_eq!(level.tot_qty, 100);
        assert!(!level.qty_mp.get(&1).is_none());
        assert_eq!(level.qty_mp.get(&1).unwrap(), &100);

        let mut v = book
            .b_prices
            .iter()
            .find_map(|(k, v)| if k == &1 { Some(v) } else { None })
            .unwrap()
            .iter()
            .collect::<Vec<&u64>>();
        v.sort();
        assert_eq!(v, Vec::from([&10, &20, &30]));
        let mut v = book
            .a_prices
            .iter()
            .find_map(|(k, v)| if k == &1 { Some(v) } else { None })
            .unwrap()
            .iter()
            .collect::<Vec<&u64>>();
        v.sort();
        assert_eq!(v, Vec::from([&40, &50, &60]));
        // /*******************************************************************************/
    }
}
