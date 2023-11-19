use std::collections::BTreeMap;
use std::collections::HashMap;
use crate::exchanges::binance::flatbuffer::orderbook::atrimo::update_events::root_as_update_event_message;
use crate::exchanges::binance::flatbuffer::snapshot::atrimo::snapshot_events::root_as_snapshot_event_message;
use crate::exchanges::binance::flatbuffer::event_factory::make_order_book_snapshot_event_aggregator;
use crate::exchanges::binance::flatbuffer::event_factory::make_order_book_update_event as make_binance_update_event;
use crate::exchanges::okx::flatbuffer::event_factory::make_order_book_update_event as make_okx_update_event;
use crate::orderbook::l2::Level;
use std::time::{Instant, Duration};
use crate::spsc::Producer;
use crate::common::FlatbufferEvent;
use crate::common::CcyPair;
use crate::spsc::SPSCQueue;
use crate::exchanges::binance::flatbuffer::event_factory::make_order_book_snapshot_event as make_binance_snapshot_event;
use crate::exchanges::okx::flatbuffer::event_factory::make_order_book_snapshot_event as make_okx_snapshot_event;
use crate::exchanges::binance::flatbuffer::pricinginfo::atrimo::pricing_events::PricingEvent;
use crate::exchanges::binance::flatbuffer::pricinginfo::atrimo::pricing_events::PricingEventArgs;
use crate::exchanges::binance::flatbuffer::pricinginfo::atrimo::pricing_events::finish_pricing_event_buffer;
use crate::common::PricingDetails;
use crate::common::ASSET_CONSTANT_MULTIPLIER;
use log::{info, trace, warn};

type Exchange = String;
type ExchangeQty = u64;

#[allow(dead_code)]
pub fn make_pricing_event_aggregator(
    details: &PricingDetails,
    instrument: &str,
) -> FlatbufferEvent {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);


    let pricing_event_message = PricingEvent::create(
        &mut builder,
        &PricingEventArgs {
            best_bid: details.best_bid,
            best_ask: details.best_ask,
            worse_bid: details.worse_bid,
            worse_ask: details.worse_ask,
            execution_bid: details.execution_bid,
            execution_ask: details.execution_ask,
            imbalance_bbo: details.imbalance_1,
            imbalance_25: details.imbalance_25,
            imbalance_50: details.imbalance_50,
            imbalance_75: details.imbalance_75,
            imbalance_100: details.imbalance_100,
            depth: details.depth,
        },
    );

    finish_pricing_event_buffer(&mut builder, pricing_event_message);
    let buffer = builder.finished_data().to_vec();
    FlatbufferEvent {
        stream_id: 3,
        buff: buffer,
    }
}
#[derive(Debug, Clone)]
pub struct ZenohEvent {
    pub streamid: u8,
    pub buff: Vec<u8>,
}

struct Metadata {
    total_qty: u64,
    exchanges_quantities: HashMap<Exchange, ExchangeQty>,
}

impl Metadata {
    fn new() -> Self {
        Metadata {
            total_qty: 0,
            exchanges_quantities: HashMap::new(),
        }
    }
}

type Price = u64;
type Symbol = String;
type AggregatedBook = BTreeMap<Price, Metadata>;

pub struct Book {
    bid_book: AggregatedBook,
    ask_book: AggregatedBook,
}

impl Book {
    pub fn new() -> Self {
        Book {
            bid_book:  AggregatedBook::new(),
            ask_book:  AggregatedBook::new(),
        }
    }
}

pub struct OrderBookAggregator {
    pub books: HashMap<Symbol, Book>,
    producer_queue: Producer<FlatbufferEvent>,
}

impl OrderBookAggregator {
    pub fn new(queue: Producer<FlatbufferEvent>) -> Self {
        Self {
            books: HashMap::new(),
            producer_queue: queue,
        }
    }
    
    fn get_best_bid(&self, instrument: &str) -> Option<(u64, &Metadata)> {
        let book = self.books.get(instrument)?;
        if let Some((best_price, meta)) = book.bid_book.iter().next_back() {
            Some((*best_price, meta))
        } else {
            None
        }
    }
    
    fn get_best_ask(&self, instrument: &str) -> Option<(u64, &Metadata)> {
        let book = self.books.get(instrument)?;
        if let Some((best_price, meta)) = book.ask_book.iter().next() {
            Some((*best_price, meta))
        } else {
            None
        }
    }
    
    fn calculate_mid_price(&self, instrument: &str) -> Option<u64> {
        let best_bid = self.get_best_bid(instrument)?;
        let best_ask = self.get_best_ask(instrument)?;
    
        Some((best_bid.0 + best_ask.0) / 2)
    }
    

    fn get_worse_ask(&mut self, instrument: &str)-> Option<(u64, &Metadata)> {
        let book = self.books.get_mut(instrument)?; 
        if let Some((best_price, meta)) = book.ask_book.iter().rev().next() {
            Some((*best_price, meta))
        } else {
            None
        }
    }

    fn get_worse_bid(&mut self, instrument: &str)-> Option<(u64, &Metadata)> {
        let book = self.books.get_mut(instrument)?; 
        if let Some((best_price, meta)) = book.bid_book.iter().rev().last() {
            Some((*best_price, meta))
        } else {
            None
        }
    }

    fn get_execution_bid(&mut self, instrument: &str, amount: Option<u64>) -> Option<u64> {
        let book = self.books.get_mut(instrument)?;
    
        match amount {
            Some(amount) => {
                let mut amt = 0;
                let mut avg = 0;
                let mut impact_price = 0;
    
                for (price, metadata) in book.bid_book.iter().rev() {
                    if amt + metadata.total_qty >= amount {
                        let x = amount - amt;
                        avg += *price * x;
                        impact_price = avg / amount;
                        return Some(impact_price);
                    } else {
                        amt += metadata.total_qty;
                        avg += *price * metadata.total_qty;
                    }
                }
                return Some(impact_price);
            }
            None => Some(self.get_best_ask(instrument).unwrap().0), 
        }
    }

    fn get_execution_ask(&mut self, instrument: &str, amount: Option<u64>) -> Option<u64> {
        let book = self.books.get_mut(instrument)?;
    
        match amount {
            Some(amount) => {
                let mut amt = 0;
                let mut avg = 0;
                let mut impact_price = 0;
                for (price, metadata) in book.ask_book.iter() {
                    if amt + metadata.total_qty >= amount {
                        let x = amount - amt;
                        avg += *price * x;
                        impact_price = avg / amount;
                        return Some(impact_price);
                    } else {
                        amt += metadata.total_qty;
                        avg += *price * metadata.total_qty;
                    }
                }
                return Some(impact_price);
            }
            None => Some(self.get_best_bid(instrument).unwrap().0), 
        }
    }

    fn get_total_bid_quantity(&mut self, instrument: &str, level: usize) -> Option<f32> {
        let mut parts = instrument.split('_');
        let base = parts.next()?;
        let quote = parts.next()?;
        let baseMultiplier = ASSET_CONSTANT_MULTIPLIER[base];

        let book = self.books.get_mut(instrument)?;
        let result = book.bid_book
                    .iter()
                    .rev()
                    .take(level)
                    .map(|(_, metadata)| metadata.total_qty as f32 / baseMultiplier as f32) 
                    .sum();
        
        Some(result)
    }   
    
    fn get_total_ask_quantity(&mut self, instrument: &str, level: usize) -> Option<f32> {
        let mut parts = instrument.split('_');
        let base = parts.next()?;
        let baseMultiplier = ASSET_CONSTANT_MULTIPLIER[base];

        let book = self.books.get_mut(instrument)?;
        let result = book.ask_book
                    .iter()
                    .take(level)
                    .map(|(_, metadata)| metadata.total_qty as f32 / baseMultiplier as f32) 
                    .sum();
        Some(result)
    }

    fn get_imbalance(&mut self, instrument: &str) -> Option<Vec<f32>> {
        let mut imbalances = Vec::new();

        for level in [1, 25, 50, 75, 100] {
            let bid_qty = self.get_total_bid_quantity(instrument, level)?;
            let ask_qty = self.get_total_ask_quantity(instrument, level)?;
            if bid_qty > 0.0 || ask_qty > 0.0 {
                let imbalance = (ask_qty - bid_qty) as f32 / (ask_qty + bid_qty) as f32;
                imbalances.push(imbalance);
            }
        }
        if imbalances.is_empty() {
            None
        } else {
            Some(imbalances)
        }
    }

    fn make_pricing_event(&mut self, instrument: &str, amount: Option<u64>) -> Option<()> {
        let mut parts = instrument.split('_');
        let base = parts.next()?;
        let quote = parts.next()?;
        let quoteMultiplier = ASSET_CONSTANT_MULTIPLIER[quote];
        let baseMultiplier = ASSET_CONSTANT_MULTIPLIER[base];

        let best_bid_price = self.get_best_bid(instrument).map(|(price, _)| price).unwrap_or(0);
        let best_bid_price = (best_bid_price as f32 / quoteMultiplier as f32) as f32;
        
        let best_ask_price = self.get_best_ask(instrument).map(|(price, _)| price).unwrap_or(0);
        let best_ask_price = (best_ask_price as f32 / quoteMultiplier as f32) as f32;
        
        let worse_ask_price = self.get_worse_ask(instrument).map(|(price, _)| price).unwrap_or(0);
        let worse_ask_price = (worse_ask_price as f32 / quoteMultiplier as f32) as f32;
        
        let worse_bid_price = self.get_worse_bid(instrument).map(|(price, _)| price).unwrap_or(0);
        let worse_bid_price = (worse_bid_price as f32 / quoteMultiplier as f32) as f32;
        
        let execution_bid_price = self.get_execution_bid(instrument, amount).unwrap_or(0);
        let execution_bid_price = (execution_bid_price as f32 / quoteMultiplier as f32) as f32;
        
        let execution_ask_price = self.get_execution_ask(instrument, amount).unwrap_or(0);
        let execution_ask_price = (execution_ask_price as f32 / quoteMultiplier as f32) as f32;

        let imbalances = self.get_imbalance(instrument)?;
        let imbalance_1 = imbalances.get(0).cloned().unwrap_or(0.0);
        let imbalance_25 = imbalances.get(1).cloned().unwrap_or(0.0);
        let imbalance_50 = imbalances.get(2).cloned().unwrap_or(0.0);
        let imbalance_75 = imbalances.get(3).cloned().unwrap_or(0.0);
        let imbalance_100 = imbalances.get(4).cloned().unwrap_or(0.0);
        
        let pricing_details = PricingDetails {
            best_bid: best_bid_price,
            best_ask: best_ask_price,
            worse_bid: worse_bid_price,
            worse_ask: worse_ask_price,
            execution_bid: execution_bid_price,
            execution_ask: execution_ask_price, 
            imbalance_1: imbalance_1,
            imbalance_25: imbalance_25,
            imbalance_50: imbalance_50,
            imbalance_75: imbalance_75,
            imbalance_100: imbalance_100,
            depth: 0,
        };

        let evnt = make_pricing_event_aggregator(&pricing_details, instrument);
        self.producer_queue.push(evnt);
        Some(())
    }

    fn clean_bid_book(&mut self, instrument: &str, mut prices_to_remove: Vec<Price>) -> Option<()> {
        let book = self.books.get_mut(instrument)?; 
        for &price in prices_to_remove.iter() {
            book.bid_book.remove(&price);
        }
        prices_to_remove.clear();
        Some(())
    }

    fn clean_ask_book(&mut self, instrument: &str, mut prices_to_remove: Vec<Price>) -> Option<()> {
        let book = self.books.get_mut(instrument)?; 
        for &price in prices_to_remove.iter() {
            book.ask_book.remove(&price);
        }
        prices_to_remove.clear();
        Some(())
    }

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
            let evnt = make_order_book_snapshot_event_aggregator(bids_flat, asks_flat, instrument);
            self.producer_queue.push(evnt);
        }
    }

    // Whenever new snapshot events comes for particular exchange this gets called
    fn reset_exchange_book(&mut self, flatbuffers_data: Vec<u8>) {
        
        let snapshot = root_as_snapshot_event_message(&flatbuffers_data);
        let snapshot_event = snapshot.expect("UNABLE TO PARSE SNAPSHOT EVENT").snapshot_event();
        let instrument = snapshot_event.expect("UNABLE TO PARSE INSTRUMENT").instrument().unwrap();
        let exchange = snapshot_event.expect("UNABLE TO PARSE EXCHANGE").exchange().unwrap();
        let Snapshot_ = snapshot_event.expect("UNABLE TO PARSE SNAPSHOT").snapshot(); 
        let bids = Snapshot_.expect("UNABLE TO PARSE SNAPSHOT BIDS").bids();
        let asks = Snapshot_.expect("UNABLE TO PARSE SNAPSHOT ASKS").asks();

        let mut bid_prices_to_remove = Vec::new();
        let mut ask_prices_to_remove = Vec::new();

        log::info!("Exchange : {} Snapshot Event For Symbol {}", exchange, instrument);

        if let Some(book) = self.books.get_mut(instrument) {
            // Reset the bid book for particular exchange
            for (price, metadata) in book.bid_book.iter_mut() {
                if let Some(qty) = metadata.exchanges_quantities.get_mut(exchange) {
                    if *qty != 0 {
                        metadata.total_qty -= *qty;
                        *qty = 0;
                        if metadata.total_qty == 0 {
                            bid_prices_to_remove.push(*price);
                        }
                    }
                }
            }   
            // Reset the ask book for particular exchange
            for (price, metadata) in book.ask_book.iter_mut() {
                if let Some(qty) = metadata.exchanges_quantities.get_mut(exchange) {
                    if *qty != 0 {
                        metadata.total_qty -= *qty;
                        *qty = 0;
                        if metadata.total_qty == 0 {
                            ask_prices_to_remove.push(*price);

                        }
                    }
                }
            }
        } else {
            let mut new_book = Book::new();
            self.books.insert(instrument.to_string(), new_book);
        }

        self.clean_ask_book(instrument, ask_prices_to_remove);
        self.clean_bid_book(instrument, bid_prices_to_remove);

        let book = self.books.get_mut(instrument).unwrap(); 

        // insert the snapshot
        for bid in bids {
            for i in 0..bid.len() {
                if let Some(metadata) = book.bid_book.get_mut(&bid.get(i).price()) {
                    if let Some(exchange_qty) = metadata.exchanges_quantities.get_mut(exchange) {
                        metadata.total_qty -= *exchange_qty;
                    }
                    metadata.total_qty += bid.get(i).qty();
                    metadata.exchanges_quantities.insert(exchange.to_string(), bid.get(i).qty());
                } else {
                    let mut metadata = Metadata::new();
                    metadata.total_qty = bid.get(i).qty();
                    metadata.exchanges_quantities.insert(exchange.to_string(), bid.get(i).qty());
                    book.bid_book.insert(bid.get(i).price(), metadata);
                }
            }
        }   

        for ask in asks {
            for i in 0..ask.len() {
                if let Some(metadata) = book.ask_book.get_mut(&ask.get(i).price()) {
                    if let Some(exchange_qty) = metadata.exchanges_quantities.get_mut(exchange) {
                        metadata.total_qty -= *exchange_qty;
                    }
                    metadata.total_qty += ask.get(i).qty();
                    metadata.exchanges_quantities.insert(exchange.to_string(), ask.get(i).qty());
                } else {
                    let mut metadata = Metadata::new();
                    metadata.total_qty = ask.get(i).qty();
                    metadata.exchanges_quantities.insert(exchange.to_string(), ask.get(i).qty());
                    book.ask_book.insert(ask.get(i).price(), metadata);
                }
            }
        }   
    }

    // whenever new update event comes for a particular exchange it calls this function
    fn update_exchange_book(&mut self,flatbuffers_data: Vec<u8>) {

        let update = root_as_update_event_message(&flatbuffers_data);
        let update_event = update.expect("UNABLE TO PARSE UPDATE EVENT").update_event();
        let instrument = update_event.expect("UNABLE TO PARSE INSTRUMENT").instrument().unwrap();
        let exchange = update_event.expect("UNABLE TO PARSE EXCHANGE").exchange().unwrap();
        let update_ = update_event.expect("UNABLE TO PARSE UPDATE").update(); 
        let bids = update_.expect("UNABLE TO PARSE UPDATE BIDS").bids();
        let asks = update_.expect("UNABLE TO PARSE UPDATE ASKS").asks();
        
        let book = self.books.get_mut(instrument).unwrap(); 

        info!("Exchange : {} Update Event For Symbol {}", exchange, instrument);

        for bid in bids {
            for i in 0..bid.len() {
                if let Some(metadata) = book.bid_book.get_mut(&bid.get(i).price()) {
                    if bid.get(i).qty() == 0 {
                        if let Some(qty_) = metadata.exchanges_quantities.get(exchange) {
                            metadata.total_qty -= *qty_;
                            metadata.exchanges_quantities.insert(exchange.to_string(), 0);
                        }
                    } else {
                        if let Some(exchange_qty) = metadata.exchanges_quantities.get_mut(exchange) {
                            metadata.total_qty -= *exchange_qty;
                        }
                        metadata.total_qty += bid.get(i).qty();
                        metadata.exchanges_quantities.insert(exchange.to_string(), bid.get(i).qty());
                    }
                } else {
                    let mut metadata = Metadata::new();
                    metadata.total_qty = bid.get(i).qty();
                    metadata.exchanges_quantities.insert(exchange.to_string(), bid.get(i).qty());
                    book.bid_book.insert(bid.get(i).price(), metadata);
                }
            }
        }   

        for ask in asks {
            for i in 0..ask.len() {
                if let Some(metadata) = book.ask_book.get_mut(&ask.get(i).price()) {
                    if ask.get(i).qty() == 0 {
                        if let Some(qty_) = metadata.exchanges_quantities.get(exchange) {
                            metadata.total_qty -= *qty_;
                            metadata.exchanges_quantities.insert(exchange.to_string(), 0);
                        }
                    } else {
                        if let Some(exchange_qty) = metadata.exchanges_quantities.get_mut(exchange) {
                            metadata.total_qty -= *exchange_qty;
                        }
                        metadata.total_qty += ask.get(i).qty();
                        metadata.exchanges_quantities.insert(exchange.to_string(), ask.get(i).qty());
                    }
                } else {
                    let mut metadata = Metadata::new();
                    metadata.total_qty = ask.get(i).qty();
                    metadata.exchanges_quantities.insert(exchange.to_string(), ask.get(i).qty());
                    book.ask_book.insert(ask.get(i).price(), metadata);
                }
            }
        }
        
        self.make_snapshot_event(instrument);
        info!("Aggregator Mid Price : {:?}", self.calculate_mid_price(instrument));
    }

    pub fn run(&mut self, data: ZenohEvent) {
        if data.streamid == 0 { // snapshot
            self.reset_exchange_book(data.buff);
       } else if data.streamid == 1 { // update
           self.update_exchange_book(data.buff);
       } else if data.streamid == 2 { // Pricing Details
       }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reset_snapshot() {
        let (mut sender_prod, mut reciever_prod) = SPSCQueue::new::<FlatbufferEvent>(20);
        let mut aggregator = OrderBookAggregator::new(sender_prod);
        
        /*********************************Snapshot Event 1 *********************************/
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
    
        let currencyPair = CcyPair {
            base: String::from("btc"),
            quote: String::from("usd"),
            product: String::from("spot"),
        };  

        let currency_pair = currencyPair.to_string();
        
        let evnt = make_binance_snapshot_event(bids, asks, currencyPair.clone());
        
        let zenoh_event = ZenohEvent {
            streamid: 0, // snapshot
            buff: evnt.buff, // flatbuffers
        };    
        
        aggregator.run(zenoh_event);
        
        // check the best book details
        let best_bid = aggregator.get_best_bid(&currency_pair).unwrap();
        assert_eq!(30, best_bid.0);
        assert_eq!(300, best_bid.1.total_qty);
        // only 1 exchange
        assert_eq!(1, best_bid.1.exchanges_quantities.len());
        
        let best_ask = aggregator.get_best_ask(&currency_pair).unwrap();
        assert_eq!(40, best_ask.0);
        assert_eq!(100, best_ask.1.total_qty);
        // only 1 exchange
        assert_eq!(1, best_ask.1.exchanges_quantities.len());
        
        /*********************************Snapshot Event 2 *********************************/
        // resetting the orderbook again
        let asks: Vec<Level> = vec![
            Level { price: 400, qty: 1000 },
            Level { price: 500, qty: 2000 },
            Level { price: 600, qty: 3000 },
        ];
    
        let bids: Vec<Level> = vec![
            Level { price: 100, qty: 1000 },
            Level { price: 200, qty: 2000 },
            Level { price: 300, qty: 3000 },
        ];
    
        let currencyPair = CcyPair {
            base: String::from("btc"),
            quote: String::from("usd"),
            product: String::from("spot"),
        };  

        let currency_pair = currencyPair.to_string();
        
        let evnt = make_binance_snapshot_event(bids, asks, currencyPair.clone());
        
        let zenoh_event = ZenohEvent {
            streamid: 0, // snapshot
            buff: evnt.buff, // flatbuffers
        };    
        
        aggregator.run(zenoh_event);

        // check the best book details again
        let best_bid = aggregator.get_best_bid(&currency_pair).unwrap();
        assert_eq!(300, best_bid.0);
        assert_eq!(3000, best_bid.1.total_qty);
        // only 1 exchange
        assert_eq!(1, best_bid.1.exchanges_quantities.len());
        
        let best_ask = aggregator.get_best_ask(&currency_pair).unwrap();
        assert_eq!(400, best_ask.0);
        assert_eq!(1000, best_ask.1.total_qty);
        // only 1 exchange
        assert_eq!(1, best_ask.1.exchanges_quantities.len());
    }

    #[test]
    fn test_update_exchange_book() {
        let (mut sender_prod, mut reciever_prod) = SPSCQueue::new::<FlatbufferEvent>(20);
        let mut aggregator = OrderBookAggregator::new(sender_prod);
        
        /********************************* Snapshot Event 1 *********************************/
        /********************************* Binance *********************************/
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
        
        let zenoh_event = ZenohEvent {
            streamid: 0, // snapshot
            buff: evnt.buff, // flatbuffers
        };    
        aggregator.run(zenoh_event);
        /********************************* Update Event 1 *********************************/
        /********************************* Binance *********************************/
        
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
    
        let currencyPair = CcyPair {
            base: String::from("btc"),
            quote: String::from("usd"),
            product: String::from("spot"),
        };  

        let currency_pair = currencyPair.to_string();

        let evnt = make_binance_update_event(bids, asks, currencyPair.clone());

        let zenoh_event = ZenohEvent {
            streamid: 1, // update
            buff: evnt.buff, // flatbuffers
        };   

        aggregator.run(zenoh_event);
        // check the best book details again
        let best_bid = aggregator.get_best_bid(&currency_pair).unwrap();
        assert_eq!(30, best_bid.0);
        assert_eq!(1500, best_bid.1.total_qty);
        // only 1 exchange
        assert_eq!(1, best_bid.1.exchanges_quantities.len());

        let best_ask = aggregator.get_best_ask(&currency_pair).unwrap();
        assert_eq!(40, best_ask.0);
        assert_eq!(1500, best_ask.1.total_qty);

        // test each bid levels
        if let Some(book) = aggregator.books.get(&currency_pair) {
            let bid_book = &book.bid_book;
            let level1 = bid_book.get(&20).unwrap();
            assert_eq!(level1.total_qty, 2000);
            let level2 = bid_book.get(&10).unwrap();
            assert_eq!(level2.total_qty, 1000);
        }

        // test each ask levels
        if let Some(book) = aggregator.books.get(&currency_pair) {
            let ask_book = &book.ask_book;
            let level1 = ask_book.get(&50).unwrap();
            assert_eq!(level1.total_qty, 2000);
            let level2 = ask_book.get(&60).unwrap();
            assert_eq!(level2.total_qty, 3000);
        }

        /********************************* Snapshot Event 1 *********************************/
        /********************************* okx *********************************/
        let asks: Vec<Level> = vec![
            Level { price: 40, qty: 200 },
            Level { price: 50, qty: 400 },
            Level { price: 60, qty: 600 },
        ];
    
        let bids: Vec<Level> = vec![
            Level { price: 10, qty: 200 },
            Level { price: 20, qty: 400 },
            Level { price: 30, qty: 600 },
        ];
    
        
        let evnt = make_okx_snapshot_event(bids, asks, currencyPair.clone());
        
        let zenoh_event = ZenohEvent {
            streamid: 0, // snapshot
            buff: evnt.buff, // flatbuffers
        };   
         
        aggregator.run(zenoh_event);

        let best_bid = aggregator.get_best_bid(&currency_pair).unwrap();
        assert_eq!(30, best_bid.0);
        // 1500 (binance) + 600 (okx)
        assert_eq!(2100, best_bid.1.total_qty);
        // two exchanges
        assert_eq!(2, best_bid.1.exchanges_quantities.len());

        let best_ask = aggregator.get_best_ask(&currency_pair).unwrap();
        assert_eq!(40, best_ask.0);
        // 1500(binance) + 200 (okx)
        assert_eq!(1700, best_ask.1.total_qty);
        // two exchanges
        assert_eq!(2, best_ask.1.exchanges_quantities.len());
        assert_eq!(200, *best_ask.1.exchanges_quantities.get("okx").unwrap());
        assert_eq!(1500, *best_ask.1.exchanges_quantities.get("binance").unwrap());

        // test each bid levels quanitities
        if let Some(book) = aggregator.books.get(&currency_pair) {
            let bid_book = &book.bid_book;
            // 2000(binance) + 400(okx)
            let level1 = bid_book.get(&20).unwrap();
            assert_eq!(level1.total_qty, 2400);
            assert_eq!(400, *level1.exchanges_quantities.get("okx").unwrap());
            assert_eq!(2000, *level1.exchanges_quantities.get("binance").unwrap());
            let level2 = bid_book.get(&10).unwrap();
            // 1000(binance) + 200(okx)
            assert_eq!(level2.total_qty, 1200);
            assert_eq!(200, *level2.exchanges_quantities.get("okx").unwrap());
            assert_eq!(1000, *level2.exchanges_quantities.get("binance").unwrap());
        }

        // test each ask levels quanitities
        if let Some(book) = aggregator.books.get(&currency_pair) {
            let ask_book = &book.ask_book;
            let level1 = ask_book.get(&50).unwrap();
            // 2000(binance) + 400(okx)
            assert_eq!(level1.total_qty, 2400);
            assert_eq!(400, *level1.exchanges_quantities.get("okx").unwrap());
            assert_eq!(2000, *level1.exchanges_quantities.get("binance").unwrap());
            let level2 = ask_book.get(&60).unwrap();
            // 3000(binance) + 600(okx)
            assert_eq!(level2.total_qty, 3600);
            assert_eq!(600, *level2.exchanges_quantities.get("okx").unwrap());
            assert_eq!(3000, *level2.exchanges_quantities.get("binance").unwrap());
        }
    }
    #[test]
    fn test_events() {
        // Testing PricingDetails event
        let (mut sender_prod, mut reciever_prod) = SPSCQueue::new::<FlatbufferEvent>(20);
        let mut aggregator = OrderBookAggregator::new(sender_prod);
        
        /********************************* Snapshot Event 1 *********************************/
        /********************************* Binance *********************************/
        let asks: Vec<Level> = vec![
            Level { price: 40, qty: 80 },
            Level { price: 50, qty: 200 },
            Level { price: 60, qty: 300 },
        ];
    
        let bids: Vec<Level> = vec![
            Level { price: 10, qty: 100 },
            Level { price: 20, qty: 200 },
            Level { price: 30, qty: 300 },
        ];
    
        let currencyPair = CcyPair {
            base: String::from("BTC"),
            quote: String::from("USDT"),
            product: String::from("SPOT"),
        };  
        
        let currency_pair = currencyPair.to_string();

        let evnt = make_binance_snapshot_event(bids, asks, currencyPair.clone());

        let zenoh_event = ZenohEvent {
            streamid: 0, // snapshot
            buff: evnt.buff, // flatbuffers
        };   
         
        aggregator.run(zenoh_event);
        
        // 2 levels total asks quantity
        let result = aggregator.get_total_ask_quantity(&currency_pair, 2).unwrap(); 
        assert_eq!(result, 0.0000028);

        // 2 levels total bids quantity
        let result = aggregator.get_total_bid_quantity(&currency_pair, 2).unwrap(); 
        assert_eq!(result, 0.000005);

        // Execution average ask price
        let result = aggregator.get_execution_ask(&currency_pair, Some(100)).unwrap();
        assert_eq!(result, 42);

        // Execution average bid price
        let result = aggregator.get_execution_bid(&currency_pair, Some(100)).unwrap();
        assert_eq!(result, 30);
        
        let result = aggregator.get_best_ask(&currency_pair).unwrap().0;
        assert_eq!(result, 40);
        
        let result = aggregator.get_best_bid(&currency_pair).unwrap().0;
        assert_eq!(result, 30);
        
        let result = aggregator.get_worse_bid(&currency_pair).unwrap().0;
        assert_eq!(result, 10);
        
        let result = aggregator.get_worse_ask(&currency_pair).unwrap().0;
        assert_eq!(result, 60);


        let asks: Vec<Level> = vec![
            Level { price: 40, qty: 800000000000 },
            Level { price: 50, qty: 200000000000 },
            Level { price: 60, qty: 300000000000 },
        ];
    
        let bids: Vec<Level> = vec![
            Level { price: 10, qty: 100000000000 },
            Level { price: 20, qty: 200000000000 },
            Level { price: 30, qty: 300000000000 },
        ];

        let currencyPair = CcyPair {
            base: String::from("BTC"),
            quote: String::from("USDT"),
            product: String::from("SPOT"),
        };  

        let currency_pair = currencyPair.to_string();

        let evnt = make_binance_snapshot_event(bids, asks, currencyPair.clone());
        
        let zenoh_event = ZenohEvent {
            streamid: 0, // snapshot
            buff: evnt.buff, // flatbuffers
        };   
         
        aggregator.run(zenoh_event);

        let imbalances = aggregator.get_imbalance(&currency_pair).unwrap();
        let imbalance_1 = imbalances.get(0).cloned().unwrap_or(0.0);
        assert_eq!(imbalance_1, 0.45454547);
        let imbalance_25 = imbalances.get(1).cloned().unwrap_or(0.0);
        assert_eq!(imbalance_25, 0.36842105);
        let imbalance_50 = imbalances.get(2).cloned().unwrap_or(0.0);
        assert_eq!(imbalance_25, 0.36842105);
        let imbalance_75 = imbalances.get(3).cloned().unwrap_or(0.0);
        assert_eq!(imbalance_25, 0.36842105);
        let imbalance_100 = imbalances.get(4).cloned().unwrap_or(0.0);
        assert_eq!(imbalance_25, 0.36842105);
    }
}