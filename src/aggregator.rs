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
type Exchange = String;
type ExchangeQty = u64;

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
    
    fn get_best_bid(&mut self, instrument: String)-> Option<(u64, &Metadata)> {
        let book = self.books.get_mut(&instrument).unwrap(); 
        if let Some((best_price, meta)) = book.bid_book.iter().next_back() {
            //println!("Best Bid Price: {}", best_bid_price);
            Some((*best_price, meta))
        } else {
            //println!("No bids in the book.");
            None
        }
    }

    fn get_best_ask(&mut self, instrument: String)-> Option<(u64, &Metadata)> {
        let book = self.books.get_mut(&instrument).unwrap(); 
        if let Some((best_price, meta)) = book.ask_book.iter().next() {
            //println!("Best Ask Price: {}", best_ask_price);
            Some((*best_price, meta))
        } else {
            //println!("No asks in the book.");
            None
        }
    }

    fn clean_bid_book(&mut self, instrument: String, prices_to_remove: &Vec<Price>) {
        let book = self.books.get_mut(&instrument).unwrap(); 
        for &price in prices_to_remove.iter() {
            book.bid_book.remove(&price);
        }
    }

    fn clean_ask_book(&mut self, instrument: String, prices_to_remove: &Vec<Price>) {
        let book = self.books.get_mut(&instrument).unwrap(); 
        for &price in prices_to_remove.iter() {
            book.ask_book.remove(&price);
        }
    }

    fn make_snapshot_event(&mut self, instrument: String) {
        let mut bids_flat: Vec<Level> = Vec::new();
        let mut asks_flat: Vec<Level> = Vec::new();

        if let Some(book) = self.books.get_mut(&instrument) {
            for (price, metadata) in book.bid_book.iter().rev() {
                bids_flat.push(Level::new(*price, metadata.total_qty));   
            }
            for (price, metadata) in book.ask_book.iter() {
                asks_flat.push(Level::new(*price, metadata.total_qty));
            }
            let evnt = make_order_book_snapshot_event_aggregator(bids_flat, asks_flat, instrument.clone());
            self.producer_queue.push(evnt);
        }
    }

    // Whenever new snapshot events comes for particular exchange this gets called
    fn reset_exchange_book(&mut self, flatbuffers_data: Vec<u8>) {
        
        let snapshot = root_as_snapshot_event_message(&flatbuffers_data);
        let SnapshotEvent_ = snapshot.expect("UNABLE TO PARSE SNAPSHOT EVENT").snapshot_event();
        let instrument = SnapshotEvent_.expect("UNABLE TO PARSE INSTRUMENT").instrument();
        let exchange = SnapshotEvent_.expect("UNABLE TO PARSE EXCHANGE").exchange();
        let Snapshot_ = SnapshotEvent_.expect("UNABLE TO PARSE SNAPSHOT").snapshot(); 
        let bids_ = Snapshot_.expect("UNABLE TO PARSE SNAPSHOT BIDS").bids();
        let asks_ = Snapshot_.expect("UNABLE TO PARSE SNAPSHOT ASKS").asks();

        let mut bid_prices_to_remove = Vec::new();
        let mut ask_prices_to_remove = Vec::new();

        if let Some(book) = self.books.get_mut(&instrument.unwrap().to_string()) {
            // Reset the bid book for particular exchange
            for (price, metadata) in book.bid_book.iter_mut() {
                if let Some(qty) = metadata.exchanges_quantities.get_mut(&exchange.unwrap().to_string()) {
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
                if let Some(qty) = metadata.exchanges_quantities.get_mut(&exchange.unwrap().to_string()) {
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
            self.books.insert(instrument.unwrap().to_string(), new_book);
        }

        self.clean_ask_book(instrument.unwrap().to_string(), &ask_prices_to_remove);
        ask_prices_to_remove.clear();
        self.clean_bid_book(instrument.unwrap().to_string(), &bid_prices_to_remove);
        bid_prices_to_remove.clear();

        let book = self.books.get_mut(&instrument.unwrap().to_string()).unwrap(); 

        // insert the snapshot
        for bid in bids_ {
            for i in 0..bid.len() {
                if let Some(metadata) = book.bid_book.get_mut(&bid.get(i).price()) {
                    if let Some(exchange_qty) = metadata.exchanges_quantities.get_mut(&exchange.unwrap().to_string()) {
                        metadata.total_qty -= *exchange_qty;
                    }
                    metadata.total_qty += bid.get(i).qty();
                    metadata.exchanges_quantities.insert(exchange.unwrap().to_string(), bid.get(i).qty());
                } else {
                    let mut metadata = Metadata::new();
                    metadata.total_qty = bid.get(i).qty();
                    metadata.exchanges_quantities.insert(exchange.unwrap().to_string(), bid.get(i).qty());
                    book.bid_book.insert(bid.get(i).price(), metadata);
                }
            }
        }   

        for ask in asks_ {
            for i in 0..ask.len() {
                if let Some(metadata) = book.ask_book.get_mut(&ask.get(i).price()) {
                    if let Some(exchange_qty) = metadata.exchanges_quantities.get_mut(&exchange.unwrap().to_string()) {
                        metadata.total_qty -= *exchange_qty;
                    }
                    metadata.total_qty += ask.get(i).qty();
                    metadata.exchanges_quantities.insert(exchange.unwrap().to_string(), ask.get(i).qty());
                } else {
                    let mut metadata = Metadata::new();
                    metadata.total_qty = ask.get(i).qty();
                    metadata.exchanges_quantities.insert(exchange.unwrap().to_string(), ask.get(i).qty());
                    book.ask_book.insert(ask.get(i).price(), metadata);
                }
            }
        }   
    }

    // whenever new update event comes for a particular exchange it calls this function
    fn update_exchange_book(&mut self,flatbuffers_data: Vec<u8>) {

        let update = root_as_update_event_message(&flatbuffers_data);
        let UpdateEvent_ = update.expect("UNABLE TO PARSE UPDATE EVENT").update_event();
        let instrument = UpdateEvent_.expect("UNABLE TO PARSE INSTRUMENT").instrument();
        let exchange = UpdateEvent_.expect("UNABLE TO PARSE EXCHANGE").exchange();
        let update_ = UpdateEvent_.expect("UNABLE TO PARSE UPDATE").update(); 
        let bids_ = update_.expect("UNABLE TO PARSE UPDATE BIDS").bids();
        let asks_ = update_.expect("UNABLE TO PARSE UPDATE ASKS").asks();
        
        let book = self.books.get_mut(&instrument.unwrap().to_string()).unwrap(); 
        
        for bid in bids_ {
            for i in 0..bid.len() {
                if let Some(metadata) = book.bid_book.get_mut(&bid.get(i).price()) {
                    if bid.get(i).qty() == 0 {
                        if let Some(qty_) = metadata.exchanges_quantities.get(&exchange.unwrap().to_string()) {
                            metadata.total_qty -= *qty_;
                            metadata.exchanges_quantities.insert(exchange.unwrap().to_string(), 0);
                        }
                    } else {
                        if let Some(exchange_qty) = metadata.exchanges_quantities.get_mut(&exchange.unwrap().to_string()) {
                            metadata.total_qty -= *exchange_qty;
                        }
                        metadata.total_qty += bid.get(i).qty();
                        metadata.exchanges_quantities.insert(exchange.unwrap().to_string(), bid.get(i).qty());
                    }
                } else {
                    let mut metadata = Metadata::new();
                    metadata.total_qty = bid.get(i).qty();
                    metadata.exchanges_quantities.insert(exchange.unwrap().to_string(), bid.get(i).qty());
                    book.bid_book.insert(bid.get(i).price(), metadata);
                }
            }
        }   

        for ask in asks_ {
            for i in 0..ask.len() {
                if let Some(metadata) = book.ask_book.get_mut(&ask.get(i).price()) {
                    if ask.get(i).qty() == 0 {
                        if let Some(qty_) = metadata.exchanges_quantities.get(&exchange.unwrap().to_string()) {
                            metadata.total_qty -= *qty_;
                            metadata.exchanges_quantities.insert(exchange.unwrap().to_string(), 0);
                        }
                    } else {
                        if let Some(exchange_qty) = metadata.exchanges_quantities.get_mut(&exchange.unwrap().to_string()) {
                            metadata.total_qty -= *exchange_qty;
                        }
                        metadata.total_qty += ask.get(i).qty();
                        metadata.exchanges_quantities.insert(exchange.unwrap().to_string(), ask.get(i).qty());
                    }
                } else {
                    let mut metadata = Metadata::new();
                    metadata.total_qty = ask.get(i).qty();
                    metadata.exchanges_quantities.insert(exchange.unwrap().to_string(), ask.get(i).qty());
                    book.ask_book.insert(ask.get(i).price(), metadata);
                }
            }
        }
        
        self.make_snapshot_event(instrument.unwrap().to_string());
        //self.get_best_bid(instrument.unwrap().to_string());
        //self.print_best_ask(instrument.unwrap().to_string());
    }

    pub fn run(&mut self, data: &ZenohEvent) {
        if data.streamid == 0 { // snapshot
            let cloned_buff = data.buff.clone();
            self.reset_exchange_book(cloned_buff);
       } else if data.streamid == 1 { // update
           let cloned_buff = data.buff.clone();
           self.update_exchange_book(cloned_buff);
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
        
        aggregator.run(&zenoh_event);
        
        // check the best book details
        let best_bid = aggregator.get_best_bid(currency_pair.to_string()).unwrap();
        assert_eq!(30, best_bid.0);
        assert_eq!(300, best_bid.1.total_qty);
        // only 1 exchange
        assert_eq!(1, best_bid.1.exchanges_quantities.len());
        
        let best_ask = aggregator.get_best_ask(currency_pair.to_string()).unwrap();
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
        
        aggregator.run(&zenoh_event);

        // check the best book details again
        let best_bid = aggregator.get_best_bid(currency_pair.to_string()).unwrap();
        assert_eq!(300, best_bid.0);
        assert_eq!(3000, best_bid.1.total_qty);
        // only 1 exchange
        assert_eq!(1, best_bid.1.exchanges_quantities.len());
        
        let best_ask = aggregator.get_best_ask(currency_pair.to_string()).unwrap();
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
        aggregator.run(&zenoh_event);
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
        let start_times = Instant::now();

        aggregator.run(&zenoh_event);
        let elapsed_times = start_times.elapsed();
        println!("Elapsed time: {} nanoseconds", elapsed_times.as_nanos());
        // check the best book details again
        let best_bid = aggregator.get_best_bid(currency_pair.to_string()).unwrap();
        assert_eq!(30, best_bid.0);
        assert_eq!(1500, best_bid.1.total_qty);
        // only 1 exchange
        assert_eq!(1, best_bid.1.exchanges_quantities.len());

        let best_ask = aggregator.get_best_ask(currency_pair.to_string()).unwrap();
        assert_eq!(40, best_ask.0);
        assert_eq!(1500, best_ask.1.total_qty);

        // test each bid levels
        if let Some(book) = aggregator.books.get(&currency_pair.to_string()) {
            let bid_book = &book.bid_book;
            let level1 = bid_book.get(&20).unwrap();
            assert_eq!(level1.total_qty, 2000);
            let level2 = bid_book.get(&10).unwrap();
            assert_eq!(level2.total_qty, 1000);
        }

        // test each ask levels
        if let Some(book) = aggregator.books.get(&currency_pair.to_string()) {
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
    
        let currency_pair = CcyPair {
            base: String::from("btc"),
            quote: String::from("usd"),
            product: String::from("spot"),
        };  
        
        let evnt = make_okx_snapshot_event(bids, asks, currency_pair.clone());
        
        let zenoh_event = ZenohEvent {
            streamid: 0, // snapshot
            buff: evnt.buff, // flatbuffers
        };   
         
        aggregator.run(&zenoh_event);

        let best_bid = aggregator.get_best_bid(currency_pair.to_string()).unwrap();
        assert_eq!(30, best_bid.0);
        // 1500 (binance) + 600 (okx)
        assert_eq!(2100, best_bid.1.total_qty);
        // two exchanges
        assert_eq!(2, best_bid.1.exchanges_quantities.len());

        let best_ask = aggregator.get_best_ask(currency_pair.to_string()).unwrap();
        assert_eq!(40, best_ask.0);
        // 1500(binance) + 200 (okx)
        assert_eq!(1700, best_ask.1.total_qty);
        // two exchanges
        assert_eq!(2, best_ask.1.exchanges_quantities.len());
        assert_eq!(200, *best_ask.1.exchanges_quantities.get("okx").unwrap());
        assert_eq!(1500, *best_ask.1.exchanges_quantities.get("binance").unwrap());

        // test each bid levels quanitities
        if let Some(book) = aggregator.books.get(&currency_pair.to_string()) {
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
        if let Some(book) = aggregator.books.get(&currency_pair.to_string()) {
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
}