use zenoh::config::Config;
use zenoh::prelude::sync::*;
use async_wss::spsc::QueueError;
use std::str::FromStr;
use std::thread;
use async_wss::aggregator::OrderBookAggregator;
use async_wss::aggregator::ZenohEvent;
use async_wss::spsc::SPSCQueue;
use async_wss::common::FlatbufferEvent;

fn main() {
    let (mut sender, mut reciever) = SPSCQueue::new::<ZenohEvent>(50000);
    let (mut sender_prod, mut reciever_prod) = SPSCQueue::new::<FlatbufferEvent>(50000);

    // consumes zenoh events and push to the queue
    let consumer_thread = thread::spawn(move || {
        let mut conf = Config::default();
        let session = zenoh::open(conf).res().unwrap();
        let datafeed = "atrimo/datafeeds";
        let key_expr = OwnedKeyExpr::from_str(datafeed).unwrap();
        let subscriber = session.declare_subscriber(&key_expr).res().unwrap();
        let mut data = Vec::new();
        loop {
            let sample = subscriber.recv().unwrap();

            match sample.encoding.prefix(){
                KnownEncoding::AppCustom => match sample.encoding.suffix() {
                    "update_event"=>{
                        println!("update_event");
                        data.clear();
                        for zslice in sample.value.payload.zslices() {
                            data.extend_from_slice(zslice.as_slice());
                        }
                        let ae = ZenohEvent {
                            streamid: 1,
                            buff: data.to_vec(),
                        };
                        sender.push(ae);
                    },
                    "snapshot_event"=>{
                        println!("snapshot_event");
                        data.clear();
                        for zslice in sample.value.payload.zslices() {
                            data.extend_from_slice(zslice.as_slice());
                        }
                        let ae = ZenohEvent {
                            streamid: 0,
                            buff: data.to_vec(),
                        };
                        sender.push(ae);
                    },
                    unknown =>println!("unknown")
                }
                unknown=> println!("unknown")
            }
        }
    });

    // // // busy poll the queue for zenoh events and process it
    let aggregator_thread = thread::spawn(move || {
        let mut aggregator = OrderBookAggregator::new(sender_prod);
        loop {
            match reciever.pop() {
                Ok(event) => {
                    aggregator.run(&event);
                }
                Err(QueueError::EmptyQueue) => {}
            }
        }
    });

    // produce to strategy client
    let producer_thread = thread::spawn(move || {
        let mut conf = Config::default();
        let session = zenoh::open(conf).res().unwrap();
        let datafeed = keyexpr::new("atrimo/aggregator").unwrap();
        loop {
            match reciever_prod.pop() {
                Ok(event) => {
                    session.put(datafeed, event.buff.clone()).res().unwrap();
                }
                Err(QueueError::EmptyQueue) => {}
            }
        }
    });

    consumer_thread
        .join()
        .expect("conusmer thread has panicked");
    aggregator_thread
        .join()
        .expect("aggregator thread has panicked");
    producer_thread
    .join()
    .expect("producer thread has panicked");
}
