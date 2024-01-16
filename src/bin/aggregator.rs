use async_wss::{
    aggregator::Aggregator,
    common::{FlatbufferEvent, ZenohEvent, DATA_FEED},
    spsc::{QueueError, SPSCQueue},
};
use std::thread;
use std::env;
use zenoh::{
    config::Config as ZenohConfig,
    key_expr::keyexpr,
    prelude::{sync::SyncResolve, Encoding, KnownEncoding},
};

fn main() {
    let (mut tx_z, mut rx_z) = SPSCQueue::new::<ZenohEvent>(50000);
    let (tx_f, mut rx_f) = SPSCQueue::new::<FlatbufferEvent>(50000);

    // consumes zenoh events and push to the queue
    let zenoh_rx_thread = thread::spawn(move || {
        
        let file_location = env::var("ZENOH_CONFIG_AGGREGATOR").expect("ZENOH_CONFIG_AGGREGATOR not set");        
        let conf = ZenohConfig::from_file(file_location).unwrap();
        let session = zenoh::open(conf).res().unwrap();

        let key_expr = keyexpr::new(DATA_FEED).expect("failed to get a zenoh key experession");
        let subscriber = session
            .declare_subscriber(key_expr)
            .res()
            .expect("failed to declare a zenoh subscriber");

        loop {
            let sample = subscriber.recv().expect("all senders have been dropped");

            let stream_id;
            match sample.encoding.prefix() {
                KnownEncoding::AppCustom => match sample.encoding.suffix() {
                    "update_event" => {
                        stream_id = 1;
                    }
                    "snapshot_event" => {
                        stream_id = 0;
                    }
                    "price_cli" => {
                        stream_id = 2;
                    }
                    _ => {
                        log::warn!("received unregistered event");
                        continue;
                    }
                },
                _ => {
                    log::warn!("received unknown encoding");
                    continue;
                }
            }
            let buff = sample
                .value
                .payload
                .zslices()
                .flat_map(|x| x.as_slice().to_owned())
                .collect::<Vec<u8>>();

            tx_z.push(ZenohEvent { stream_id, buff });
        }
    });

    // poll the queue for zenoh events and send to aggregator to process it
    let aggregator_thread = thread::spawn(move || {
        let mut aggregator = Aggregator::new(tx_f);
        loop {
            match rx_z.pop() {
                Ok(event) => {
                    if let Err(e) = aggregator.process(event) {
                        log::error!("{e}");
                    }

                    let mid_price = aggregator.get_mid_price("BTC-USDT-spot");
                    println!("mid price: {:?}", mid_price);
                    log::info!("Mid price: {:?}", mid_price);
                }
                Err(QueueError::EmptyQueue) => {
                    log::trace!("no zenoh event for the aggregator");
                }
            }
        }
    });

    // produce to remote/external services
    let extern_producing_thread = thread::spawn(move || {
        
        let file_location = env::var("ZENOH_CONFIG_PRODUCER").expect("ZENOH_CONFIG_PRODUCER not set");        
        let conf = ZenohConfig::from_file(file_location).unwrap();
        let session = zenoh::open(conf).res().unwrap();

        let key_expr = keyexpr::new(DATA_FEED).expect("failed to get a zenoh key experession");

        loop {
            match rx_f.pop() {
                Ok(event) => {
                    if event.stream_id == 3 {
                        let encoding = match Encoding::APP_CUSTOM.with_suffix("pricingDetails") {
                            Ok(enc) => enc,
                            Err(e) => {
                                log::error!("failed to encode pricing details\n{:?}", e);
                                continue;
                            }
                        };
                        if let Err(e) = session.put(key_expr, event.buff).encoding(encoding).res() {
                            log::error!("failed to send encoding for pricing details\n{:?}", e);
                        }
                    } else {
                        // continue;
                    }
                }
                Err(QueueError::EmptyQueue) => {
                    log::trace!("no event to be sent to external service")
                }
            }
        }
    });

    zenoh_rx_thread
        .join()
        .expect("zenoh event conusmer thread has panicked");
    aggregator_thread
        .join()
        .expect("aggregator thread has panicked");
    extern_producing_thread
        .join()
        .expect("external services producing thread has panicked");
}
