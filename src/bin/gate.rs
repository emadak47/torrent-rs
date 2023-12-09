use async_wss::exchanges::gate::{spot::SpotWSClientBuilder};
use async_wss::exchanges::gate::orderbook::GateFeedManager;

#[tokio::main]
async fn main() {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "api=info");
    }
    pretty_env_logger::init();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    let mut ws_builder = SpotWSClientBuilder::default();
    // ws_builder.sub_trade("ETHUSDT");
    ws_builder.sub_ob_depth("BTC_USDT");

    let ws_client = ws_builder
        .build()
        .await
        .expect("failed to get spot wss client");
    
    let mut ws_client = ws_client.connect(tx).await.expect("error connecting");

    let mut ob_feed = GateFeedManager::new(rx);
    tokio::spawn(async move {
        ob_feed.run().await.unwrap();
    });

    // tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    ws_client.sub_ob_depth("BTC_USDT").await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    // ws_client.unsub_ob_depth(50, "BTC-USDT").await.unwrap();
    // tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    
}


// #####################
// use serde::{Deserialize, Serialize};
// use serde_json;


// #[derive(Debug, Clone, Deserialize)]
// pub struct Level2Changes {
//     pub t: u64,
//     pub e: String,
//     pub E: u64,
//     pub s: String,
//     pub U: u64,
//     pub u: u64,
//     pub b: Vec<Vec<String>>,
//     pub a: Vec<Vec<String>>,
// }

// #[derive(Debug, Clone, Deserialize)]
// pub struct Level2 {
//     pub time: u64,
//     pub time_ms: u64,
//     pub channel: String,
//     pub event: String,
//     pub result: Level2Changes,
// }

// #[derive(Debug, Clone, Deserialize)]
// #[serde(untagged)]
// pub enum Event {
//     Level2 (Level2),
// }

// fn main() {
//     let json_str = r#"
//         {
//           "time": 1606294781,
//           "time_ms": 1606294781236,
//           "channel": "spot.order_book_update",
//           "event": "update",
//           "result": {
//             "t": 1606294781123,
//             "e": "depthUpdate",
//             "E": 1606294781,
//             "s": "BTC_USDT",
//             "U": 48776301,
//             "u": 48776306,
//             "b": [
//               ["19137.74", "0.0001"],
//               ["19088.37", "0"]
//             ],
//             "a": [["19137.75", "0.6135"]]
//           }
//         }
//     "#;

//     let event_result: Result<Event, serde_json::Error> = serde_json::from_str(json_str);

//     match event_result {
//         Ok(event) => {
//             println!("Successfully deserialized: {:#?}", event);
//         }
//         Err(e) => {
//             println!("Failed to deserialize: {:#?}", e);
//         }
//     }
// }
