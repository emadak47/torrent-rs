use async_wss::exchanges::bullish::{ws::SpotWSClientBuilder};
use async_wss::exchanges::bullish::ws::WsEndPoint;
use async_wss::exchanges::bullish::orderbook::BullishFeedManager;
#[tokio::main]
async fn main() {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "api=info");
    }
    pretty_env_logger::init();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    let mut ws_builder = SpotWSClientBuilder::new(WsEndPoint::MULTI_ORDERBOOK);

    ws_builder.sub_ob_depth("BTC-USDC-PERP");

    let ws_client = ws_builder
        .build()
        .await
        .expect("failed to get spot wss client");

    let _ws_client = ws_client.connect(tx).await.expect("error connecting");

    let mut ob_feed = BullishFeedManager::new(rx).unwrap();
    tokio::spawn(async move {
        ob_feed.run().await.unwrap();
    });

    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    // _ws_client.sub_ob_depth("BTCUSD").await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    // ws_client.unsub_ob_depth("BTC-USDT").await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
}
