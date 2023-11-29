use async_wss::exchanges::coinbase::{spot::SpotWSClientBuilder};

#[tokio::main]
async fn main() {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "api=info");
    }
    pretty_env_logger::init();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    let mut ws_builder = SpotWSClientBuilder::default();
    // ws_builder.sub_trade("ETHUSDT");
    ws_builder.sub_ob_depth(&["BTC-USD"]);

    let ws_client = ws_builder
        .build()
        .await
        .expect("failed to get spot wss client");
    
    let mut ws_client = ws_client.connect(tx).await.expect("error connecting");

    // let mut ob_feed = BybitFeedManager::new(rx);
    // tokio::spawn(async move {
    //     ob_feed.run().await.unwrap();
    // });

    // tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    ws_client.sub_ob_depth(&["BTC-USD"]).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    // ws_client.unsub_ob_depth(50, "BTC-USDT").await.unwrap();
    // tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    
}
