use async_wss::exchanges::binance::spot::SpotWSClientBuilder;

#[tokio::main]
async fn main() {
    let mut ws_builder = SpotWSClientBuilder::default();
    ws_builder.sub_trade("ETHUSDT");
    ws_builder.sub_ob_depth("BTCUSDT");

    let ws_client = ws_builder
        .build()
        .await
        .expect("failed to get spot wss client");
    let mut ws_client = ws_client.connect().await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    ws_client.sub_trade("SOLUSDT").await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    ws_client.unsub_trade("ETHUSDT").await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    ws_client.unsub_ob_depth("BTCUSDT").await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
}
