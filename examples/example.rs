use async_wss::exchanges::binance::spot::{SpotWSClient, SpotWSClientBuilder};

#[tokio::main]
async fn main() {
    /*
    let ws_builder = SpotWSClientBuilder::default();
    ws_builder::sub_trade("ETHUSDT");
    ws_builder::sub_ob_depth("BTCUSDT");

    let mut ws_client = ws_builder
        .build()
        .await
        .expect("failed to get spot wss client");

    ws_client.run();

    ws_client.sub_trade("SOLUSDT");
    ws_client.unsub_trade("ETHUSDT");
    ws_client.unsub_ob_depth("BTCUSDT");
    */
}
