use async_wss::bullish::{Channel as BullishChannel, Manager as BullishManager};
use async_wss::utils::Exchange;
use async_wss::websocket::WebSocketClient;

#[tokio::main]
async fn main() {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "api=info");
    }

    dotenv::dotenv().ok();

    let products = vec!["BTC-USDC-PERP".to_string() /* "ETH-USDT".to_string() */];
    let manager = BullishManager::new();
    let mut wss = WebSocketClient::new();

    let socket_reader = wss.connect(Exchange::BULLISH).await.unwrap();
    let listener = WebSocketClient::listen_with(socket_reader, manager);

    wss.subscribe(BullishChannel::l2Orderbook.to_string(), products)
        .await
        .unwrap();

    listener.await;
}
