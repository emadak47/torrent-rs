use async_wss::bybit::{Channel as BybitChannel, Manager as BybitManager};
use async_wss::utils::Exchange;
use async_wss::websocket::WebSocketClient;

#[tokio::main]
async fn main() {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "api=info");
    }
    dotenv::dotenv().ok();

    let products = vec!["BTC-USDT".to_string() /* "ETH-USDT".to_string() */];
    let manager = BybitManager::new();
    let mut wss = WebSocketClient::new();

    let socket_reader = wss.connect(Exchange::BYBIT).await.unwrap();
    let listener = WebSocketClient::listen_with(socket_reader, manager);

    wss.subscribe(BybitChannel::Orderbook(50,"BTCUSDT".to_string()).to_string(), products)
        .await
        .unwrap();

    listener.await;
}
