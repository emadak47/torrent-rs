use async_wss::binance::{DepthSnapshot, Manager as BinanceManager, Message, RequestError};
use async_wss::utils::Exchange;
use async_wss::websocket::WebSocketClient;

#[tokio::main]
async fn main() {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "api=info");
    }
    dotenv::dotenv().ok();

    let products = vec!["BTC-USDT".to_string() /* "ETH-USDT".to_string() */];
    let cb_obj = BinanceManager::new();
    let mut wss = WebSocketClient::new();

    let socket_reader = wss.connect(Exchange::BINANCE).await.unwrap();

    let listener = wss
        .depth_subscribe::<BinanceManager, RequestError, Message, DepthSnapshot>(
            socket_reader,
            products,
            cb_obj,
        )
        .await
        .unwrap();

    listener.await.unwrap();
}
