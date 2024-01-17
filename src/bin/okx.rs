use async_wss::okx::{Channel as OkxChannel, Manager as OkxManager};
use async_wss::utils::Exchange;
use async_wss::websocket::WebSocketClient;

#[tokio::main]
async fn main() {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "api=info");
    }
    dotenv::dotenv().ok();

    let products = vec!["BTC-USDT".to_string() /* "ETH-USDT".to_string() */];
    let manager = OkxManager::new();
    let mut wss = WebSocketClient::new();

    let socket_reader = wss.connect(Exchange::OKX).await.unwrap();
    let listener = WebSocketClient::listen_with(socket_reader, manager);

    wss.subscribe(OkxChannel::BOOKS.to_string(), products)
        .await
        .unwrap();

    listener.await;
}
