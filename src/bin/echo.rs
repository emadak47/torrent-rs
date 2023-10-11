use futures_util::{future, StreamExt, SinkExt};
use tokio_tungstenite::tungstenite::protocol::Message;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let url = url::Url::parse("wss://stream.binance.com:9443/ws").unwrap();
    let topics = subscribe_trade()?;

    let (mut stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("failed to connect");

    let (mut write, read) = stream.split();

    Ok(())
}

fn subscribe_trade() -> Result<String, serde_json::error::Error> {
    let topic = format!("ethusdt@trade");

    let sub = Subscription {
        method: "SUBSCRIBE".to_string(),
        params: vec![topic],
        id: 1,
    };

    let serialised_sub = serde_json::to_string(&sub).expect("failed to serialise Subscription");
    Ok(serialised_sub)
}

#[derive(serde::Serialize)]
struct Subscription {
    method: String,
    params: Vec<String>,
    id: usize,
}
