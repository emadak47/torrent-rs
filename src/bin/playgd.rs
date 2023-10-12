use std::process::exit;

use futures_util::{
    future,
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use tokio_tungstenite::{tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream};

use std::sync::Arc;
use tokio::{net::TcpStream, sync::Mutex};

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let start = std::time::Instant::now();

    let url = url::Url::parse("wss://stream.binance.com:9443/ws").unwrap();
    let topics = subscribe_trade()?;

    let (ws_stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("failed to connect");

    let (mut write, read) = ws_stream.split();

    let write_1 = Arc::new(Mutex::new(write));
    let write_clone = Arc::clone(&write_1);

    tokio::spawn(run_for_good(write_clone, read));

    println!("=========================================================== before first sleep");
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    if start.elapsed() > std::time::Duration::from_secs(5) {
        println!("=========================================================== inside if");

        let mut write_1 = write_1.lock().await;

        println!("=========================================================== asking for list");
        write_1
            .send(Message::Text(subscribe_list().unwrap()))
            .await
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        write_1
            .send(Message::Text(unsubscribe_trade().unwrap()))
            .await
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        write_1
            .send(Message::Text(subscribe_depth().unwrap()))
            .await
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        write_1
            .send(Message::Text(subscribe_list().unwrap()))
            .await
            .unwrap();
    }

    println!("=========================================================== before second sleep");
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    Ok(())
}

async fn run_for_good(
    write_clone: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
    mut read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
) {
    {
        let mut write_clone = write_clone.lock().await;

        println!(
            "=========================================================== before sub to trades"
        );
        write_clone
            .send(Message::Text(subscribe_trade().unwrap()))
            .await
            .expect("init subscription msgs should be sent to binance stream");
    }

    loop {
        let res = read.next().await;
        match res.unwrap() {
            Ok(msg) => match msg {
                Message::Text(msg) => {
                    println!("{}", msg);
                }
                Message::Ping(_) => {
                    let mut write_clone = write_clone.lock().await;
                    write_clone.send(Message::Pong(vec![])).await.expect("");
                }
                Message::Binary(_) => {}
                Message::Pong(_) => {}
                Message::Close(_) => {}
                Message::Frame(_) => {}
            },
            Err(e) => {
                println!("erorrrrr! {:?}", e);
                break;
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct List {
    pub method: String,
    pub id: usize,
}

fn subscribe_list() -> Result<String, serde_json::error::Error> {
    let sub = List {
        method: "LIST_SUBSCRIPTIONS".to_string(),
        id: 8,
    };

    let serialised_sub = serde_json::to_string(&sub).expect("failed to serialise Subscription");
    Ok(serialised_sub)
}

fn subscribe_depth() -> Result<String, serde_json::error::Error> {
    let topic = format!("btcusdt@depth");
    let sub = Subscription {
        method: "SUBSCRIBE".to_string(),
        params: vec![topic],
        id: 0,
    };

    let serialised_sub = serde_json::to_string(&sub).expect("failed to serialise Subscription");
    Ok(serialised_sub)
}

fn unsubscribe_trade() -> Result<String, serde_json::error::Error> {
    let topic = format!("ethusdt@trade");
    let sub = Subscription {
        method: "UNSUBSCRIBE".to_string(),
        params: vec![topic],
        id: 2,
    };

    let serialised_sub = serde_json::to_string(&sub).expect("failed to serialise Subscription");
    Ok(serialised_sub)
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
