use futures_util::{
    stream::{SplitSink, SplitStream},
    StreamExt,
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self},
    MaybeTlsStream, WebSocketStream,
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type SocketReader = SplitStream<Socket>;
type SocketWriter = SplitSink<Socket, tungstenite::Message>;
type Result<T> = std::result::Result<T, TorrentError>;
type Callback<T> = fn(Result<T>);

pub trait MessageCallback<T> {
    fn message_callback(&mut self, msg: Result<T>);
}

#[allow(non_camel_case_types)]
#[derive(Debug)]
pub enum Exchange {
    OKX,
    COINBASE,
}

impl std::fmt::Display for Exchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Exchange::COINBASE => write!(f, "Coinbase"),
            Exchange::OKX => write!(f, "Okx"),
        }
    }
}

#[derive(Debug)]
pub enum TorrentError {
    BadConnection(String),
    BadParse(String),
}

impl std::fmt::Display for TorrentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TorrentError::BadConnection(v) => write!(f, "connection error: {}", v),
            TorrentError::BadParse(v) => write!(f, "prasing error: {}", v),
        }
    }
}

pub trait Wss: std::fmt::Display {
    fn subscribe(&self, channel: String, topics: Vec<String>) -> Result<()>;
}

#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, Debug)]
pub enum CoinbaseChannel {
    STATUS,
    LEVEL2,
    HEARTBEATS,
}

struct Coinbase {
    api_key: String,
    private_key: String,
}

impl std::fmt::Display for Coinbase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Coinbase")
    }
}

impl Coinbase {
    const URL: &'static str = "wss://advanced-trade-ws.coinbase.com";

    pub fn new(api_key: String, private_key: String) -> Self {
        Self {
            api_key,
            private_key,
        }
    }

    pub fn sign_ws(
        &self,
        timestamp: &str,
        channel: &str,
        product_ids: Vec<String>,
    ) -> Result<String> {
        let prehash = format!("{}{}{}", timestamp, channel, product_ids.join(","));
        let prehash_bytes = prehash.as_bytes();
        let private_key_bytes = self.private_key.as_bytes();
        let mut mac = match Hmac::<sha2::Sha256>::new_from_slice(private_key_bytes) {
            Ok(mac) => mac,
            Err(e) => return Err(TorrentError::BadParse(format!("Coinbase signature: {}", e))),
        };
        mac.update(prehash_bytes);
        let signature_bytes = mac.finalize().into_bytes();
        Ok(hex::encode(signature_bytes))
    }
}

impl Wss for Coinbase {
    fn subscribe(&self, channel: String, topics: Vec<String>) -> Result<()> {
        let timestamp = now().to_string();
        let signature = self.sign_ws(timestamp.as_str(), channel.as_str(), topics)?;
        dbg!(signature);
        Ok(())
    }
}

struct Okx {}

impl Okx {
    const URL: &'static str = "wss://ws.okx.com:8443/ws/v5/public";

    pub fn new() -> Self {
        Self {}
    }
}

impl std::fmt::Display for Okx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Okx")
    }
}

impl Wss for Okx {
    fn subscribe(&self, _channel: String, _topics: Vec<String>) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct WebSocketClient {
    exchange: Option<Box<dyn Wss>>,
    socket_w: Option<SocketWriter>,
}

impl WebSocketClient {
    pub fn new() -> Self {
        Self {
            exchange: None,
            socket_w: None,
        }
    }

    pub async fn connect(&mut self, exchange: Exchange) -> Result<SocketReader> {
        if self.exchange.is_some() && self.socket_w.is_some() {
            return Err(TorrentError::BadConnection(format!(
                "Already connected to: {}",
                exchange
            )));
        }

        let url = match exchange {
            Exchange::COINBASE => Coinbase::URL,
            Exchange::OKX => Okx::URL,
        };

        let reader = match connect_async(url).await {
            Ok((socket, _)) => {
                let (writer, reader) = socket.split();
                self.socket_w = Some(writer);
                reader
            }
            Err(e) => {
                return Err(TorrentError::BadConnection(format!(
                    "Unable to handshake: {}",
                    e
                )))
            }
        };

        match exchange {
            Exchange::COINBASE => {
                let coinbase = Coinbase::new("k".to_string(), "p".to_string());
                self.exchange = Some(Box::new(coinbase));
            }
            Exchange::OKX => {
                let okx = Okx::new();
                self.exchange = Some(Box::new(okx));
            }
        };

        Ok(reader)
    }

    async fn subscribe(&self, channel: String, topics: Vec<String>) -> Result<()> {
        match &self.exchange {
            Some(ex) => match &self.socket_w {
                Some(_socket) => {
                    ex.subscribe(channel, topics)?;
                    Ok(())
                }
                None => Err(TorrentError::BadParse(format!(
                    "Not connected to {} socket",
                    ex
                ))),
            },
            None => Err(TorrentError::BadParse(
                "Not connected to exchange".to_string(),
            )),
        }
    }
}

#[macro_export]
macro_rules! dbg {
    ($fmt:expr $(, $($arg:tt)*)?) => {
        println!(concat!("[DEBUG] ", $fmt), $($($arg)*)?);
    };
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backword")
        .as_secs()
}
