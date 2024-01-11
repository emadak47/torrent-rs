use crate::utils::{from_str, now, Result, TorrentError};
use core::fmt;
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self},
    MaybeTlsStream, WebSocketStream,
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type SocketReader = SplitStream<Socket>;
type SocketWriter = SplitSink<Socket, tungstenite::Message>;
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

pub trait Wss: std::fmt::Display {
    fn subscribe(&self, channel: String, topics: Vec<String>) -> Result<String>;
}

#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, Debug)]
pub enum CoinbaseChannel {
    STATUS,
    LEVEL2,
    HEARTBEATS,
}

impl fmt::Display for CoinbaseChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CoinbaseChannel::HEARTBEATS => write!(f, "heartbeats"),
            CoinbaseChannel::LEVEL2 => write!(f, "level2"),
            CoinbaseChannel::STATUS => write!(f, "status"),
        }
    }
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
    fn subscribe(&self, channel: String, topics: Vec<String>) -> Result<String> {
        let timestamp = now().to_string();
        let _signature = self.sign_ws(timestamp.as_str(), channel.as_str(), topics.clone())?;
        Ok(String::from("Hello from Coinbase"))
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
    fn subscribe(&self, _channel: String, _topics: Vec<String>) -> Result<String> {
        Ok(String::from("Hello from Okx"))
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

    pub async fn subscribe(&mut self, channel: String, topics: Vec<String>) -> Result<()> {
        match &self.exchange {
            Some(ex) => match &mut self.socket_w {
                Some(ref mut socket) => {
                    let sub_req = ex.subscribe(channel, topics)?;
                    match socket.send(tungstenite::Message::text(sub_req)).await {
                        Ok(_) => Ok(()),
                        Err(e) => Err(TorrentError::BadConnection(format!(
                            "couldn't write to socket: {}",
                            e
                        ))),
                    }
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
