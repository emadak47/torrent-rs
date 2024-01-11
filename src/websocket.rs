use crate::coinbase::Coinbase;
use crate::okx::Okx;
use crate::utils::{Exchange, Result, TorrentError};
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
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
    fn message_callback(&mut self, msg: Result<T>) -> Result<()>;
}

pub trait Wss: std::fmt::Display {
    fn subscribe(&self, channel: String, topics: Vec<String>) -> Result<String>;
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
                let key_name = std::env::var("KEY_NAME").unwrap_or_default();
                let private_key = std::env::var("PRIVATE_KEY").unwrap_or_default();
                if key_name.is_empty() || private_key.is_empty() {
                    panic!("Coinbase credentails uninitalised. See `.env.example`");
                }
                let coinbase = Coinbase::new(key_name, private_key, true);
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

    pub async fn listen_with<T, M>(reader: SocketReader, callback_manager: M)
    where
        M: MessageCallback<T>,
        T: serde::de::DeserializeOwned,
    {
        let mut manager: M = callback_manager;

        let read_fut = reader.for_each(|m| {
            let data: String = match m {
                Ok(v) => v.to_string(),
                Err(e) => format!("websocket sent the error: {}", e),
            };

            let _ = match serde_json::from_str(&data) {
                Ok(msg) => manager.message_callback(Ok(msg)),
                Err(e) => manager.message_callback(Err(TorrentError::BadParse(format!(
                    "Unable to parse msg because {} {}",
                    e, data
                )))),
            };

            async {}
        });

        read_fut.await
    }
}
