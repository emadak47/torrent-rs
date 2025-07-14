use crate::binance::{Binance, Channel, Spot, API};
use crate::bybit::Bybit;
use crate::coinbase::Coinbase;
use crate::okx::Okx;
use crate::rest::RestClient;
use crate::utils::{Exchange, Result, Symbol, TorrentError};
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::env;
use std::fmt::{Debug, Display};
use std::marker;
use tokio::{net::TcpStream, sync::mpsc};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self},
    MaybeTlsStream, WebSocketStream,
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type SocketReader = SplitStream<Socket>;
type SocketWriter = SplitSink<Socket, tungstenite::Message>;

pub trait MessageCallback<T> {
    fn message_callback(&mut self, msg: Result<T>) -> Result<()>;
}

pub trait Wss: Display {
    fn subscribe(&mut self, channel: String, topics: Vec<String>) -> Result<String>;
    fn to_enum(&self) -> Exchange;
    fn ping_routine(&self) -> Option<PingRoutine> {
        None
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
            Exchange::BINANCE => Binance::URL,
            Exchange::BYBIT => Bybit::URL,
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
                let key_name = env::var("KEY_NAME").unwrap_or_default();
                let private_key = env::var("PRIVATE_KEY").unwrap_or_default();
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
            Exchange::BINANCE => {
                let binance = Binance::new();
                self.exchange = Some(Box::new(binance));
            }
            Exchange::BYBIT => {
                let bybit = Bybit::new();
                self.exchange = Some(Box::new(bybit));
            }
        };

        /*
        if let Some(exchange) = self.exchange {
            if let Some(ping_routine) = exchange.ping_routine() {
                let socket_w = self
                    .socket_w
                    .expect("connected exchange must have writer socket");

                tokio::spawn(schedule_pings(ping_routine));
            }
        }
        */
        Ok(reader)
    }

    pub async fn subscribe(&mut self, channel: String, topics: Vec<String>) -> Result<()> {
        match &mut self.exchange {
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
        T: DeserializeOwned,
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

    pub async fn depth_subscribe<M, E, T, Snapshot>(
        &mut self,
        reader: SocketReader,
        topics: Vec<String>,
        callback_manager: M,
    ) -> Result<tokio::task::JoinHandle<()>>
    where
        M: DepthCallback<T, Snapshot> + Send + 'static,
        E: Display + DeserializeOwned + 'static,
        T: Debug + DeserializeOwned + 'static + marker::Send,
        Snapshot: Debug + DeserializeOwned + 'static + marker::Send,
    {
        let listener = match &mut self.exchange {
            Some(ex) => match ex.to_enum() {
                Exchange::BINANCE => {
                    let channel = Channel::DEPTH.to_string();
                    let endpoint = API::Spot(Spot::Depth);
                    match self.subscribe(channel, topics.clone()).await {
                        Ok(_) => (),
                        Err(err) => return Err(err),
                    }
                    let mut params: HashMap<Symbol, [(String, String); 2]> = HashMap::new();
                    for topic in topics {
                        let topic = topic.split('-').collect::<Vec<&str>>();
                        if topic.len() != 2 {
                            eprintln!("{} does't conform to X-Y format", topic.join(""));
                            continue;
                        }
                        let topic = topic.join("");
                        let param = [
                            ("symbol".to_string(), topic.clone()),
                            ("limit".to_string(), "5000".to_string()),
                        ];
                        params.insert(topic, param);
                    }
                    tokio::spawn(DepthManager::<M, Snapshot, T>::request_snapshot::<
                        [(String, String); 2],
                        E,
                    >(
                        reader, endpoint, params, callback_manager
                    ))
                }
                _ => {
                    return Err(TorrentError::BadRequest(format!(
                        "Use `subscribe` method for depth orderbook subscription to {}",
                        ex
                    )))
                }
            },
            None => {
                return Err(TorrentError::BadParse(
                    "Not connected to exchange".to_string(),
                ))
            }
        };
        Ok(listener)
    }
}

pub trait DepthCallback<T, Snapshot> {
    const REST_URL: &'static str;

    fn depth_callback(&mut self, msg: Result<T>, snapshots_mp: Option<HashMap<Symbol, Snapshot>>);
}

pub struct DepthManager<M, Snapshot, T>
where
    M: DepthCallback<T, Snapshot>,
    Snapshot: DeserializeOwned,
{
    user_manager: M,
    snapshots_mp: Option<HashMap<Symbol, Snapshot>>,
    _marker: marker::PhantomData<T>,
}

impl<M, Snapshot, T> DepthManager<M, Snapshot, T>
where
    M: DepthCallback<T, Snapshot>,
    Snapshot: DeserializeOwned + Debug,
{
    pub async fn request_snapshot<S, E>(
        reader: SocketReader,
        endpoint: impl Into<String>,
        params: HashMap<Symbol, S>,
        callback_manager: M,
    ) where
        M: DepthCallback<T, Snapshot>,
        T: Debug + DeserializeOwned,
        S: Debug + Serialize,
        E: Display + DeserializeOwned,
    {
        let rest_client = RestClient::new(M::REST_URL);
        let endpoint = endpoint.into();
        let mut snapshots_mp: HashMap<Symbol, Snapshot> = HashMap::new();
        for (symbol, param) in params {
            let depth_snapshot = match rest_client
                .get::<Snapshot, E, S>(&endpoint, Some(param))
                .await
            {
                Ok(depth_snapshot) => depth_snapshot,
                Err(e) => {
                    eprintln!("{}", TorrentError::BadRequest(e.to_string()));
                    continue;
                }
            };
            snapshots_mp.insert(symbol, depth_snapshot);
        }
        if snapshots_mp.is_empty() {
            eprintln!("All snapshot requests were unsuccessful");
            return;
        }
        let manager = Self {
            user_manager: callback_manager,
            snapshots_mp: Some(snapshots_mp),
            _marker: marker::PhantomData,
        };
        WebSocketClient::listen_with(reader, manager).await;
    }
}

impl<M, Snapshot, T> MessageCallback<T> for DepthManager<M, Snapshot, T>
where
    M: DepthCallback<T, Snapshot>,
    Snapshot: DeserializeOwned,
    T: Debug,
{
    fn message_callback(&mut self, msg: Result<T>) -> Result<()> {
        if self.snapshots_mp.is_some() {
            let snapshots_mp = self.snapshots_mp.take();
            self.user_manager.depth_callback(msg, snapshots_mp);
            return Ok(());
        }
        self.user_manager.depth_callback(msg, None);
        Ok(())
    }
}

pub struct PingRoutine {
    interval: tokio::time::Interval,
    ping_fn: fn() -> tungstenite::Message,
}

async fn distribute_msgs(
    mut socket_w: SocketWriter,
    mut socket_w_rx: mpsc::UnboundedReceiver<tungstenite::Message>,
) {
    while let Some(msg) = socket_w_rx.recv().await {
        if let Err(e) = socket_w.send(msg).await {
            if matches!(
                e,
                tungstenite::Error::ConnectionClosed
                    | tungstenite::Error::AlreadyClosed
                    | tungstenite::Error::Io(_)
                    | tungstenite::Error::Protocol(
                        tungstenite::error::ProtocolError::SendAfterClosing
                    )
            ) {
                break;
            }
            eprintln!("failed to write to exchange wss");
        }
    }
}

async fn schedule_pings(
    socket_w_tx: mpsc::UnboundedSender<tungstenite::Message>,
    PingRoutine {
        mut interval,
        ping_fn,
    }: PingRoutine,
) {
    loop {
        interval.tick().await;
        let payload = ping_fn();
        if socket_w_tx.send(payload).is_err() {
            break;
        }
    }
}
