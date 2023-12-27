use super::types::{Event, Methods, Subscription};
use crate::exchanges::{binance::types::FuturesDepthOrderBookEvent, Config};
use failure::{Error, ResultExt};
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use log;
use std::sync::Arc;
use tokio::{net::TcpStream, sync::Mutex};
use tokio_tungstenite::{tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream};

pub struct FuturesWSClientBuilder {
    url: String,
    topics: Vec<String>,
}

impl Default for FuturesWSClientBuilder {
    fn default() -> Self {
        let config = Config::binance();
        Self {
            url: config.futures_ws_endpoint,
            topics: Vec::new(),
        }
    }
}

impl FuturesWSClientBuilder {
    pub fn sub_ob_depth(&mut self, symb: impl Into<String>) {
        let param = format!("{}@depth", symb.into()).to_lowercase();
        self.topics.push(param);
    }

    pub async fn build(self) -> Result<FuturesWSClient, Error> {
        Ok(FuturesWSClient {
            url: self.url,
            topics: self.topics,
            id: 0,
            write: None,
        })
    }
}

type SharedWSS = Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>;
pub struct FuturesWSClient {
    url: String,
    topics: Vec<String>,
    id: usize,
    write: Option<SharedWSS>,
}

impl FuturesWSClient {
    pub async fn connect(
        self,
        tx: tokio::sync::mpsc::UnboundedSender<Event>,
    ) -> Result<Self, Error> {
        let (stream, _) = tokio_tungstenite::connect_async(&self.url)
            .await
            .context("failed to connect to binance wss")?;

        let (mut write, read) = stream.split();

        if !self.topics.is_empty() {
            let sub = Subscription::new(Methods::Subscribe, Some(&self.topics), &0);
            let sub_req = serde_json::to_string(&sub).expect("failed to serialise sub request");

            write
                .send(Message::Text(sub_req))
                .await
                .expect("failed to send init sub request to stream");
        }
        let write = Arc::new(Mutex::new(write));

        tokio::spawn(FuturesWSClient::run(Arc::clone(&write), read, tx));

        Ok(Self {
            url: self.url,
            topics: self.topics,
            id: self.id + 1,
            write: Some(write),
        })
    }

    pub async fn sub_ob_depth(&mut self, symb: impl Into<String>) -> Result<(), Error> {
        let param = format!("{}@depth", symb.into()).to_lowercase();
        self.subscribe(param).await
    }

    pub async fn unsub_ob_depth(&mut self, symb: impl Into<String>) -> Result<(), Error> {
        let param = format!("{}@depth", symb.into()).to_lowercase();
        self.unsubscribe(param).await
    }

    pub async fn list_subs(&self) -> &Vec<String> {
        &self.topics
    }

    async fn subscribe(&mut self, param: impl Into<String>) -> Result<(), Error> {
        let param = param.into();
        if self.topics.contains(&param) {
            return Ok(());
        }

        let topic = &vec![param.clone()];
        let sub = Subscription::new(Methods::Subscribe, Some(topic), &self.id);
        let sub_req = serde_json::to_string(&sub).expect("failed to serialise sub request");

        let write = self.write.as_ref().unwrap();
        let mut write = write.lock().await;
        write
            .send(Message::Text(sub_req))
            .await
            .context("failed to send sub request to stream")?;

        self.topics.push(param);
        self.id += 1;

        Ok(())
    }

    async fn unsubscribe(&mut self, param: impl Into<String>) -> Result<(), Error> {
        let param = param.into();
        if !self.topics.contains(&param) {
            return Ok(());
        }

        let topic = &vec![param.clone()];
        let unsub = Subscription::new(Methods::Unsubscribe, Some(topic), &self.id);
        let unsub_req = serde_json::to_string(&unsub).expect("failed to serialise sub request");

        let write = self.write.as_ref().unwrap();
        let mut write = write.lock().await;
        write
            .send(Message::Text(unsub_req))
            .await
            .context("failed to send sub request to stream")?;

        self.topics.retain(|val| *val != param);
        self.id += 1;

        Ok(())
    }

    async fn run(
        write: SharedWSS,
        mut read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        tx: tokio::sync::mpsc::UnboundedSender<Event>,
    ) {
        loop {
            match read.next().await {
                Some(res) => match res {
                    Ok(msg) => match msg {
                        Message::Text(msg) => {
                            let mut value: serde_json::Value = match serde_json::from_str(&msg) {
                                Ok(v) => v,
                                Err(e) => {
                                    log::error!("Failed to serialise binance msg\n{:?}", e);
                                    continue;
                                }
                            };

                            if value.get("result").is_some() && value.get("id").is_some() {
                                continue;
                            }

                            if value.get("data").is_some() {
                                let data = value["data"].take();
                                if data.get("pu").is_some() {
                                    let d: FuturesDepthOrderBookEvent =
                                        match serde_json::from_value(data) {
                                            Ok(d) => d,
                                            Err(e) => {
                                                log::error!(
                                                    "Failed to serialise binance event\n{:?}",
                                                    e
                                                );
                                                continue;
                                            }
                                        };

                                    if let Err(e) = tx.send(Event::FuturesDepthOrderBook(d)) {
                                        log::error!("Error sending depth ob event through tokio channel \n {:#?}", e);
                                    }
                                } else {
                                    continue;
                                }
                            } else {
                                log::warn!("binance stream didn't send data payload");
                                continue;
                            }
                        }
                        Message::Ping(_) => {
                            let mut write = write.lock().await;
                            if let Err(e) = write.send(Message::Pong(vec![])).await {
                                log::error!(
                                    "Error sending Pong in response to Ping. Err Msg: \n {:#?}",
                                    e
                                );
                            }
                        }
                        Message::Binary(_) => {}
                        Message::Pong(_) => {}
                        Message::Close(msg) => {
                            log::warn!("Websocket conn closed. Err msg: \n {:#?}", msg);
                        }
                        Message::Frame(_) => {}
                    },
                    Err(e) => {
                        log::error!("Websocket Error: \n {:#?}", e);
                    }
                },
                None => {
                    log::debug!("'None` read from Webscoket stream");
                }
            }
        }
    }
}
