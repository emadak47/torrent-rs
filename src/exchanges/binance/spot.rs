use super::{super::config::Config, types::Event};
use failure::{Error, ResultExt};
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use log;
use serde::Serialize;
use std::sync::Arc;
use tokio::{net::TcpStream, sync::Mutex};
use tokio_tungstenite::{tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream};

#[derive(Debug, Serialize)]
struct Sub<'a, 'b> {
    method: Methods,
    params: Option<&'a Vec<String>>,
    id: &'b usize,
}

impl<'a, 'b> Sub<'a, 'b> {
    pub(crate) fn new(method: Methods, params: Option<&'a Vec<String>>, id: &'b usize) -> Self {
        Self { method, params, id }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Methods {
    Subscribe,
    Unsubscribe,
}

pub struct SpotWSClientBuilder {
    url: String,
    topics: Vec<String>,
}

impl Default for SpotWSClientBuilder {
    fn default() -> Self {
        let config = Config::binance();
        Self {
            url: config.spot_ws_endpoint.into(),
            topics: Vec::new(),
        }
    }
}

impl SpotWSClientBuilder {
    pub fn sub_trade(&mut self, symb: impl Into<String>) {
        let param = format!("{}@trade", symb.into()).to_lowercase();
        self.topics.push(param);
    }

    pub fn sub_ob_depth(&mut self, symb: impl Into<String>) {
        let param = format!("{}@depth", symb.into()).to_lowercase();
        self.topics.push(param);
    }

    pub async fn build(self) -> Result<SpotWSClient, Error> {
        Ok(SpotWSClient {
            url: self.url,
            topics: self.topics,
            id: 0,
            write: None,
        })
    }
}

pub struct SpotWSClient {
    url: String,
    topics: Vec<String>,
    id: usize,
    write: Option<Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>>,
}

impl SpotWSClient {
    #[must_use]
    pub async fn connect(
        self,
        tx: tokio::sync::mpsc::UnboundedSender<Event>,
    ) -> Result<Self, Error> {
        let (stream, _) = tokio_tungstenite::connect_async(&self.url)
            .await
            .context("failed to connect to binance wss")?;

        let (mut write, read) = stream.split();

        if !self.topics.is_empty() {
            let sub = Sub::new(Methods::Subscribe, Some(&self.topics), &0);
            let sub_req = serde_json::to_string(&sub).expect("failed to serialise sub request");

            write
                .send(Message::Text(sub_req))
                .await
                .expect("failed to send init sub request to stream");
        }
        let write = Arc::new(Mutex::new(write));

        tokio::spawn(SpotWSClient::run(Arc::clone(&write), read, tx));

        Ok(Self {
            url: self.url,
            topics: self.topics,
            id: self.id + 1,
            write: Some(write),
        })
    }

    pub async fn sub_trade(&mut self, symb: impl Into<String>) -> Result<(), Error> {
        let param = format!("{}@trade", symb.into()).to_lowercase();
        self.subscribe(param).await
    }

    pub async fn sub_ob_depth(&mut self, symb: impl Into<String>) -> Result<(), Error> {
        let param = format!("{}@depth", symb.into()).to_lowercase();
        self.subscribe(param).await
    }

    pub async fn unsub_trade(&mut self, symb: impl Into<String>) -> Result<(), Error> {
        let param = format!("{}@trade", symb.into()).to_lowercase();
        self.unsubscribe(param).await
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

        let ref topic = vec![param.clone()];
        let sub = Sub::new(Methods::Subscribe, Some(topic), &self.id);
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

        let ref topic = vec![param.clone()];
        let unsub = Sub::new(Methods::Unsubscribe, Some(topic), &self.id);
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
        write: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
        mut read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        tx: tokio::sync::mpsc::UnboundedSender<Event>,
    ) {
        loop {
            match read.next().await {
                Some(res) => match res {
                    Ok(msg) => match msg {
                        Message::Text(msg) => {
                            let value: serde_json::Value = serde_json::from_str(&msg)
                                .expect("Failed to serialise msg received from binance WS");

                            if value.get("result").is_some() && value.get("id").is_some() {
                                continue;
                            }

                            let event: Event = serde_json::from_str(value.to_string().as_str())
                                .expect("Failed to serialise binance event");

                            match event {
                                Event::DepthOrderBook(d) => {
                                    if let Err(e) = tx.send(Event::DepthOrderBook(d)) {
                                        log::error!("Error sending depth ob event through tokio channel \n {:#?}", e);
                                    }
                                }
                                Event::PublicTrade(t) => {
                                    println!("{}-{}", t.event_type, t.symbol);
                                }
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
