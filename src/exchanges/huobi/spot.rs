use super::{super::config::Config, types::Event};
use failure::{Error, ResultExt};
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::{net::TcpStream, sync::Mutex};
use tokio_tungstenite::{tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize)]
struct Sub {
    sub: String,
    id: String,
}

impl Sub {
    pub(crate) fn new( sub: String) -> Self {
        Self { sub: sub, id: "atrimo".to_string() }
    }
}

#[derive(Debug, Serialize)]
struct UnSub {
    unsub: Vec<String>,
    id: String,
}

impl UnSub {
    pub(crate) fn new(unsub: &Vec<String>) -> Self {
        Self { unsub: unsub.to_vec(), id: "id1".to_string() }
    }
}

pub struct SpotWSClientBuilder {
    url: String,
    topics: Vec<String>,
}

impl Default for SpotWSClientBuilder {
    fn default() -> Self {
        let config = Config::huobi();
        Self {
            url: config.spot_ws_endpoint.into(),
            topics: Vec::new(),
        }
    }
}

impl SpotWSClientBuilder {
    pub fn sub_ob_depth(&mut self, depth: u8, symb: impl Into<String>) {
        let param = format!("market.{}.mbp.{}", symb.into().to_lowercase(), depth);
        let sub = Sub::new(param);
        let sub_req = serde_json::to_string(&sub).expect("failed to serialise sub request");
        self.topics.push(sub_req);
    }

    pub async fn build(self) -> Result<SpotWSClient, Error> {
        Ok(SpotWSClient {
            url: self.url,
            topics: self.topics,
            write: None,
        })
    }
}

pub struct SpotWSClient {
    url: String,
    topics: Vec<String>,
    write: Option<Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>>,
}

impl SpotWSClient {
    #[must_use]
    pub async fn connect(
        self,
        tx: tokio::sync::mpsc::UnboundedSender<Event>,
    ) -> Result<Self, Error> {
        println!("self.url {}", self.url);
        let (stream, _) = tokio_tungstenite::connect_async(&self.url)
            .await
            .context("failed to connect to huobi wss")?;

        let (mut write, read) = stream.split();

        if !self.topics.is_empty() {
            for topic in &self.topics {
                write
                    .send(Message::Text(topic.clone()))
                    .await
                    .expect("failed to send init sub request to stream");
            }
        }
        let write = Arc::new(Mutex::new(write));

        tokio::spawn(SpotWSClient::run(Arc::clone(&write), read, tx));

        Ok(Self {
            url: self.url,
            topics: self.topics,
            write: Some(write),
        })
    }

    pub async fn sub_ob_depth(&mut self, depth: u8, symb: impl Into<String>) -> Result<(), Error> {
        let param = format!("market.{}.mbp.{}", symb.into().to_lowercase(), depth);
        self.topics.push(param.clone());
        self.subscribe(param).await
    }

    pub async fn unsub_ob_depth(&mut self, depth: u8, symb: impl Into<String>) -> Result<(), Error> {
        let param = format!("orderbook.{}.{}", depth, symb.into().to_lowercase());
        self.topics.push(param.clone());
        self.unsubscribe(param).await
    }

    pub async fn list_subs(&self) -> &Vec<String> {
        &self.topics
    }

    async fn subscribe(&mut self, param: String) -> Result<(), Error> {
        if self.topics.contains(&param) {
            return Ok(());
        }

        let topic = param.clone();
        let sub = Sub::new(topic);
        let sub_req = serde_json::to_string(&sub).expect("failed to serialise sub request");

        let write = self.write.as_ref().unwrap();
        let mut write = write.lock().await;
        write
            .send(Message::Text(sub_req))
            .await
            .context("failed to send sub request to stream")?;

        self.topics.push(param);

        Ok(())
    }

    async fn unsubscribe(&mut self, param: String) -> Result<(), Error> {
        if !self.topics.contains(&param) {
            return Ok(());
        }

        let ref topic = vec![param.clone()];
        let unsub = UnSub::new(&topic);
        let unsub_req = serde_json::to_string(&unsub).expect("failed to serialise sub request");

        let write = self.write.as_ref().unwrap();
        let mut write = write.lock().await;
        write
            .send(Message::Text(unsub_req))
            .await
            .context("failed to send sub request to stream")?;

        self.topics.retain(|val| *val != param);

        Ok(())
    }

    pub async fn run(
        write: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
        mut read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        tx: tokio::sync::mpsc::UnboundedSender<Event>,
    ) {
        loop {
            match read.next().await {
                Some(res) => match res {
                    Ok(msg) => match msg {
                        Message::Text(msg) => {
                            //println!("message {}", msg);
                            let event = match serde_json::from_str(&msg)
                                .and_then(|v: serde_json::Value| serde_json::from_value::<Event>(v))
                                .map_err(|e| {
                                    log::error!("Failed to serialise okx event. \n {:#?}", e)
                                })
                                .ok()
                            {
                                None => continue,
                                Some(ev) => ev,
                            };
                            match event {
                                Event::OrderBook(d) => {
                                    println!("we got orderbook message");
                                }
                                Event::InitResponse(d) => {
                                    println!("init");
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
                    println!("none read from websocket");
                    log::debug!("'None` read from Webscoket stream");
                }
            }
        }
    }
}
