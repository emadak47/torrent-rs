use super::{super::config::Config, types::Event};
use failure::{Error, ResultExt};
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde::Serialize;
use serde::Deserialize;

use std::collections::HashMap;
use crate::exchanges::util::get_time;
use std::sync::Arc;
use tokio::{net::TcpStream, sync::Mutex};
use tokio_tungstenite::{tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream};

#[derive(Debug, Serialize)]
pub struct Sub {
    time: u128,
    channel: String,
    event: Methods,
    payload: Vec<String>, // [symbol, interval]
}

impl Sub {
    pub(crate) fn new(
        method: Methods, 
        channel: &str, 
        payload: Vec<String>) -> Self {
        Self { time: get_time(), channel: channel.to_string(), event: method, payload: payload}
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
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
        let config = Config::gate();
        Self {
            url: config.spot_ws_endpoint.into(),
            topics: Vec::new(),
        }
    }
}

impl SpotWSClientBuilder {
    pub fn sub_ob_depth(&mut self, symb: &str) {

        let mut payload = vec!(String::from(symb));
        payload.push(String::from("100ms"));

        let sub = Sub::new(Methods::Subscribe, 
                  "spot.order_book_update", 
                           payload);

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
        let (stream, _) = tokio_tungstenite::connect_async(&self.url)
            .await
            .context("failed to connect to gate wss")?;

        let (mut write, read) = stream.split();

        if !self.topics.is_empty() {
            for topic in &self.topics {
                write
                    .send(Message::Text(topic.to_string()))
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

    pub async fn sub_ob_depth(&mut self, symb: &str) -> Result<(), Error> {
        let mut payload = vec!(String::from(symb));
        payload.push(String::from("100ms"));

        let sub = Sub::new(Methods::Subscribe, 
                  "spot.order_book_update", 
                           payload);

        let sub_req = serde_json::to_string(&sub).expect("failed to serialise sub request");
        self.subscribe(sub_req).await
    }

    pub async fn unsub_ob_depth(&mut self, symb: &str) -> Result<(), Error> {
        let mut payload = vec!(String::from(symb));
        payload.push(String::from("100ms"));

        let sub = Sub::new(Methods::Unsubscribe, 
                  "spot.order_book_update", 
                           payload);

        let un_sub_req = serde_json::to_string(&sub).expect("failed to serialise sub request");
        self.unsubscribe(un_sub_req).await
    }

    pub async fn list_subs(&self) -> &Vec<String> {
        &self.topics
    }

    async fn subscribe(&mut self, sub: String) -> Result<(), Error> {
        if self.topics.contains(&sub) {
            return Ok(());
        }

        let write = self.write.as_ref().unwrap();
        let mut write = write.lock().await;
        write
            .send(Message::Text(sub.to_string()))
            .await
            .context("failed to send sub request to stream")?;

        self.topics.push(sub.to_string());
        Ok(())
    }

    async fn unsubscribe(&mut self, unsub: String) -> Result<(), Error> {

        let write = self.write.as_ref().unwrap();
        let mut write = write.lock().await;
        write
            .send(Message::Text(unsub.to_string()))
            .await
            .context("failed to send sub request to stream")?;

        self.topics.retain(|val| *val != unsub.to_string());
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
                            let event = match serde_json::from_str(&msg)
                                .and_then(|v: serde_json::Value| serde_json::from_value::<Event>(v))
                                .map_err(|e| {
                                    log::error!("Failed to serialise gate event. \n {:#?}", e)
                                })
                                .ok()
                            {
                                None => continue,
                                Some(ev) => ev,
                            };
                            match event {
                                Event::Level2(d) => {
                                    if let Err(e) = tx.send(Event::Level2(d)) {
                                        log::error!("Error sending depth ob event through tokio channel \n {:#?}", e);
                                    }
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
