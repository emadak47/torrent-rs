use super::types::Event;
use crate::exchanges::Config;
use failure::{Error, ResultExt};
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde::Serialize;
use std::{collections::HashMap, sync::Arc};
use tokio::{net::TcpStream, sync::Mutex};
use tokio_tungstenite::{tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream};

#[derive(Debug, Serialize)]
struct Sub<'a> {
    op: Methods,
    args: Option<&'a Vec<HashMap<String, String>>>,
}

impl<'a> Sub<'a> {
    pub(crate) fn new(op: Methods, args: Option<&'a Vec<HashMap<String, String>>>) -> Self {
        Self { op, args }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Methods {
    Subscribe,
    Unsubscribe,
}

pub struct FuturesWSClientBuilder {
    url: String,
    topics: Vec<HashMap<String, String>>,
}
impl Default for FuturesWSClientBuilder {
    fn default() -> Self {
        let config = Config::okx();
        Self {
            url: config.spot_ws_endpoint,
            topics: Vec::new(),
        }
    }
}

impl FuturesWSClientBuilder {
    pub fn sub_trade(&mut self, symb: impl Into<String>) {
        let channel = ("channel".to_string(), "trades".to_string());
        let inst_id = ("instId".to_string(), symb.into());
        let inst_type = ("inst_type".to_string(), "FUTURES".to_string());
        let param = HashMap::from([channel, inst_type, inst_id]);
        self.topics.push(param);
    }

    pub fn sub_ob_depth(&mut self, symb: impl Into<String>) {
        let channel = ("channel".to_string(), "books".to_string());
        let inst_id = ("instId".to_string(), symb.into());
        let inst_type = ("inst_type".to_string(), "FUTURES".to_string());
        let param = HashMap::from([channel, inst_type, inst_id]);
        self.topics.push(param);
    }

    pub async fn build(self) -> Result<FuturesWSClient, Error> {
        Ok(FuturesWSClient {
            url: self.url,
            topics: self.topics,
            write: None,
        })
    }
}

type SharedWSS = Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>;
pub struct FuturesWSClient {
    url: String,
    topics: Vec<HashMap<String, String>>,
    write: Option<SharedWSS>,
}

impl FuturesWSClient {
    pub async fn connect(
        self,
        tx: tokio::sync::mpsc::UnboundedSender<Event>,
    ) -> Result<Self, Error> {
        let (stream, _) = tokio_tungstenite::connect_async(&self.url)
            .await
            .context("failed to connect to okx wss")?;

        let (mut write, read) = stream.split();

        if !self.topics.is_empty() {
            let sub = Sub::new(Methods::Subscribe, Some(&self.topics));
            let sub_req = serde_json::to_string(&sub).context("failed to serialise sub request")?;
            write
                .send(Message::Text(sub_req.to_string()))
                .await
                .context("failed to send init sub request to stream")?;
        }
        let write = Arc::new(Mutex::new(write));

        tokio::spawn(FuturesWSClient::run(Arc::clone(&write), read, tx));

        Ok(Self {
            url: self.url,
            topics: self.topics,
            write: Some(write),
        })
    }

    pub async fn sub_trade(&mut self, symb: impl Into<String>) -> Result<(), Error> {
        let channel = ("channel".to_string(), "trades".to_string());
        let inst_id = ("instId".to_string(), symb.into());
        let inst_type = ("inst_type".to_string(), "FUTURES".to_string());
        let param = HashMap::from([channel, inst_id, inst_type]);
        self.subscribe(param).await
    }

    pub async fn sub_ob_depth(&mut self, symb: impl Into<String>) -> Result<(), Error> {
        let channel = ("channel".to_string(), "books".to_string());
        let inst_id = ("instId".to_string(), symb.into());
        let inst_type = ("inst_type".to_string(), "FUTURES".to_string());
        let param = HashMap::from([channel, inst_type, inst_id]);
        self.subscribe(param).await
    }

    pub async fn unsub_trade(&mut self, symb: impl Into<String>) -> Result<(), Error> {
        let channel = ("channel".to_string(), "trades".to_string());
        let inst_id = ("instId".to_string(), symb.into());
        let inst_type = ("inst_type".to_string(), "FUTURES".to_string());
        let param = HashMap::from([channel, inst_id, inst_type]);
        self.unsubscribe(param).await
    }

    pub async fn unsub_ob_depth(&mut self, symb: impl Into<String>) -> Result<(), Error> {
        let channel = ("channel".to_string(), "books".to_string());
        let inst_id = ("instId".to_string(), symb.into());
        let inst_type = ("inst_type".to_string(), "FUTURES".to_string());
        let param = HashMap::from([channel, inst_type, inst_id]);
        self.unsubscribe(param).await
    }

    pub async fn list_subs(&self) -> &Vec<HashMap<String, String>> {
        &self.topics
    }

    async fn subscribe(&mut self, param: HashMap<String, String>) -> Result<(), Error> {
        if self.topics.contains(&param) {
            return Ok(());
        }

        let topic = &vec![param.clone()];
        let sub = Sub::new(Methods::Subscribe, Some(topic));
        let sub_req = serde_json::to_string(&sub).context("failed to serialise sub request")?;

        let write = self.write.as_ref().unwrap();
        let mut write = write.lock().await;
        write
            .send(Message::Text(sub_req))
            .await
            .context("failed to send sub request to stream")?;

        self.topics.push(param);

        Ok(())
    }

    async fn unsubscribe(&mut self, param: HashMap<String, String>) -> Result<(), Error> {
        if !self.topics.contains(&param) {
            return Ok(());
        }

        let topic = &vec![param.clone()];
        let unsub = Sub::new(Methods::Unsubscribe, Some(topic));
        let unsub_req = serde_json::to_string(&unsub).context("failed to serialise sub request")?;

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
                                Event::InitResponse(i) => {
                                    if i.event == "error" {
                                        let code = i.code.unwrap_or("unknown".to_string());
                                        let msg = i.msg.unwrap_or("unknown".to_string());

                                        log::warn!("subscription failed w/ msg: {}", msg);
                                        if msg.contains("Invalid request") {
                                            panic!("Invalid request to Okx with code: {}", code);
                                        }
                                        // TODO: other types of error?
                                        continue;
                                    }

                                    if i.event == "subscribe" {
                                        continue;
                                    }
                                }
                                Event::DepthOrderBook(d) => {
                                    if d.action == "snapshot" || d.action == "update" {
                                        if let Err(e) = tx.send(Event::DepthOrderBook(d)) {
                                            log::error!("Error sending depth ob event through tokio channel \n {:#?}", e);
                                        }
                                    }
                                }
                                Event::PublicTrade(t) => {
                                    println!("{:?}", t);
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
