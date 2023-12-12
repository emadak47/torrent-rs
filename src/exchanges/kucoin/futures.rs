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

use crate::exchanges::kucoin::api::API;
use crate::exchanges::kucoin::api::Spot;
use crate::exchanges::kucoin::client::Client;
use crate::exchanges::kucoin::types::APIDatum;
use crate::exchanges::kucoin::utils::get_time;
use crate::exchanges::kucoin::types::InstanceServers;

#[derive(Debug, Serialize)]
struct Sub {
    id: u128,
    #[serde(rename = "type")]
    Type: Methods,
    topic: String, 
    response: bool
}

impl Sub {
    pub(crate) fn new(op: Methods, args: String) -> Self {
        let timestamp = get_time();
        Self { id: timestamp, Type: op, topic: args, response: true}
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
    topics: Vec<String>,
}

impl Default for FuturesWSClientBuilder {
    fn default() -> Self {
        let config = Config::kucoin();
        Self {
            url: config.futures_ws_endpoint.into(),
            topics: Vec::new(),
        }
    }
}

impl FuturesWSClientBuilder {
    pub fn sub_ob_depth(&mut self, symb: impl Into<String>) {
        let param = format!("/market/level2:{}", symb.into().to_uppercase());
        self.topics.push(param);
    }

    pub async fn build(self) -> Result<FuturesWSClient, Error> {
        let config = Config::kucoin();
        Ok(FuturesWSClient {
            url: self.url,
            topics: self.topics,
            client: Client::new("api_key".to_string(), "secret_key".to_string(), config.spot_rest_api_endpoint),
            write: None,
        })
    }
}

pub struct FuturesWSClient {
    url: String,
    topics: Vec<String>,
    client: Client,
    write: Option<Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>>,
}

impl FuturesWSClient {
    #[must_use]
    pub async fn connect(
        self,
        tx: tokio::sync::mpsc::UnboundedSender<Event>,
    ) -> Result<Self, Error> {
        let url = self.get_ws_public_socket().await;
        let url_str = url.unwrap().clone();
        let (stream, _) = tokio_tungstenite::connect_async(&url_str)
            .await
            .context("failed to connect to okx wss")?;
        let (mut write, read) = stream.split();
        if !self.topics.is_empty() {
            for topic in &self.topics {
                let sub = Sub::new(Methods::Subscribe, topic.clone());
                let sub_req = serde_json::to_string(&sub).expect("failed to serialise sub request");
                println!("sub req {}", sub_req);
                write
                    .send(Message::Text(sub_req))
                    .await
                    .expect("failed to send init sub request to stream");
            }
        }
        let write = Arc::new(Mutex::new(write));

        tokio::spawn(FuturesWSClient::run(Arc::clone(&write), read, tx));

        Ok(Self {
            url: self.url,
            topics: self.topics,
            client: Client::new("api_key".to_string(), "secret_key".to_string(), "host".to_string()),
            write: Some(write),
        })
    }
    
    pub async fn get_ws_public_socket(&self) -> Result<String, Error> {
        let mut endpoint: String = String::new();
        let mut token: String = String::new();
        let timestamp = get_time();
        let resp = self.client.post::<APIDatum<InstanceServers>>(API::Spot(Spot::BulletPublic), None).await.unwrap();
        if let Some(r) = &resp.data {
            token = r.token.to_owned();
            endpoint = r.instance_servers[0].endpoint.to_owned();
        }
        let url = format!(
            "{}?token={}&[connectId={}]",
            endpoint, token, timestamp
        );
        Ok(url)
    }

    pub async fn sub_ob_depth(&mut self, symb: impl Into<String>) -> Result<(), Error> {
        let param = format!("/market/level2:{}", symb.into().to_uppercase());
        self.topics.push(param.clone());
        self.subscribe(param).await
    }

    pub async fn unsub_ob_depth(&mut self, depth: u8, symb: impl Into<String>) -> Result<(), Error> {
        let param = format!("/market/level2:{}", symb.into().to_uppercase());
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

        let ref topic = vec![param.clone()];
        let sub = Sub::new(Methods::Subscribe, param.clone());
        let sub_req = serde_json::to_string(&sub).expect("failed to serialise sub request");

        let write = self.write.as_ref().unwrap();
        let mut write = write.lock().await;
        write
            .send(Message::Text(sub_req))
            .await
            .context("failed to send sub request to stream")?;

        self.topics.push(param.clone());

        Ok(())
    }

    async fn unsubscribe(&mut self, param: String) -> Result<(), Error> {
        if !self.topics.contains(&param) {
            return Ok(());
        }

        let unsub = Sub::new(Methods::Unsubscribe, param.clone());
        let unsub_req = serde_json::to_string(&unsub).expect("failed to serialise sub request");

        let write = self.write.as_ref().unwrap();
        let mut write = write.lock().await;
        write
            .send(Message::Text(unsub_req))
            .await
            .context("failed to send sub request to stream")?;

        self.topics.retain(|val| *val != param.clone());

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
                                    log::error!("Failed to serialise kucoin event. \n {:#?}", e)
                                })
                                .ok()
                            {
                                None => continue,
                                Some(ev) => ev,
                            };

                            match event {
                                Event::OrderBookMsg(d) => {
                                    //println!("sending");
                                    if let Err(e) = tx.send(Event::OrderBookMsg(d)) {
                                        println!("error sending to tokio channel");
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
