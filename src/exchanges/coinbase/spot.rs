use super::{super::config::Config, types::Event};
use failure::{Error, ResultExt};
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde::Serialize;
use serde::Deserialize;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::{net::TcpStream, sync::Mutex};
use tokio_tungstenite::{tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream};
use crate::exchanges::coinbase::utils::generate_signature;

#[derive(Debug, Deserialize, Serialize)]
pub struct AuthParams {
    signature: String,
    key: String,
    passphrase: String,
}

impl AuthParams {
    pub fn new(key: String, secret: String, passphrase: String) -> Self {
        let signature = generate_signature(&secret); 
        Self { signature, key, passphrase }
    }
}

// Authenticated Websocket Requests
#[derive(Debug, Serialize)]
pub struct Sub {
    #[serde(rename = "type")]
    Type: Methods,
    product_ids: Option<Vec<String>>,
    channels: Option<Vec<String>>,
    #[serde(flatten)]
    auth_info: AuthParams,
}

impl Sub {
    pub(crate) fn new(
        Type: Methods, 
        channels: Option<Vec<String>>, 
        product_ids: Option<Vec<String>>) -> Self {
        let auth_info = AuthParams::new("key".to_string(), 
                                       "FW8j4YobD26PVP6QLu0sv4Dv7OzrtfgQKzn8FoIMwGzMW9Y0VmX1DatbLIfXoCHV".to_string(),
                                       "passphrase".to_string());
        Self { Type, product_ids, channels, auth_info }
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
        let config = Config::coinbase();
        Self {
            url: config.spot_ws_endpoint.into(),
            topics: Vec::new(),
        }
    }
}

impl SpotWSClientBuilder {
    pub fn sub_ob_depth(&mut self, args: &[&str]) {
        let param = vec!(String::from("level2"));
        let mut symb = Vec::new();
        for arg in args {
            let param = format!("{}", arg);
            symb.push(param);
        }

        let sub = Sub::new(Methods::Subscribe, 
                           Some(symb), 
                           Some(param));

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
            .context("failed to connect to coinbase wss")?;

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

    pub async fn sub_ob_depth(&mut self, args: &[&str]) -> Result<(), Error> {
        let param = vec!(String::from("level2"));
        let mut symb = Vec::new();
        for arg in args {
            let param = format!("{}", arg);
            symb.push(param);
        }
        
        let sub = Sub::new(Methods::Subscribe, 
                           Some(symb), 
                           Some(param));
        
        let sub_req = serde_json::to_string(&sub).expect("failed to serialise sub request");
        self.subscribe(sub_req).await
    }

    pub async fn unsub_ob_depth(&mut self, args: &[&str]) -> Result<(), Error> {
        let param = vec!(String::from("level2"));
        let mut symb = Vec::new();
        for arg in args {
            let param = format!("{}", arg);
            symb.push(param);
        }
        let unsub = Sub::new(Methods::Unsubscribe, 
                            Some(symb), 
                            Some(param));

        let un_sub_req = serde_json::to_string(&unsub).expect("failed to serialise sub request");
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
                                    log::error!("Failed to serialise coinbase event. \n {:#?}", e)
                                })
                                .ok()
                            {
                                None => continue,
                                Some(ev) => ev,
                            };
                            match event {
                                Event::Snapshot(d) => {
                                    println!("snapshot recieved");
                                }
                                Event::L2update(d) => {
                                    println!("update event received");
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
