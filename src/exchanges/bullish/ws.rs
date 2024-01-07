use super::{types::{Event, Methods, Subscription}};
use crate::exchanges::Config;
use failure::{Error, ResultExt};
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use std::{collections::HashMap, sync::Arc};
use tokio::{net::TcpStream, sync::Mutex};
use tokio_tungstenite::{tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream};

// For Spot and Futures same endpoint
// Ticker Symbol changes

pub enum WsEndPoint {
    MULTI_ORDERBOOK
}

pub struct SpotWSClientBuilder {
    url: String,
    topics: Vec<String>,
}

impl SpotWSClientBuilder {
    pub fn new(endpoint: WsEndPoint) -> Self {
        let config = Config::bullish();

        let url = match endpoint {
            WsEndPoint::MULTI_ORDERBOOK => format!(
                "{}/trading-api/v1/market-data/orderbook", config.spot_ws_endpoint
            ),
        };

        Self {
            url,
            topics: Vec::new(),
        }
    }
}


impl SpotWSClientBuilder {
    pub fn sub_ob_depth(&mut self, symb: &str) {
        let sub = Subscription::new(Methods::Subscribe, "l2Orderbook", symb, 1);
        let sub_req = serde_json::to_string(&sub).unwrap();
        self.topics.push(sub_req);
    }

    pub async fn build(self) -> Result<WSClient, Error> {
        Ok(WSClient {
            url: self.url,
            topics: self.topics,
            write: None,
        })
    }
}

type SharedWSS = Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>;
pub struct WSClient {
    url: String,
    topics: Vec<String>,
    write: Option<SharedWSS>,
}

impl WSClient {
    pub async fn connect(
        self,
        tx: tokio::sync::mpsc::UnboundedSender<Event>,
    ) -> Result<Self, Error> {
        // depending on which
        let (stream, _) = tokio_tungstenite::connect_async(&self.url)
            .await
            .context("failed to connect to bullish wss")?;

        let (mut write, read) = stream.split();

        for topic in &self.topics {
            write
                .send(Message::Text(topic.clone()))
                .await
                .context("failed to send init sub request to stream")?;
        }
        
        let write = Arc::new(Mutex::new(write));

        tokio::spawn(WSClient::run(Arc::clone(&write), read, tx));

        Ok(Self {
            url: self.url,
            topics: self.topics,
            write: Some(write),
        })
    }

    pub async fn sub_ob_depth(&mut self, symb: &str) -> Result<(), Error> {
        let sub = Subscription::new(Methods::Subscribe, "l2Orderbook", symb, 1);
        let sub_req = serde_json::to_string(&sub).unwrap();
        self.subscribe(sub_req).await
    }

    pub async fn unsub_ob_depth(&mut self, symb: &str) -> Result<(), Error> {
        let sub = Subscription::new(Methods::Subscribe, "l2Orderbook", symb, 1);
        let sub_req = serde_json::to_string(&sub).unwrap();
        self.unsubscribe(sub_req).await
    }

    pub async fn list_subs(&self) -> &Vec<String> {
        &self.topics
    }

    async fn subscribe(&mut self, param: String) -> Result<(), Error> {
        if self.topics.contains(&param) {
            return Ok(());
        }

        let write = self.write.as_ref().unwrap();
        let mut write = write.lock().await;
        write
            .send(Message::Text(param.clone()))
            .await
            .context("failed to send sub request to stream")?;

        self.topics.push(param);

        Ok(())
    }

    async fn unsubscribe(&mut self, param: String) -> Result<(), Error> {
        if !self.topics.contains(&param) {
            return Ok(());
        }

        let write = self.write.as_ref().unwrap();
        let mut write = write.lock().await;
        write
            .send(Message::Text(param.clone()))
            .await
            .context("failed to send sub request to stream")?;

        self.topics.retain(|val| *val != param);

        Ok(())
    }

    pub async fn run(
        write: SharedWSS,
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
                                    println!("Failed to serialise bullish event. \n {:#?}", e)
                                })
                                .ok()
                            {
                                None => continue,
                                Some(ev) => ev,
                            };  

                            match event {
                                Event::OrderBookMsg(d) => {
                                    if let Err(e) = tx.send(Event::OrderBookMsg(d)) {
                                        println!("Error sending depth ob event through tokio channel \n {:#?}", e);
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
                    println!("none read");
                    log::debug!("'None` read from Webscoket stream");
                }
            }
        }
    }
}
