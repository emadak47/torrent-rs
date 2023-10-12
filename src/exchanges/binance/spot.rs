use super::config::Config;
use failure::{Error, ResultExt};
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
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
enum Methods {
    Subscribe,
    Unsubscribe,
    Listsubs,
}

pub struct SpotWSClientBuilder {
    url: String,
    topics: Vec<String>,
}

impl Default for SpotWSClientBuilder {
    fn default() -> Self {
        let config = Config::default();
        Self {
            url: config.spot_ws_endpoint.into(),
            topics: Vec::new(),
        }
    }
}

impl SpotWSClientBuilder {
    pub fn sub_trade(&mut self, symb: impl Into<String>) {
        self.topics.push(symb.into());
    }

    pub fn sub_ob_depth(&mut self, symb: impl Into<String>) {
        self.topics.push(symb.into());
    }

    pub async fn build(self) -> Result<SpotWSClient, Error> {
        let (stream, _) = tokio_tungstenite::connect_async(self.url)
            .await
            .context("failed to connect to binance wss")?;

        let (write, read) = stream.split();

        Ok(SpotWSClient {
            topics: self.topics,
            id: 0,
            write: Arc::new(Mutex::new(write)),
            read,
        })
    }
}

pub struct SpotWSClient {
    topics: Vec<String>,
    id: usize,
    write: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
    read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

impl SpotWSClient {
    pub fn sub_trade(&mut self, symb: impl Into<String>) {
        let param = format!("{}@trade", symb.into());
        self.topics.push(param);
    }

    pub fn sub_ob_depth(&mut self, symb: impl Into<String>) {
        let param = format!("{}@depth", symb.into());
        self.topics.push(param.clone());
    }

    async fn subscribe(&mut self, param: impl Into<String>) -> Result<(), Error> {
        let param = param.into();
        if self.topics.contains(&param) {
            return Ok(());
        }

        let ref topic = vec![param.clone()];
        let sub = Sub::new(Methods::Subscribe, Some(topic), &self.id);
        let sub_req = serde_json::to_string(&sub).expect("failed to serialise sub request");

        let mut write = self.write.lock().await;
        write
            .send(Message::Text(sub_req))
            .await
            .context("failed to send sub request to stream")?;

        self.topics.push(param);
        self.id += 1;

        Ok(())
    }

    pub fn unsub_trade(&mut self, symb: impl Into<String>) {
        let param = format!("{}@trade", symb.into());
    }

    pub fn unsub_ob_depth(&mut self, symb: impl Into<String>) {
        let param = format!("{}@depth", symb.into());
    }

    pub fn unsubscribe(&self) {}
}
