use super::{
    api::API,
    errors::{BinanceContentError, ErrorKind, Result},
};
use error_chain::bail;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use reqwest::{Response, StatusCode};
use serde::de::DeserializeOwned;

#[derive(Clone)]
pub struct Client {
    api_key: String,
    secret_key: String,
    host: String,
    inner_client: reqwest::Client,
}

impl Client {
    pub fn new(
        api_key: impl Into<String>,
        secret_key: impl Into<String>,
        host: impl Into<String>,
    ) -> Self {
        Client {
            api_key: api_key.into(),
            secret_key: secret_key.into(),
            host: host.into(),
            inner_client: reqwest::Client::builder().build().unwrap(),
        }
    }

    pub async fn post<T: DeserializeOwned>(
        &self,
        endpoint: API,
        request: Option<String>,
    ) -> Result<T> {
        let mut url: String = format!("{}{}", self.host, String::from(endpoint));
        if let Some(request) = request {
            if !request.is_empty() {
                url.push_str(format!("?{}", request).as_str());
            }
        }

        let client = &self.inner_client;
        let response = client.post(url.as_str()).send().await?;

        self.handler(response).await
    }

    pub async fn get<T: DeserializeOwned>(
        &self,
        endpoint: API,
        request: Option<String>,
    ) -> Result<T> {
        let mut url: String = format!("{}{}", self.host, String::from(endpoint));
        if let Some(request) = request {
            if !request.is_empty() {
                url.push_str(format!("?{}", request).as_str());
            }
        }

        let client = &self.inner_client;
        let response = client.get(url.as_str()).send().await?;

        self.handler(response).await
    }

    async fn handler<T: DeserializeOwned>(&self, response: Response) -> Result<T> {
        match response.status() {
            StatusCode::OK => Ok(response.json::<T>().await?),
            StatusCode::INTERNAL_SERVER_ERROR => {
                bail!("Internal Server Error");
            }
            StatusCode::SERVICE_UNAVAILABLE => {
                bail!("Service Unavailable");
            }
            StatusCode::UNAUTHORIZED => {
                bail!("Unauthorized");
            }
            StatusCode::BAD_REQUEST => {
                let error: BinanceContentError = response.json().await?;

                Err(ErrorKind::BinanceError(error).into())
            }
            s => {
                bail!(format!("Received response: {:?}", s));
            }
        }
    }
}