use crate::utils::{Result, TorrentError};
use reqwest::{Response, StatusCode};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Display;

pub struct RestClient<'a> {
    pub host: &'a str,
    client: reqwest::Client,
}

impl<'a> RestClient<'a> {
    pub fn new(host: &'a str) -> Self {
        Self {
            host,
            client: reqwest::Client::builder().build().unwrap(),
        }
    }

    pub async fn get<T, E, S>(&self, endpoint: impl Into<String>, request: Option<S>) -> Result<T>
    where
        T: DeserializeOwned,
        E: Display + DeserializeOwned,
        S: Serialize,
    {
        let url = format!("{}{}", self.host, endpoint.into());
        let response = if let Some(request) = request {
            self.client.get(url.as_str()).query(&request).send().await
        } else {
            self.client.get(url.as_str()).send().await
        };
        let response = match response {
            Ok(r) => r,
            Err(e) => return Err(TorrentError::BadRequest(format!("{}", e))),
        };

        self.handler::<T, E>(response).await
    }

    async fn handler<T, E>(&self, response: Response) -> Result<T>
    where
        T: DeserializeOwned,
        E: Display + DeserializeOwned,
    {
        match response.status() {
            StatusCode::OK => match response.json::<T>().await {
                Ok(res) => Ok(res),
                Err(e) => Err(TorrentError::BadParse(format!("{}", e))),
            },
            StatusCode::INTERNAL_SERVER_ERROR => {
                Err(TorrentError::BadStatus("Internal Server Error".to_string()))
            }
            StatusCode::SERVICE_UNAVAILABLE => {
                Err(TorrentError::BadStatus("Service Unavailable".to_string()))
            }
            StatusCode::UNAUTHORIZED => Err(TorrentError::BadStatus("Unauthorized".to_string())),
            StatusCode::BAD_REQUEST => {
                let err = match response.json::<E>().await {
                    Ok(err) => err,
                    Err(e) => return Err(TorrentError::BadParse(format!("{}", e))),
                };
                Err(TorrentError::BadRequest(format!("{}", err)))
            }
            e => Err(TorrentError::Unknown(format!("{:?}", e))),
        }
    }
}
