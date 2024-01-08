pub mod binance;
pub mod bullish;
pub mod okx;

#[derive(Clone, Debug)]
pub struct Config {
    pub spot_rest_api_endpoint: String,
    pub futures_rest_api_endpoint: String,
    pub spot_ws_endpoint: String,
    pub futures_ws_endpoint: String,
}

impl Config {
    pub fn binance() -> Self {
        Self {
            spot_rest_api_endpoint: "https://api.binance.com".into(),
            futures_rest_api_endpoint: "https://fapi.binance.com".into(),
            spot_ws_endpoint: "wss://stream.binance.com:9443/ws".into(),
            futures_ws_endpoint: "wss://fstream.binance.com/stream?streams=".into(),
        }
    }

    pub fn okx() -> Self {
        // same endpoints
        Self {
            spot_rest_api_endpoint: "https://www.okx.com/".into(),
            futures_rest_api_endpoint: "https://www.okx.com/".into(),
            spot_ws_endpoint: "wss://ws.okx.com:8443/ws/v5/public".into(),
            futures_ws_endpoint: "wss://ws.okx.com:8443/ws/v5/public".into(),
        }
    }

    pub fn bullish() -> Self {
        // same endpoints
        Self {
            spot_rest_api_endpoint: "https://www.okx.com/".into(),
            futures_rest_api_endpoint: "https://www.okx.com/".into(),
            spot_ws_endpoint: "wss://api.exchange.bullish.com".into(),
            futures_ws_endpoint: "wss://ws.okx.com:8443/ws/v5/public".into(),
        }
    }
}
