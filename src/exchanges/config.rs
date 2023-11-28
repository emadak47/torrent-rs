#[derive(Clone, Debug)]
pub struct Config {
    pub spot_rest_api_endpoint: String,
    pub spot_ws_endpoint: String,
}

impl Config {
    pub fn binance() -> Self {
        Self {
            spot_rest_api_endpoint: "https://api.binance.com".into(),
            spot_ws_endpoint: "wss://stream.binance.com:9443/ws".into(),
        }
    }

    pub fn okx() -> Self {
        Self {
            spot_rest_api_endpoint: "https://www.okx.com/".into(),
            spot_ws_endpoint: "wss://ws.okx.com:8443/ws/v5/public".into(),
        }
    }

    pub fn bybit() -> Self {
        Self {
            spot_rest_api_endpoint: "https://www.bybit.com/".into(),
            spot_ws_endpoint: "wss://stream.bybit.com/v5/public/spot".into(),
        }
    }
}