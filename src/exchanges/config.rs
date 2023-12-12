#[derive(Clone, Debug)]
pub struct Config {
    pub spot_rest_api_endpoint: String,
    pub spot_ws_endpoint: String,
    pub futures_ws_endpoint: String,
    pub futures_rest_api_endpoint: String,
}

impl Config {
    pub fn binance() -> Self {
        Self {
            spot_rest_api_endpoint: "https://api.binance.com".into(),
            futures_rest_api_endpoint: "".into(),
            spot_ws_endpoint: "wss://stream.binance.com:9443/ws".into(),
            futures_ws_endpoint: "wss://ws-api-spot.kucoin.com/".into(),
        }
    }

    pub fn okx() -> Self {
        Self {
            spot_rest_api_endpoint: "https://www.okx.com/".into(),
            futures_rest_api_endpoint: "".into(),
            spot_ws_endpoint: "wss://ws.okx.com:8443/ws/v5/public".into(),
            futures_ws_endpoint: "wss://ws-api-spot.kucoin.com/".into(),
        }
    }

    pub fn bybit() -> Self {
        Self {
            spot_rest_api_endpoint: "https://www.bybit.com/".into(),
            futures_rest_api_endpoint: "".into(),
            spot_ws_endpoint: "wss://stream.bybit.com/v5/public/spot".into(),
            futures_ws_endpoint: "wss://ws-api-spot.kucoin.com/".into(),
        }
    }

    pub fn coinbase() -> Self {
        Self {
            spot_rest_api_endpoint: "https://www.coinbase.com/".into(),
            futures_rest_api_endpoint: "".into(),
            spot_ws_endpoint: "wss://ws-feed.pro.coinbase.com".into(),
            futures_ws_endpoint: "wss://ws-api-spot.kucoin.com/".into(),
        }
    }

    pub fn huobi() -> Self {
        Self {
            spot_rest_api_endpoint: "https://api.huobi.pro".into(),
            futures_rest_api_endpoint: "".into(),
            spot_ws_endpoint: "wss://api.huobi.pro/feed".into(),
            futures_ws_endpoint: "wss://ws-api-spot.kucoin.com/".into(),
        }
    }

    pub fn kucoin() -> Self {
        Self {
            spot_rest_api_endpoint: "https://api.kucoin.com".into(),
            futures_rest_api_endpoint: "https://api-futures.kucoin.com".into(),
            spot_ws_endpoint: "wss://ws-api-futures.kucoin.com/".into(),
            futures_ws_endpoint: "wss://ws-api-spot.kucoin.com/".into(),
        }
    }
}