#[derive(Clone, Debug)]
pub struct Config {
    pub spot_rest_api_endpoint: String,
    pub spot_ws_endpoint: String,
    pub recv_window: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            spot_rest_api_endpoint: "https://api.binance.com".into(),
            spot_ws_endpoint: "wss://stream.binance.com:9443/ws".into(),
            recv_window: 5000,
        }
    }
}