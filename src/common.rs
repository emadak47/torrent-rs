use ::phf::phf_map;

pub enum Side {
    BUY, 
    SELL
}

pub static ASSET_CONSTANT_MULTIPLIER: phf::Map<&'static str, f64> = phf_map! {
    "USDT" => 100000000.0,
    "ETH" => 1000000000.0,
    "BTC" => 100000000.0,
    "btc" => 100000000.0,
};

pub struct FlatbufferEvent {
    pub stream_id: u8,
    pub buff: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct CcyPair {
    pub base: String,
    pub quote: String,
    pub product: String,
}

// aggregator pricing details
pub struct PricingDetails {
    pub best_bid: f32,
    pub best_ask: f32,
    pub worse_bid: f32,
    pub worse_ask: f32,
    pub execution_bid: f32,
    pub execution_ask: f32,
    pub imbalance_1: f32,
    pub imbalance_25: f32,
    pub imbalance_50: f32,
    pub imbalance_75: f32,
    pub imbalance_100: f32,
    pub depth: u64,
}


impl CcyPair {
    pub fn to_string(&self) -> String {
        format!("{}_{}_{}", self.base, self.quote, self.product)
    }
}