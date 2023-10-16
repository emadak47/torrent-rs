use ::phf::phf_map;

pub enum Side {
    BUY, 
    SELL
}

#[derive(Debug)]
pub struct CcyPair {
    pub base: String, 
    pub quote: String, 
}

pub static ASSET_CONSTANT_MULTIPLIER: phf::Map<&'static str, f64> = phf_map! {
    "USDT" => 100000000.0,
    "ETH" => 1000000000.0,
    "BTC" => 100000000.0,
};