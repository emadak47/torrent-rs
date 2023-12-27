pub static DATA_FEED: &str = "atrimo/datafeeds";
static ASSET_CONSTANT_MULTIPLIER: f64 = 1e10;

pub fn scale<'a>(num: impl Into<&'a str>) -> Result<u64, std::num::ParseFloatError> {
    let num_f64: f64 = num.into().parse()?;
    Ok((num_f64 * ASSET_CONSTANT_MULTIPLIER) as u64)
}

pub enum Exchange {
    Binance,
    Okx,
}

impl From<Exchange> for String {
    fn from(value: Exchange) -> Self {
        match value {
            Exchange::Binance => String::from("binance"),
            Exchange::Okx => String::from("okx"),
        }
    }
}

pub enum Side {
    BUY,
    SELL,
}

pub enum SymbolPair<'a> {
    BinanceSpot(&'a str),
    BinanceFutures(&'a str),
    OkxSpot(&'a str),
    OkxFutures(&'a str),
}

pub fn get_symbol_pair(pair: SymbolPair) -> Option<CcyPair> {
    match pair {
        SymbolPair::BinanceSpot(symb) => {
            let regex =
                regex::Regex::new(r"^(\w+)(BTC|TRY|ETH|BNB|USDT|PAX|TUSD|USDC|XRP|USDS)$").ok()?;
            let capture = regex.captures(symb)?;
            let (base, quote) = (capture.get(1)?, capture.get(2)?);

            return Some(CcyPair {
                base: base.as_str().to_string(),
                quote: quote.as_str().to_string(),
                product: "spot".to_string(),
            });
        }
        SymbolPair::BinanceFutures(symb) => {
            let symb = symb.strip_suffix("FUTURES")?;
            let regex =
                regex::Regex::new(r"^(\w+)(BTC|TRY|ETH|BNB|USDT|PAX|TUSD|USDC|XRP|USDS)$").ok()?;
            let capture = regex.captures(symb)?;
            let (base, quote) = (capture.get(1)?, capture.get(2)?);

            return Some(CcyPair {
                base: base.as_str().to_string(),
                quote: quote.as_str().to_string(),
                product: "futures".to_string(),
            });
        }
        SymbolPair::OkxSpot(symb) => {
            let parts = symb.split('-').collect::<Vec<&str>>();

            if parts.len() == 2 {
                Some(CcyPair {
                    base: parts[0].to_string(),
                    quote: parts[1].to_string(),
                    product: "spot".to_string(),
                })
            } else {
                None
            }
        }
        SymbolPair::OkxFutures(symb) => {
            let parts = symb.split('-').collect::<Vec<&str>>();

            if parts.len() == 3 {
                Some(CcyPair {
                    base: parts[0].to_string(),
                    quote: parts[1].to_string(),
                    product: "futures".to_string(),
                })
            } else {
                None
            }
        }
    }
}

#[derive(Debug)]
pub struct FlatbufferEvent {
    pub stream_id: u8,
    pub buff: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct ZenohEvent {
    pub stream_id: u8,
    pub buff: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct CcyPair {
    pub base: String,
    pub quote: String,
    pub product: String,
}

impl From<CcyPair> for String {
    fn from(value: CcyPair) -> Self {
        format!("{}-{}-{}", value.base, value.quote, value.product)
    }
}
