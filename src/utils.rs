use serde::{Deserialize, Deserializer};
use std::fmt::{self, Display};
use std::result;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

pub type Result<T> = result::Result<T, TorrentError>;
pub type Symbol = String;

#[allow(non_camel_case_types)]
#[derive(Debug)]
pub enum Exchange {
    OKX,
    COINBASE,
    BINANCE,
}

impl Display for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Exchange::COINBASE => write!(f, "Coinbase"),
            Exchange::OKX => write!(f, "Okx"),
            Exchange::BINANCE => write!(f, "Binance"),
        }
    }
}

#[derive(Debug)]
pub enum TorrentError {
    BadStatus(String),
    BadRequest(String),
    BadConnection(String),
    BadParse(String),
    Unknown(String),
    BadZenoh(String),
}

impl Display for TorrentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TorrentError::BadStatus(v) => write!(f, "non-200 status code: {}", v),
            TorrentError::BadRequest(v) => write!(f, "request error: {}", v),
            TorrentError::BadConnection(v) => write!(f, "connection error: {}", v),
            TorrentError::BadParse(v) => write!(f, "prasing error: {}", v),
            TorrentError::Unknown(v) => write!(f, "unknown error: {}", v),
            TorrentError::BadZenoh(v) => write!(f, "zenoh error: {}", v),
        }
    }
}

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backword")
        .as_secs()
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StringOrNumeric {
    String(String),
    Numeric(f64),
}

pub fn from_str<'de, S, D>(deserializer: D) -> result::Result<S, D::Error>
where
    S: FromStr + Default,
    S::Err: Display,
    D: Deserializer<'de>,
{
    let s: String = match Deserialize::deserialize(deserializer) {
        Ok(value) => match value {
            StringOrNumeric::String(v) => v,
            StringOrNumeric::Numeric(v) => v.to_string(),
        },
        Err(_) => String::default(),
    };

    Ok(S::from_str(&s).unwrap_or_default())
}

#[macro_export]
macro_rules! dbg {
    ($fmt:expr $(, $($arg:tt)*)?) => {
        println!(concat!("[DEBUG] ", $fmt), $($($arg)*)?);
    };
}
