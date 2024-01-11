use serde::{Deserialize, Deserializer};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

pub type Result<T> = std::result::Result<T, TorrentError>;

#[allow(non_camel_case_types)]
#[derive(Debug)]
pub enum Exchange {
    OKX,
    COINBASE,
}

impl fmt::Display for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Exchange::COINBASE => write!(f, "Coinbase"),
            Exchange::OKX => write!(f, "Okx"),
        }
    }
}

#[derive(Debug)]
pub enum TorrentError {
    BadConnection(String),
    BadParse(String),
}

impl std::fmt::Display for TorrentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TorrentError::BadConnection(v) => write!(f, "connection error: {}", v),
            TorrentError::BadParse(v) => write!(f, "prasing error: {}", v),
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

pub fn from_str<'de, S, D>(deserializer: D) -> std::result::Result<S, D::Error>
where
    S: std::str::FromStr + Default,
    S::Err: std::fmt::Display,
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
