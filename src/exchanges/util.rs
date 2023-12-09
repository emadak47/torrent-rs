use crate::common::CcyPair;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use regex::Regex;

pub fn get_time() -> u128 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");

    since_the_epoch.as_millis()
}


pub fn get_symbol_pair(symbol: &str) -> CcyPair {
    // Create the regex pattern
    let re = Regex::new(r"^(\w+)(BTC|TRY|ETH|BNB|USDT|PAX|TUSD|USDC|XRP|USDS)$").unwrap();
    let symbol_str: &str = symbol;
    // Check for matches in the symbol
    if let Some(captures) = re.captures(symbol_str) {
        if let (Some(base), Some(quote)) = (captures.get(1), captures.get(2)) {
            return CcyPair {
                base: base.as_str().to_string(),
                quote: quote.as_str().to_string(),
                product: "spot".to_string(),
            };
        }
    }

    // If the quote is not found, panic with an error message
    panic!(
        "Quote not found in the symbol: {}, Please add before running again",
        symbol
    );
}