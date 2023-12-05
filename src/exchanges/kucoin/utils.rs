use std::time::{SystemTime, UNIX_EPOCH};
use crate::common::CcyPair;


pub fn get_time() -> u128 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");

    since_the_epoch.as_millis()
}


pub fn get_symbol_pair(symbol: &str) -> CcyPair {
    let parts: Vec<&str> = symbol.split('-').collect();

    if parts.len() == 2 {
        return CcyPair {
            base: parts[0].to_string(),
            quote: parts[1].to_string(),
            product: "spot".to_string(), // TODO : not default
        };
    }

    // If the quote is not found, panic with an error message
    panic!(
        "Quote not found in the symbol: {}, Please add before running again",
        symbol
    );
}