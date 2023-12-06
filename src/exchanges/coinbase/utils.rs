use {
    base64::{engine::general_purpose, Engine},
    hmac::{Hmac, Mac},
    sha2::Sha256,
    std::time::{SystemTime, UNIX_EPOCH},
};

type HmacSha256 = Hmac<Sha256>;

// for public market data coinbase need authentication
pub fn generate_signature(secret: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();
    let message = [&timestamp, "GET", "/users/self/verify"].concat();
    let secret = general_purpose::STANDARD.decode(&secret).unwrap();
    let mut hmac = HmacSha256::new_from_slice(&secret).unwrap();
    hmac.update(message.as_bytes());
    let signature = general_purpose::STANDARD.encode(hmac.finalize().into_bytes());

    signature
}

pub fn get_time() -> u128 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");

    since_the_epoch.as_millis()
}
