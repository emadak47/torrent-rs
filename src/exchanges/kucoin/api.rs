#[allow(clippy::all)]
pub enum API {
    Spot(Spot),
}

/// Endpoint for production and test orders.
///
/// Orders issued to test are validated, but not sent into the matching engine.
pub enum Spot {
    OrderBookSnapshot,
    BulletPublic
}

impl From<API> for String {
    fn from(item: API) -> Self {
        String::from(match item {
            API::Spot(route) => match route {
                Spot::OrderBookSnapshot => "/api/v1/market/orderbook/level2_100",
                Spot::BulletPublic => "/api/v1/bullet-public",
            },
        })
    }
}
