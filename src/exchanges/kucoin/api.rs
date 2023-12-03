#[allow(clippy::all)]
pub enum API {
    Spot(Spot),
}

/// Endpoint for production and test orders.
///
/// Orders issued to test are validated, but not sent into the matching engine.
pub enum Spot {
    OrderBook,
    BulletPublic
}

impl From<API> for String {
    fn from(item: API) -> Self {
        String::from(match item {
            API::Spot(route) => match route {
                Spot::OrderBook => "/api/v3/ping",
                Spot::BulletPublic => "/api/v1/bullet-public",
            },

        })
    }
}
