pub mod app;
mod client;
mod middlewares;
mod routes;
mod serde;
mod services;
#[cfg(test)]
mod testing;

use std::borrow::Borrow;

use futures::prelude::*;

pub use self::client::Client;
pub use self::middlewares::AuthToken;
pub use self::routes::{NextHop, Route};

// TODO Limit max ilp packet (or http body) length
// TODO maybe support ping protocol

pub trait Service<Req: Request>: Clone {
    type Future: 'static + Send + Future<
        Item = ilp::Fulfill,
        Error = ilp::Reject,
    >;

    fn call(self, request: Req) -> Self::Future;
}

pub trait Request: Into<ilp::Prepare> + Borrow<ilp::Prepare> {}
impl Request for ilp::Prepare {}

/// Allow closures as services when testing.
#[cfg(test)]
impl<F, Req, Res> Service<Req> for F
where
    F: Clone + Fn(Req) -> Res,
    Req: Request,
    Res: 'static + Send + Future<
        Item = ilp::Fulfill,
        Error = ilp::Reject,
    >,
{
    type Future = Res;

    fn call(self, request: Req) -> Self::Future {
        (self)(request)
    }
}
