pub mod app;
mod client;
mod combinators;
mod middlewares;
mod serde;
mod services;
#[cfg(test)]
mod testing;

use std::borrow::Borrow;

use futures::prelude::*;

pub use self::client::Client;
pub use self::middlewares::AuthToken;
pub use self::services::{BigQueryConfig, BigQueryServiceConfig, DebugServiceOptions};
pub use self::services::{NextHop, RouteFailover, RoutingTable, StaticRoute};

// TODO maybe support ping protocol

pub trait Service<Req: Request>: Clone {
    type Future: 'static + Send
        + Future<Output = Result<ilp::Fulfill, ilp::Reject>>;

    fn setup(&mut self) {}
    fn call(self, request: Req) -> Self::Future;
}

pub trait Request: Into<ilp::Prepare> + Borrow<ilp::Prepare> {}
impl Request for ilp::Prepare {}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Relation {
    Child,
    Peer,
    Parent,
}

/// Allow closures as services when testing.
#[cfg(test)]
impl<F, Req, Res> Service<Req> for F
where
    F: Clone + Fn(Req) -> Res,
    Req: Request,
    Res: 'static + Send + Future<Output = Result<ilp::Fulfill, ilp::Reject>>,
{
    type Future = Res;

    fn call(self, request: Req) -> Self::Future {
        (self)(request)
    }
}
