use std::sync::{Arc, RwLock};

use bytes::Bytes;
use futures::future::{Either, err};
use futures::prelude::*;
use log::warn;

use crate::{Service, Request};
use crate::client::Client;
use crate::routes::{Route, RoutingTable};

#[derive(Clone, Debug)]
pub struct RouterService {
    data: Arc<ServiceData>,
    client: Client,
}

#[derive(Debug)]
struct ServiceData {
    address: ilp::Address,
    routes: RwLock<RoutingTable>,
}

impl<Req> Service<Req> for RouterService
where
    Req: Request,
{
    type Future = Box<
        dyn Future<
            Item = ilp::Fulfill,
            Error = ilp::Reject,
        > + Send + 'static,
    >;

    fn call(self, request: Req) -> Self::Future {
        Box::new(self.forward(request.into()))
    }
}

impl RouterService {
    pub fn new(client: Client, routes: Vec<Route>) -> Self {
        RouterService {
            data: Arc::new(ServiceData {
                address: client.address().clone(),
                routes: RwLock::new(RoutingTable::new(routes)),
            }),
            client,
        }
    }

    /// Replace the routing table.
    pub fn set_routes(&self, new_routes: Vec<Route>) {
        let mut routes = self.data.routes.write().unwrap();
        *routes = RoutingTable::new(new_routes);
    }

    fn forward(self, prepare: ilp::Prepare)
        -> impl Future<Item = ilp::Fulfill, Error = ilp::Reject>
    {
        let routes = self.data.routes.read().unwrap();
        let route = match routes.resolve(prepare.destination()) {
            Some(route) => route,
            None => {
                warn!(
                    "no route found: destination=\"{}\"",
                    prepare.destination(),
                );
                return Either::B(err(self.make_reject(
                    ilp::ErrorCode::F02_UNREACHABLE,
                    b"no route found",
                )))
            },
        };

        let next_hop = route.endpoint(
            self.data.address.as_addr(),
            prepare.destination(),
        );
        let next_hop = match next_hop {
            Ok(uri) => uri,
            Err(_error) => return Either::B(err(self.make_reject(
                ilp::ErrorCode::F02_UNREACHABLE,
                b"invalid address segment",
            ))),
        };

        let mut builder = hyper::Request::builder();
        builder.method(hyper::Method::POST);
        builder.uri(&next_hop);
        if let Some(auth) = route.auth() {
            builder.header(
                hyper::header::AUTHORIZATION,
                Bytes::from(auth.clone()),
            );
        }

        Either::A(self.client.request(builder, prepare))
    }

    fn make_reject(&self, code: ilp::ErrorCode, message: &[u8]) -> ilp::Reject {
        ilp::RejectBuilder {
            code,
            message,
            triggered_by: self.data.address.as_addr(),
            data: b"",
        }.build()
    }
}

#[cfg(test)]
mod test_router_service {
    use bytes::Bytes;
    use hyper::Uri;
    use lazy_static::lazy_static;

    use crate::NextHop;
    use crate::testing::{self, ADDRESS, RECEIVER_ORIGIN, ROUTES};
    use super::*;

    lazy_static! {
        static ref CLIENT: Client = Client::new(ADDRESS.to_address());
        static ref ROUTER: RouterService =
            RouterService::new(CLIENT.clone(), ROUTES.clone());
    }

    #[test]
    fn test_outgoing_request_bilateral() {
        testing::MockServer::new()
            .test_request(|req| {
                assert_eq!(req.method(), hyper::Method::POST);
                assert_eq!(req.uri().path(), "/alice");
                assert_eq!(
                    req.headers().get("Authorization").unwrap(),
                    "alice_auth",
                );
                assert_eq!(
                    req.headers().get("Content-Type").unwrap(),
                    "application/octet-stream",
                );
            })
            .test_body(|body| {
                assert_eq!(body.as_ref(), testing::PREPARE.as_ref());
            })
            .with_response(|| {
                hyper::Response::builder()
                    .status(200)
                    .body(hyper::Body::from(testing::FULFILL.as_ref()))
                    .unwrap()
            })
            .run({
                ROUTER.clone()
                    .call(testing::PREPARE.clone())
                    .then(|result| -> Result<(), ()> {
                        assert_eq!(result.unwrap(), *testing::FULFILL);
                        Ok(())
                    })
            });
    }

    #[test]
    fn test_outgoing_request_multilateral() {
        testing::MockServer::new()
            .test_request(|req| {
                assert_eq!(req.method(), hyper::Method::POST);
                assert_eq!(req.uri().path(), "/bob/1234/ilp");
                assert_eq!(
                    req.headers().get("Authorization").unwrap(),
                    "bob_auth",
                );
            })
            .with_response(|| {
                hyper::Response::builder()
                    .status(200)
                    .body(hyper::Body::from(testing::FULFILL.as_ref()))
                    .unwrap()
            })
            .run({
                ROUTER.clone()
                    .call(testing::PREPARE_MULTILATERAL.clone())
                    .then(|result| -> Result<(), ()> {
                        assert_eq!(result.unwrap(), *testing::FULFILL);
                        Ok(())
                    })
            });
    }

    #[test]
    fn test_no_route() {
        let expect_reject = ilp::RejectBuilder {
            code: ilp::ErrorCode::F02_UNREACHABLE,
            message: b"no route found",
            triggered_by: ADDRESS,
            data: b"",
        }.build();
        let router = RouterService::new(CLIENT.clone(), vec![ROUTES[1].clone()]);
        testing::MockServer::new().run({
            router
                .call(testing::PREPARE.clone())
                .then(move |result| -> Result<(), ()> {
                    assert_eq!(result.unwrap_err(), expect_reject);
                    Ok(())
                })
        });
    }

    #[test]
    fn test_set_routes() {
        let router = ROUTER.clone();
        router.set_routes(vec![
            Route::new(
                Bytes::from("test.alice."),
                NextHop::Bilateral {
                    endpoint: format!("{}/new_alice", RECEIVER_ORIGIN).parse::<Uri>().unwrap(),
                    auth: None,
                },
            ),
        ]);
        testing::MockServer::new()
            .test_request(|req| {
                assert_eq!(req.uri().path(), "/new_alice");
            })
            .with_response(|| {
                hyper::Response::builder()
                    .status(200)
                    .body(hyper::Body::from(testing::FULFILL.as_ref()))
                    .unwrap()
            })
            .run({
                router
                    .call(testing::PREPARE.clone())
                    .then(|result| -> Result<(), ()> {
                        assert_eq!(result.unwrap(), *testing::FULFILL);
                        Ok(())
                    })
            });
    }
}
