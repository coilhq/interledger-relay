use std::pin::Pin;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use futures::future::{Either, err};
use futures::prelude::*;
use log::{debug, warn};

use crate::{Service, Request};
use crate::client::{Client, RequestOptions};
use super::{RoutingError, RoutingTable};

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
    type Future = Pin<Box<
        dyn Future<
            Output = Result<ilp::Fulfill, ilp::Reject>,
        > + Send + 'static,
    >>;

    fn call(self, request: Req) -> Self::Future {
        Box::pin(self.forward(request.into()))
    }
}

impl RouterService {
    pub fn new(client: Client, routes: RoutingTable) -> Self {
        RouterService {
            data: Arc::new(ServiceData {
                address: client.address().clone(),
                routes: RwLock::new(routes),
            }),
            client,
        }
    }

    /// Replace the routing table.
    pub fn set_routes(&self, new_routes: RoutingTable) {
        let mut routes = self.data.routes.write().unwrap();
        *routes = new_routes;
    }

    fn forward(self, prepare: ilp::Prepare)
        -> impl Future<Output = Result<ilp::Fulfill, ilp::Reject>>
    {
        let routes = self.data.routes.read().unwrap();
        let (route_index, route) = match routes.resolve(&prepare) {
            Ok((i, route)) => (i, route),
            Err(RoutingError::NoRoute) => {
                debug!(
                    "no route exists: destination=\"{}\"",
                    prepare.destination(),
                );
                return Either::Right(err(self.make_reject(
                    ilp::ErrorCode::F02_UNREACHABLE,
                    b"no route exists",
                )));
            },
            Err(RoutingError::NoHealthyRoute) => {
                debug!(
                    "no healthy route found: destination=\"{}\"",
                    prepare.destination(),
                );
                return Either::Right(err(self.make_reject(
                    ilp::ErrorCode::T01_PEER_UNREACHABLE,
                    b"no healthy route found",
                )));
            },
        };
        let has_failover = route.config.failover.is_some();

        let next_hop = route.config.endpoint(
            self.data.address.as_addr(),
            prepare.destination(),
        );
        let next_hop = match next_hop {
            Ok(uri) => uri,
            Err(error) => {
                warn!("error generating endpoint: error={}", error);
                return Either::Right(err(self.make_reject(
                    ilp::ErrorCode::F02_UNREACHABLE,
                    b"invalid address segment",
                )));
            },
        };

        let auth = route.config.auth().cloned().map(Bytes::from);
        // Don't hold onto the table mutex during the HTTP request.
        std::mem::drop(routes);

        let service_data = Arc::clone(&self.data);
        let do_request = self.client
            .request(RequestOptions {
                method: hyper::Method::POST,
                uri: next_hop,
                auth,
                peer_name: None,
            }, prepare)
            .inspect(move |result| {
                if has_failover {
                    let is_success =
                        response_is_ok(service_data.address.as_addr(), result);
                    service_data.routes
                        .read()
                        .unwrap()
                        .update(route_index, is_success)
                }
            });

        Either::Left(do_request)
    }

    fn make_reject(&self, code: ilp::ErrorCode, message: &[u8]) -> ilp::Reject {
        ilp::RejectBuilder {
            code,
            message,
            triggered_by: Some(self.data.address.as_addr()),
            data: b"",
        }.build()
    }
}

fn response_is_ok(
    connector_address: ilp::Addr,
    response: &Result<ilp::Fulfill, ilp::Reject>,
) -> bool {
    let is_unhealthy = match response {
        Ok(_) => false,
        Err(reject) => {
            // Corresponds to a 5xx error and connection errors.
            reject.code() == ilp::ErrorCode::T01_PEER_UNREACHABLE
                && reject.triggered_by() == Some(connector_address)
        },
    };
    !is_unhealthy
}

#[cfg(test)]
mod test_router_service {
    use bytes::Bytes;
    use hyper::Uri;
    use lazy_static::lazy_static;

    use crate::{NextHop, RouteFailover, RoutingPartition, StaticRoute};
    use crate::testing::{self, ADDRESS, RECEIVER_ORIGIN, ROUTES};
    use super::super::table::RouteIndex;
    use super::*;

    lazy_static! {
        static ref CLIENT: Client = Client::new(ADDRESS.to_address());
        static ref ROUTER: RouterService = RouterService::new(
            CLIENT.clone(),
            RoutingTable::new(ROUTES.clone(), RoutingPartition::default()),
        );
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
                    .map(|result| {
                        assert_eq!(result.unwrap(), *testing::FULFILL);
                    })
            });
    }

    #[test]
    fn test_mark_as_unhealthy() {
        let router = RouterService::new(CLIENT.clone(), RoutingTable::new(vec![
            StaticRoute {
                failover: Some(RouteFailover {
                    window_size: 20,
                    fail_ratio: 0.01,
                    fail_duration: std::time::Duration::from_secs(5),
                }),
                ..ROUTES[0].clone()
            },
        ], RoutingPartition::default()));
        testing::MockServer::new()
            .test_request(|req| { assert_eq!(req.uri().path(), "/alice"); })
            .test_body(|body| { assert_eq!(body.as_ref(), testing::PREPARE.as_ref()); })
            .with_response(|| {
                hyper::Response::builder()
                    .status(500)
                    .body(hyper::Body::empty())
                    .unwrap()
            })
            .run({
                router.clone()
                    .call(testing::PREPARE.clone())
                    .map(move |result| {
                        assert!(result.is_err());
                        let table = router.data.routes.read().unwrap();
                        let route = &table[RouteIndex {
                            group_index: 0,
                            route_index: 0,
                        }];
                        assert_eq!(route.is_available(), false);
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
                    .map(|result| {
                        assert_eq!(result.unwrap(), *testing::FULFILL);
                    })
            });
    }

    #[test]
    fn test_no_route_exists() {
        let expect_reject = ilp::RejectBuilder {
            code: ilp::ErrorCode::F02_UNREACHABLE,
            message: b"no route exists",
            triggered_by: Some(ADDRESS),
            data: b"",
        }.build();
        let router = RouterService::new(
            CLIENT.clone(),
            RoutingTable::new(vec![ROUTES[1].clone()], RoutingPartition::default()),
        );
        testing::MockServer::new().run({
            router
                .call(testing::PREPARE.clone())
                .map(move |result| {
                    assert_eq!(result.unwrap_err(), expect_reject);
                })
        });
    }

    #[test]
    fn test_set_routes() {
        let router = ROUTER.clone();
        router.set_routes(RoutingTable::new(vec![
            StaticRoute::new(
                Bytes::from("test.alice."),
                NextHop::Bilateral {
                    endpoint: format!("{}/new_alice", RECEIVER_ORIGIN).parse::<Uri>().unwrap(),
                    auth: None,
                },
            ),
        ], RoutingPartition::default()));
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
                    .map(|result| {
                        assert_eq!(result.unwrap(), *testing::FULFILL);
                    })
            });
    }
}
