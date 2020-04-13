mod config;

use std::time;

use serde::Deserialize;

pub use self::config::{ConnectorRoot, RelationConfig, SetupError};
use crate::{Client, RoutingTable, StaticRoute};
use crate::middlewares::{AuthTokenFilter, HealthCheckFilter, MethodFilter, Receiver};
use crate::services::{ConfigService, DebugService, DebugServiceOptions, EchoService};
use crate::services::{ExpiryService, FromPeerService, RouterService};
use ilp::ildcp;

/// The maximum duration that the outgoing HTTP client will wait for a response,
/// even if the Prepare's expiry is longer.
const DEFAULT_MAX_TIMEOUT: time::Duration = time::Duration::from_secs(60);

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub root: ConnectorRoot,
    pub peers: Vec<RelationConfig>,
    pub routes: Vec<StaticRoute>,
    #[serde(default)]
    pub debug_service: DebugServiceOptions,
}

// TODO This should be an existential type once they are stable.
pub type Connector =
    // HTTP Middlewares:
    HealthCheckFilter<MethodFilter<AuthTokenFilter<
        Receiver<
            // ILP Services:
            DebugService<ExpiryService<FromPeerService<ConfigService<EchoService<
                RouterService,
            >>>>>,
        >,
    >>>;

impl Config {
    pub async fn start(self) -> Result<Connector, SetupError> {
        let ildcp = self.root.load_config().await?;
        self.start_with_ildcp(ildcp)
    }

    // Used by benchmarks.
    #[doc(hidden)]
    pub fn start_with_ildcp(self, ildcp: ildcp::Response)
        -> Result<Connector, SetupError>
    {
        let address = ildcp.client_address().to_address();
        let auth_tokens = self.peers
            .iter()
            .flat_map(|peer| peer.auth_tokens().iter())
            .cloned();
        let peers = self.peers
            .iter()
            .map(|peer| {
                peer.with_parent(&address)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let client = Client::new(address.clone());
        let router_svc = RouterService::new(client, RoutingTable::new(self.routes));
        let echo_svc = EchoService::new(address.clone(), router_svc);
        let ildcp_svc = ConfigService::new(ildcp, echo_svc);
        let from_peer_svc =
            FromPeerService::new(address.clone(), peers, ildcp_svc);
        let expiry_svc =
            ExpiryService::new(address, DEFAULT_MAX_TIMEOUT, from_peer_svc);
        let debug_svc =
            DebugService::new("packet", self.debug_service, expiry_svc);

        let receiver = Receiver::new(debug_svc);
        let auth_filter = AuthTokenFilter::new(auth_tokens, receiver);
        let method_filter = MethodFilter::new(hyper::Method::POST, auth_filter);
        Ok(HealthCheckFilter::new(method_filter))
    }
}

#[cfg(test)]
mod test_config {
    use futures::prelude::*;
    use hyper::service::Service;
    use lazy_static::lazy_static;

    use crate::AuthToken;
    use crate::combinators;
    use crate::testing::{self, FULFILL, PREPARE};
    use super::*;

    static CONNECTOR_ADDR: ([u8; 4], u16) = ([127, 0, 0, 1], 3002);

    lazy_static! {
        static ref PEERS: Vec<RelationConfig> = vec![
            RelationConfig::Child {
                auth: vec![AuthToken::new("secret_child")],
                suffix: "child".to_owned(),
            },
            RelationConfig::Parent {
                auth: vec![AuthToken::new("secret_parent")],
            },
        ];
    }

    #[test]
    fn test_static() {
        let connector = Config {
            root: ConnectorRoot::Static {
                address: ilp::Address::new(b"example.alice"),
                asset_scale: 9,
                asset_code: "XRP".to_owned(),
            },
            peers: PEERS.clone(),
            routes: testing::ROUTES.clone(),
            debug_service: DebugServiceOptions::default(),
        };

        let future = connector
            .start()
            .then(|connector_result| {
                connector_result.unwrap().call({
                    hyper::Request::post("http://127.0.0.1:3002/ilp")
                        .header("Authorization", "secret_child")
                        .body(hyper::Body::from(PREPARE.as_ref()))
                        .unwrap()
                })
            })
            .map(|response| {
                assert_eq!(response.unwrap().status(), 200);
            });

        testing::MockServer::new()
            .test_request(|req| {
                assert_eq!(req.method(), hyper::Method::POST);
                assert_eq!(req.uri().path(), "/alice");
            })
            .test_body(|body| {
                assert_eq!(body.as_ref(), PREPARE.as_ref());
            })
            .with_response(|| {
                hyper::Response::builder()
                    .status(200)
                    .body(hyper::Body::from(FULFILL.as_ref()))
                    .unwrap()
            })
            .run(future);
    }

/*
    #[test]
    fn test_dynamic() {
        let connector = ConnectorBuilder {
            root: ConnectorRoot::Dynamic {
                parent_endpoint: format!("{}/bob", testing::RECEIVER_ORIGIN),
                parent_auth: b"receiver_secret".to_vec(),
                name: b"carl".to_vec(),
            },
            auth_tokens: vec![AuthToken::new(b"secret".to_vec())],
            routes: testing::ROUTES.clone(),
        };

        let future = connector.build()
            .map_err(|err| panic!(err))
            .and_then(|mut connector| {
                connector.call({
                    hyper::Request::post("http://127.0.0.1:3002/ilp")
                        .header("Authorization", "secret")
                        .body(hyper::Body::from(PREPARE.as_bytes()))
                        .unwrap()
                })
            })
            .map_err(|err| panic!(err))
            .map(|response| {
                assert_eq!(response.status(), 200);
            });

        testing::MockServer::new()
            .test_request(|req| {
                assert_eq!(req.method(), hyper::Method::POST);
                assert_eq!(req.uri().path(), "/alice");
            })
            .test_body(|body| {
                assert_eq!(body.as_ref(), PREPARE.as_bytes());
            })
            .with_response(|| {
                hyper::Response::builder()
                    .status(200)
                    .body(hyper::Body::from(FULFILL.as_bytes()))
                    .unwrap()
            })
            .run(future);
    }
*/

    // TODO maybe add an actual integration test using stream, and remove this one
    #[test]
    fn test_integration() {
        let start_connector = Config {
            root: ConnectorRoot::Static {
                address: ilp::Address::new(b"example.alice"),
                asset_scale: 9,
                asset_code: "XRP".to_owned(),
            },
            peers: PEERS.clone(),
            routes: testing::ROUTES.clone(),
            debug_service: DebugServiceOptions::default(),
        }.start();

        let request = hyper::Client::new()
            .request({
                hyper::Request::post("http://127.0.0.1:3002/ilp")
                    .header("Authorization", "secret_child")
                    .body(hyper::Body::from(PREPARE.as_ref()))
                    .unwrap()
            })
            .then(|response_result| {
                let response = response_result.unwrap();
                assert_eq!(response.status(), 200);
                combinators::collect_http_response(response)
            })
            .map(|body_result| {
                let body = body_result.unwrap();
                assert_eq!(body.as_ref(), FULFILL.as_ref());
            });

        let start_server = start_connector.then(|connector_result| {
            let connector = connector_result.unwrap();
            hyper::Server::bind(&CONNECTOR_ADDR.into())
                .serve(hyper::service::make_service_fn(move |_socket| {
                    future::ok::<_, std::convert::Infallible>(connector.clone())
                }))
                .with_graceful_shutdown(request)
                .map(|result| { result.unwrap(); })
        });

        testing::MockServer::new()
            .test_request(|req| {
                assert_eq!(req.method(), hyper::Method::POST);
                assert_eq!(req.uri().path(), "/alice");
            })
            .test_body(|body| {
                assert_eq!(body.as_ref(), PREPARE.as_ref());
            })
            .with_response(|| {
                hyper::Response::builder()
                    .status(200)
                    .body(hyper::Body::from(FULFILL.as_ref()))
                    .unwrap()
            })
            .run(start_server);
    }
}
