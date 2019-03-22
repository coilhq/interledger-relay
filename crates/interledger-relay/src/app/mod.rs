mod config;

use std::time;

use futures::prelude::*;
use serde::Deserialize;

pub use self::config::{ConnectorRoot, PeerConfig, SetupError};
use crate::{Client, Route};
use crate::middlewares::{AuthTokenFilter, HealthCheckFilter, MethodFilter, Receiver};
use crate::services::{ConfigService, ExpiryService, FromPeerService, RouterService};

/// The maximum duration that the outgoing HTTP client will wait for a response,
/// even if the Prepare's expiry is longer.
const DEFAULT_MAX_TIMEOUT: time::Duration = time::Duration::from_secs(60);

#[derive(Debug, PartialEq, Deserialize)]
pub struct Config {
    pub root: ConnectorRoot,
    pub peers: Vec<PeerConfig>,
    pub routes: Vec<Route>,
}

// TODO This should be an existential type once they are stable.
pub type Connector =
    // HTTP Middlewares:
    HealthCheckFilter<MethodFilter<AuthTokenFilter<
        Receiver<
            // ILP Services:
            ExpiryService<FromPeerService<ConfigService<
                RouterService,
            >>>,
        >,
    >>>;

impl Config {
    pub fn start(self) -> impl Future<Item = Connector, Error = SetupError> {
        self.root.load_config().and_then(move |ildcp| {
            let address = ildcp.client_address().to_address();
            let auth_tokens = self.peers
                .iter()
                .flat_map(|peer| peer.auth_tokens().iter())
                .map(Clone::clone);
            let peers = self.peers
                .iter()
                .map(|peer| {
                    peer.with_parent(&address)
                })
                .collect::<Result<Vec<_>, _>>()?;

            let client = Client::new(address.clone());
            let router_svc = RouterService::new(client, self.routes);
            let ildcp_svc = ConfigService::new(ildcp, router_svc);
            let from_peer_svc =
                FromPeerService::new(address.clone(), peers, ildcp_svc);
            let expiry_svc =
                ExpiryService::new(address, DEFAULT_MAX_TIMEOUT, from_peer_svc);

            let receiver = Receiver::new(expiry_svc);
            let auth_filter = AuthTokenFilter::new(auth_tokens, receiver);
            let method_filter = MethodFilter::new(hyper::Method::POST, auth_filter);
            Ok(HealthCheckFilter::new(method_filter))
        })
    }
}

#[cfg(test)]
mod test_config {
    use hyper::service::Service;
    use lazy_static::lazy_static;

    use crate::AuthToken;
    use crate::testing::{self, FULFILL, PREPARE};
    use super::*;

    static CONNECTOR_ADDR: ([u8; 4], u16) = ([127, 0, 0, 1], 3002);

    lazy_static! {
        static ref PEERS: Vec<PeerConfig> = vec![
            PeerConfig::Child {
                auth: vec![AuthToken::new("secret_child")],
                suffix: "child".to_owned(),
            },
            PeerConfig::Parent {
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
            //auth_tokens: vec![AuthToken::new("secret")],
            routes: testing::ROUTES.clone(),
        };

        let future = connector
            .start()
            .map_err(|err| panic!(err))
            .and_then(|mut connector| {
                connector.call({
                    hyper::Request::post("http://127.0.0.1:3002/ilp")
                        .header("Authorization", "secret_child")
                        .body(hyper::Body::from(PREPARE.as_ref()))
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
            //auth_tokens: vec![AuthToken::new("secret")],
            peers: PEERS.clone(),
            routes: testing::ROUTES.clone(),
        }.start();

        let request = hyper::Client::new()
            .request({
                hyper::Request::post("http://127.0.0.1:3002/ilp")
                    .header("Authorization", "secret_child")
                    .body(hyper::Body::from(PREPARE.as_ref()))
                    .unwrap()
            })
            .and_then(|response| {
                assert_eq!(response.status(), 200);
                response.into_body().concat2()
            })
            .map(|body| {
                assert_eq!(body.as_ref(), FULFILL.as_ref());
            });

        let start_server = start_connector.and_then(|connector| {
            hyper::Server::bind(&CONNECTOR_ADDR.into())
                .serve(move || -> Result<_, &'static str> {
                    Ok(connector.clone())
                })
                .with_graceful_shutdown(request)
                .map_err(|err| panic!("unexpected error: {}", err))
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
