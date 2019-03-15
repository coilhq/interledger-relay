use std::borrow::Borrow;
use std::error;
use std::fmt;
use std::time;

use futures::future::{Either, ok};
use futures::prelude::*;
use hyper::Uri;
use serde::Deserialize;

use crate::{AuthToken, Client, Route};
use crate::middlewares::{AuthTokenFilter, HealthCheckFilter, MethodFilter, Receiver};
use crate::serde::deserialize_uri;
use crate::services::{ConfigService, ExpiryService, RouterService};
use ilp::ildcp;

/// The maximum duration that the outgoing HTTP client will wait for a response,
/// even if the Prepare's expiry is longer.
const DEFAULT_MAX_TIMEOUT: time::Duration = time::Duration::from_secs(60);

#[derive(Debug, PartialEq, Deserialize)]
pub struct Config {
    pub root: ConnectorRoot,
    /// Incoming requests must present one of these tokens in the
    /// `Authorization` header.
    pub auth_tokens: Vec<AuthToken>,
    pub routes: Vec<Route>,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(tag = "type")]
pub enum ConnectorRoot {
    Static {
        address: ilp::Address,
        asset_scale: u8,
        asset_code: String,
    },
    Dynamic {
        #[serde(deserialize_with = "deserialize_uri")]
        parent_endpoint: Uri,
        parent_auth: AuthToken,
        // TODO should "name" be optional?
        name: String,
    },
}

// TODO This should be an existential type once they are stable.
pub type Connector =
    HealthCheckFilter<MethodFilter<AuthTokenFilter<
        Receiver<
            ExpiryService<ConfigService<
                RouterService,
            >>,
        >,
    >>>;

impl Config {
    pub fn start(self) -> impl Future<Item = Connector, Error = SetupError> {
        self.root.load_config().map(move |ildcp| {
            let address = ildcp.client_address().to_address();

            let client = Client::new(address.clone());
            let router_svc = RouterService::new(client, self.routes);
            let ildcp_svc = ConfigService::new(ildcp, router_svc);
            let expiry_svc =
                ExpiryService::new(address, DEFAULT_MAX_TIMEOUT, ildcp_svc);

            let receiver = Receiver::new(expiry_svc);
            let auth_filter = AuthTokenFilter::new(self.auth_tokens, receiver);
            let method_filter = MethodFilter::new(hyper::Method::POST, auth_filter);
            HealthCheckFilter::new(method_filter)
        })
    }
}

impl ConnectorRoot {
    fn load_config(&self)
        -> impl Future<Item = ildcp::Response, Error = SetupError>
    {
        match self {
            ConnectorRoot::Static {
                address,
                asset_code,
                asset_scale,
            } => Either::A(ok(ildcp::ResponseBuilder {
                client_address: address.as_addr(),
                asset_code: asset_code.as_bytes(),
                asset_scale: *asset_scale,
            }.build())),
            ConnectorRoot::Dynamic {
                parent_endpoint,
                parent_auth,
                name,
            } => Either::B(fetch_ildcp(
                parent_endpoint,
                parent_auth.borrow(),
                name.as_bytes(),
            )),
        }
    }
}

fn fetch_ildcp(endpoint: &Uri, auth: &[u8], peer_name: &[u8])
    -> impl Future<Item = ildcp::Response, Error = SetupError>
{
    let mut request = hyper::Request::builder();
    request.method(hyper::Method::POST);
    request.uri(endpoint);
    request.header(hyper::header::AUTHORIZATION, auth);
    request.header("ILP-Peer-Name", peer_name);
    let prepare = ildcp::Request::new().to_prepare();

    // Use a dummy address as the sender since the connector doesn't know its
    // address yet.
    Client::new(ilp::Address::new(b"self.ildcp"))
        .request(request, prepare)
        .from_err()
        .and_then(|fulfill| {
            ildcp::Response::try_from(fulfill)
                .into_future()
                .from_err()
        })
}

#[derive(Debug)]
pub struct SetupError(ErrorKind);

#[derive(Debug)]
enum ErrorKind {
    ParseError(ilp::ParseError),
    Reject(ilp::Reject),
}

impl error::Error for SetupError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match &self.0 {
            ErrorKind::ParseError(inner) => Some(inner),
            ErrorKind::Reject(_) => None,
        }
    }
}

impl fmt::Display for SetupError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.0 {
            ErrorKind::ParseError(inner) => write!(f, "SetupError({})", inner),
            ErrorKind::Reject(reject) => write!(f, "SetupError({:?})", reject),
        }
    }
}

impl From<ilp::ParseError> for SetupError {
    fn from(inner: ilp::ParseError) -> Self {
        SetupError(ErrorKind::ParseError(inner))
    }
}

impl From<ilp::Reject> for SetupError {
    fn from(reject: ilp::Reject) -> Self {
        SetupError(ErrorKind::Reject(reject))
    }
}

#[cfg(test)]
mod test_config {
    use hyper::service::Service;

    use crate::testing::{self, FULFILL, PREPARE};
    use super::*;

    static CONNECTOR_ADDR: ([u8; 4], u16) = ([127, 0, 0, 1], 3002);

    #[test]
    fn test_static() {
        let connector = Config {
            root: ConnectorRoot::Static {
                address: ilp::Address::new(b"example.alice"),
                asset_scale: 9,
                asset_code: "XRP".to_owned(),
            },
            auth_tokens: vec![AuthToken::new("secret")],
            routes: testing::ROUTES.clone(),
        };

        let future = connector
            .start()
            .map_err(|err| panic!(err))
            .and_then(|mut connector| {
                connector.call({
                    hyper::Request::post("http://127.0.0.1:3002/ilp")
                        .header("Authorization", "secret")
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
            auth_tokens: vec![AuthToken::new("secret")],
            routes: testing::ROUTES.clone(),
        }.start();

        let request = hyper::Client::new()
            .request({
                hyper::Request::post("http://127.0.0.1:3002/ilp")
                    .header("Authorization", "secret")
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

#[cfg(test)]
mod test_connector_root {
    use bytes::{Bytes, BytesMut};

    use crate::testing::{self, RECEIVER_ORIGIN};
    use super::*;

    #[test]
    fn test_static() {
        let root = ConnectorRoot::Static {
            address: ilp::Address::new(b"test.alice"),
            asset_scale: 9,
            asset_code: "XRP".to_owned(),
        };
        assert_eq!(
            root.load_config().wait().unwrap(),
            ildcp::ResponseBuilder {
                client_address: ilp::Addr::new(b"test.alice"),
                asset_scale: 9,
                asset_code: b"XRP",
            }.build(),
        );
    }

    #[test]
    fn test_dynamic() {
        let root = ConnectorRoot::Dynamic {
            parent_endpoint: RECEIVER_ORIGIN.parse().unwrap(),
            parent_auth: AuthToken::new("parent_secret"),
            name: "carl".to_owned(),
        };

        static PARENT_RESPONSE: ildcp::ResponseBuilder<'static> =
            ildcp::ResponseBuilder {
                client_address: unsafe {
                    ilp::Addr::new_unchecked(b"test.parent.carl")
                },
                asset_scale: 9,
                asset_code: b"XRP",
            };

        let load_config = root.load_config()
            .map(|response| {
                assert_eq!(response, PARENT_RESPONSE.build());
            });

        testing::MockServer::new()
            .test_request(|req| {
                assert_eq!(req.method(), hyper::Method::POST);
                assert_eq!(
                    req.headers().get("Authorization").unwrap(),
                    "parent_secret",
                );
                assert_eq!(
                    req.headers().get("ILP-Peer-Name").unwrap(),
                    "carl",
                );
            })
            .test_body(|body| {
                let body = Bytes::from(body);
                let body = BytesMut::from(body);
                let prepare = ilp::Prepare::try_from(body).unwrap();
                ildcp::Request::try_from(prepare)
                    .expect("invalid ildcp request");
            })
            .with_response(|| {
                let response = PARENT_RESPONSE.build();
                let fulfill = ilp::Fulfill::from(response);
                let response = BytesMut::from(fulfill);
                hyper::Response::builder()
                    .status(200)
                    .body(hyper::Body::from(response.freeze()))
                    .unwrap()
            })
            .run(load_config);
    }
}
