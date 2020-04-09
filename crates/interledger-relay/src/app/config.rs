use std::collections::HashSet;
use std::error;
use std::fmt;

use bytes::{Bytes, BytesMut};
use futures::future::{Either, ok};
use futures::prelude::*;
use hyper::Uri;
use serde::Deserialize;

use crate::{AuthToken, Client, Relation};
use crate::client::RequestOptions;
use crate::serde::deserialize_uri;
use crate::services::ConnectorPeer;
use ilp::ildcp;

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// The `auth` token lists are valid incoming authentication tokens.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "type")]
pub enum RelationConfig {
    Child {
        auth: Vec<AuthToken>,
        /// The suffix must be an ILP address segment.
        suffix: String,
    },
    Peer {
        auth: Vec<AuthToken>,
    },
    Parent {
        auth: Vec<AuthToken>,
    },
}

impl ConnectorRoot {
    pub(crate) fn load_config(&self)
        -> impl Future<Output = Result<ildcp::Response, SetupError>>
    {
        match self {
            ConnectorRoot::Static {
                address,
                asset_code,
                asset_scale,
            } => Either::Left(ok(ildcp::ResponseBuilder {
                client_address: address.as_addr(),
                asset_code: asset_code.as_bytes(),
                asset_scale: *asset_scale,
            }.build())),
            ConnectorRoot::Dynamic {
                parent_endpoint,
                parent_auth,
                name,
            } => Either::Right(fetch_ildcp(
                parent_endpoint,
                parent_auth.as_bytes(),
                name.as_bytes(),
            )),
        }
    }
}

fn fetch_ildcp(endpoint: &Uri, auth: Bytes, peer_name: &[u8])
    -> impl Future<Output = Result<ildcp::Response, SetupError>>
{
    let prepare = ildcp::Request::new().to_prepare();

    // Use a dummy address as the sender since the connector doesn't know its
    // address yet.
    Client::new(ilp::Address::new(b"self.ildcp"))
        .request(RequestOptions {
            method: hyper::Method::POST,
            uri: endpoint.clone(),
            auth: Some(auth),
            peer_name: Some(BytesMut::from(peer_name).freeze()),
        }, prepare)
        .err_into()
        .and_then(|fulfill| {
            future::ready(ildcp::Response::try_from(fulfill))
                .err_into()
        })
}

impl RelationConfig {
    fn relation(&self) -> Relation {
        match self {
            RelationConfig::Child { .. } => Relation::Child,
            RelationConfig::Peer { .. } => Relation::Peer,
            RelationConfig::Parent { .. } =>  Relation::Parent,
        }
    }

    pub(crate) fn auth_tokens(&self) -> &[AuthToken] {
        match self {
            RelationConfig::Child { auth, .. } => auth,
            RelationConfig::Peer { auth, .. } => auth,
            RelationConfig::Parent { auth, .. } => auth,
        }
    }

    pub(crate) fn with_parent(&self, parent_address: &ilp::Address)
        -> Result<ConnectorPeer, SetupError>
    {
        let address = match self {
            RelationConfig::Child { suffix, .. } => {
                parent_address.with_suffix(suffix.as_bytes())?
            },
            // This is the wrong address for the peer, but we don't know their address.
            // It's only ever used in logging.
            RelationConfig::Peer { .. } => parent_address.clone(),
            RelationConfig::Parent { .. } => parent_address.clone(),
        };

        Ok(ConnectorPeer {
            relation: self.relation(),
            address,
            auth: self
                .auth_tokens()
                .iter()
                .cloned()
                .collect::<HashSet<_>>(),
        })
    }
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

impl From<ilp::AddressError> for SetupError {
    fn from(inner: ilp::AddressError) -> Self {
        SetupError(ErrorKind::ParseError(inner.into()))
    }
}

impl From<ilp::Reject> for SetupError {
    fn from(reject: ilp::Reject) -> Self {
        SetupError(ErrorKind::Reject(reject))
    }
}

#[cfg(test)]
mod test_connector_root {
    use bytes::BytesMut;

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
            futures::executor::block_on(root.load_config()).unwrap(),
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
            .map(|response_result| {
                let response = response_result.unwrap();
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
                let body = BytesMut::from(body.as_ref());
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
