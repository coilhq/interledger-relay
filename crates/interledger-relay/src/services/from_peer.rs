use std::borrow::Borrow;
use std::collections::HashSet;
use std::sync::Arc;

use futures::future::{Either, Ready, err};
use log::error;

use crate::{AuthToken, Relation, Request, Service};
use crate::middlewares::RequestWithHeaders;
use crate::services::RequestWithPeerName;

/// Use the incoming `Authorization` header to tag requests with their peer's
/// address.
#[derive(Clone, Debug)]
pub struct FromPeerService<S> {
    address: ilp::Address,
    peers: Arc<Vec<ConnectorPeer>>,
    next: S,
}

pub trait RequestWithFrom: Request {
    //fn from_account(&self) -> &Arc<String>;
    fn from_relation(&self) -> Relation;
    fn from_address(&self) -> ilp::Addr;
}

impl<S> FromPeerService<S> {
    pub fn new(
        address: ilp::Address,
        peers: Vec<ConnectorPeer>,
        next: S,
    ) -> Self {
        FromPeerService {
            address,
            peers: Arc::new(peers),
            next,
        }
    }
}

impl<S> Service<RequestWithHeaders> for FromPeerService<S>
where
    S: Service<RequestFromPeer>,
{
    type Future = Either<
        S::Future,
        Ready<Result<ilp::Fulfill, ilp::Reject>>,
    >;

    fn call(self, req: RequestWithHeaders) -> Self::Future {
        let auth = req.header(hyper::header::AUTHORIZATION);
        let peer = self.peers
            .iter()
            .find(|peer| {
                match auth {
                    Some(auth) => peer.is_authorized(auth),
                    None => false,
                }
            });

        // The auth middleware has already been run, so a peer should always be
        // found. Check just to be safe.
        let peer = match peer {
            Some(peer) => peer,
            None => {
                error!("could not determine packet source: auth={:?}", auth);
                return Either::Right(err(ilp::RejectBuilder {
                    code: ilp::ErrorCode::F00_BAD_REQUEST,
                    message: b"could not determine packet source",
                    triggered_by: Some(self.address.as_addr()),
                    data: &[],
                }.build()))
            },
        };

        Either::Left(self.next.call(RequestFromPeer {
            base: req,
            from_relation: peer.relation,
            from_address: peer.address.clone(),
        }))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RequestFromPeer {
    base: RequestWithHeaders,
    from_relation: Relation,
    from_address: ilp::Address,
}

impl Request for RequestFromPeer {}

impl Into<ilp::Prepare> for RequestFromPeer {
    fn into(self) -> ilp::Prepare {
        self.base.into()
    }
}

impl Borrow<ilp::Prepare> for RequestFromPeer {
    fn borrow(&self) -> &ilp::Prepare {
        self.base.borrow()
    }
}

impl RequestWithPeerName for RequestFromPeer {
    fn peer_name(&self) -> Option<&[u8]> {
        self.base.peer_name()
    }
}

impl RequestWithFrom for RequestFromPeer {
    //fn from_account(&self) -> &std::rc::Rc<String> {
    //    self.from_account
    //}

    fn from_relation(&self) -> Relation {
        self.from_relation
    }

    fn from_address(&self) -> ilp::Addr {
        self.from_address.as_addr()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConnectorPeer {
    pub relation: Relation,
    pub address: ilp::Address,
    /// The list of valid incoming authentication tokens.
    pub auth: HashSet<AuthToken>,
}

impl ConnectorPeer {
    fn is_authorized(&self, token: &[u8]) -> bool {
        static BEARER_PREFIX: &[u8] = b"Bearer ";
        let token = if token.starts_with(BEARER_PREFIX) {
            &token[BEARER_PREFIX.len()..]
        } else {
            token
        };
        self.auth.contains(token)
    }
}

#[cfg(test)]
mod test_from_peer_service {
    use std::iter::FromIterator;

    use futures::executor::block_on;
    use hyper::HeaderMap;
    use lazy_static::lazy_static;

    use crate::testing::{FULFILL, PREPARE, MockService, PanicService};
    use super::*;

    lazy_static! {
        static ref PEERS: Vec<ConnectorPeer> = vec![
            ConnectorPeer {
                relation: Relation::Child,
                address: ilp::Address::new(b"test.relay.child"),
                auth: HashSet::from_iter(vec![AuthToken::new("token_1")]),
            },
            ConnectorPeer {
                relation: Relation::Parent,
                address: ilp::Address::new(b"test.relay"),
                auth: HashSet::from_iter(vec![AuthToken::new("token_2")]),
            },
        ];
    }

    #[test]
    fn test_peer_not_found() {
        let service = FromPeerService::new(
            ilp::Address::new(b"test.relay"),
            PEERS.clone(),
            PanicService,
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            hyper::header::AUTHORIZATION,
            "invalid_token".parse().unwrap(),
        );

        let reject = block_on({
            service.call(RequestWithHeaders::new(PREPARE.clone(), headers))
        }).unwrap_err();
        assert_eq!(reject.code(), ilp::ErrorCode::F00_BAD_REQUEST);
        assert_eq!(reject.message(), &b"could not determine packet source"[..]);
    }

    #[test]
    fn test_peer_found() {
        let next = MockService::new(Ok(FULFILL.clone()));
        let service = FromPeerService::new(
            ilp::Address::new(b"test.relay"),
            PEERS.clone(),
            next.clone(),
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            hyper::header::AUTHORIZATION,
            "token_1".parse().unwrap(),
        );

        let fulfill = block_on({
            service.call(RequestWithHeaders::new(PREPARE.clone(), headers.clone()))
        }).unwrap();
        assert_eq!(fulfill, *FULFILL);

        assert_eq!(
            next.requests().collect::<Vec<_>>(),
            vec![RequestFromPeer {
                base: RequestWithHeaders::new(PREPARE.clone(), headers),
                from_relation: Relation::Child,
                from_address: ilp::Address::new(b"test.relay.child"),
            }],
        );
    }
}

#[cfg(test)]
mod test_connector_peer {
    use super::*;

    static TOKENS: &'static [&'static str] = &["token_1", "token_2"];

    #[test]
    fn test_is_authorized() {
        let peer = ConnectorPeer {
            relation: Relation::Child,
            address: ilp::Address::new(b"test.relay"),
            auth: TOKENS
                .iter()
                .cloned()
                .map(AuthToken::new)
                .collect::<HashSet<_>>(),
        };
        assert_eq!(peer.is_authorized(b"token_1"), true);
        assert_eq!(peer.is_authorized(b"token_2"), true);
        assert_eq!(peer.is_authorized(b"token_3"), false);
        assert_eq!(peer.is_authorized(b"Bearer token_1"), true);
        assert_eq!(peer.is_authorized(b"Bearer token_2"), true);
        assert_eq!(peer.is_authorized(b"Bearer token_3"), false);
    }
}
