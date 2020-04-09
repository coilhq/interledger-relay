use std::sync::Arc;

use futures::future::{Either, Ready, err, ok};
use log::warn;

use crate::{Relation, Request, Service};
use ilp::ildcp;
use super::RequestWithFrom;

#[derive(Clone, Debug)]
pub struct ConfigService<S> {
    config: Arc<ildcp::Response>,
    next: S,
}

pub trait RequestWithPeerName: Request {
    /// The value of the `ILP-Peer-Name` header.
    fn peer_name(&self) -> Option<&[u8]>;
}

impl<S> ConfigService<S> {
    pub fn new(config: ildcp::Response, next: S) -> Self {
        ConfigService {
            config: Arc::new(config),
            next,
        }
    }

    fn make_reject(&self, code: ilp::ErrorCode, message: &[u8]) -> ilp::Reject {
        ilp::RejectBuilder {
            code,
            message,
            triggered_by: Some(self.config.client_address()),
            data: &[],
        }.build()
    }
}

impl<S, Req> Service<Req> for ConfigService<S>
where
    S: Service<Req>,
    Req: RequestWithPeerName + RequestWithFrom,
{
    type Future = Either<
        Ready<Result<ilp::Fulfill, ilp::Reject>>,
        S::Future,
    >;

    fn call(self, request: Req) -> Self::Future {
        let prepare = request.borrow();
        if prepare.destination() != ildcp::DESTINATION {
            return Either::Right(self.next.call(request));
        }

        // TODO use matches!() macro
        match request.from_relation() {
            Relation::Child => {},
            _ => {
                warn!(
                    "ildcp request from non-child peer: relation={:?} from_address={:?}",
                    request.from_relation(), request.from_address(),
                );
                return Either::Left(err(self.make_reject(
                    ilp::ErrorCode::F00_BAD_REQUEST,
                    b"ILDCP request from non-child peer",
                )))
            },
        }

        let peer_name = match request.peer_name() {
            Some(peer_name) => peer_name,
            None => {
                warn!(
                    "ildcp request missing ILP-Peer-Name: from_address={:?}",
                    request.from_address(),
                );
                return Either::Left(err(self.make_reject(
                    ilp::ErrorCode::F00_BAD_REQUEST,
                    b"Missing ILP-Peer-Name header",
                )))
            },
        };

        // If the generated address is invalid it is probably too long or the
        // `ILP-Peer-Name` was invalid.
        let client_address = request.from_address().with_suffix(peer_name);
        let client_address = match client_address {
            Ok(addr) => addr,
            Err(_) => return Either::Left(err(self.make_reject(
                ilp::ErrorCode::F00_BAD_REQUEST,
                b"Invalid generated client address",
            ))),
        };

        debug_assert!({
            AsRef::<[u8]>::as_ref(&client_address)
                .starts_with(self.config.client_address().as_ref())
        });

        Either::Left(ok(ildcp::ResponseBuilder {
            client_address: client_address.as_addr(),
            asset_scale: self.config.asset_scale(),
            asset_code: self.config.asset_code(),
        }.build().into()))
    }
}

#[cfg(test)]
mod test_config_service {
    use std::borrow::Borrow;

    use futures::executor::block_on;
    use lazy_static::lazy_static;

    use crate::testing::{FULFILL, MockService, PREPARE};
    use super::*;

    static ILDCP_RESPONSE: ildcp::ResponseBuilder<'static> =
        ildcp::ResponseBuilder {
            client_address: unsafe { ilp::Addr::new_unchecked(b"test.carl") },
            asset_scale: 9,
            asset_code: b"XRP",
        };

    lazy_static! {
        static ref CONFIG: ConfigService<MockService<TestRequest>> =
            ConfigService::new(
                ILDCP_RESPONSE.build(),
                MockService::new(Ok(FULFILL.clone())),
            );

        static ref REQUEST_PREPARE: TestRequest = TestRequest {
            prepare: PREPARE.clone(),
            peer_name: None,
            from_relation: Relation::Child,
            from_address: ilp::Address::new(b"test.carl.child.123"),
        };

        static ref REQUEST_ILDCP: TestRequest = TestRequest {
            prepare: ilp::Prepare::from(ildcp::Request::new()),
            peer_name: Some(b"bob"),
            from_relation: Relation::Child,
            from_address:  ilp::Address::new(b"test.carl.child.123"),
        };
    }

    #[test]
    fn test_passthrough() {
        assert_eq!(
            block_on(CONFIG.clone().call(REQUEST_PREPARE.clone()))
                .unwrap(),
            *FULFILL,
        );
    }

    #[test]
    fn test_ildcp_missing_peer_name() {
        let request = {
            let mut request = REQUEST_ILDCP.clone();
            request.peer_name = None;
            request
        };
        assert_eq!(
            block_on(CONFIG.clone().call(request))
                .unwrap_err()
                .code(),
            ilp::ErrorCode::F00_BAD_REQUEST,
        );
    }

    #[test]
    fn test_ildcp_from_parent() {
        let request = {
            let mut request = REQUEST_ILDCP.clone();
            request.from_relation = Relation::Parent;
            request
        };
        assert_eq!(
            block_on(CONFIG.clone().call(request))
                .unwrap_err()
                .code(),
            ilp::ErrorCode::F00_BAD_REQUEST,
        );
    }

    #[test]
    fn test_ildcp_response() {
        let fulfill = block_on({
            CONFIG.clone().call(REQUEST_ILDCP.clone())
        }).unwrap();
        let response = ildcp::Response::try_from(fulfill).unwrap();
        assert_eq!(
            response.client_address(),
            ilp::Addr::new(b"test.carl.child.123.bob"),
        );
        assert_eq!(response.asset_scale(), 9);
        assert_eq!(response.asset_code(), b"XRP");
    }

    #[derive(Clone, Debug)]
    struct TestRequest {
        prepare: ilp::Prepare,
        peer_name: Option<&'static [u8]>,
        from_relation: Relation,
        from_address: ilp::Address,
    }

    impl Request for TestRequest {}

    impl Borrow<ilp::Prepare> for TestRequest {
        fn borrow(&self) -> &ilp::Prepare {
            &self.prepare
        }
    }

    impl Into<ilp::Prepare> for TestRequest {
        fn into(self) -> ilp::Prepare {
            self.prepare
        }
    }

    impl RequestWithPeerName for TestRequest {
        fn peer_name(&self) -> Option<&[u8]> {
            self.peer_name
        }
    }

    impl RequestWithFrom for TestRequest {
        fn from_relation(&self) -> Relation {
            self.from_relation
        }

        fn from_address(&self) -> ilp::Addr {
            self.from_address.as_addr()
        }
    }
}
