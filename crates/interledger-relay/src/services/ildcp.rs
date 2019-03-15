use std::sync::Arc;

use futures::future::{Either, FutureResult, err, ok};

use crate::{Request, Service};
use ilp::ildcp;

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
            triggered_by: self.config.client_address(),
            data: &[],
        }.build()
    }
}

impl<S, Req> Service<Req> for ConfigService<S>
where
    S: Service<Req>,
    Req: RequestWithPeerName,
{
    type Future = Either<
        FutureResult<ilp::Fulfill, ilp::Reject>,
        S::Future,
    >;

    fn call(self, request: Req) -> Self::Future {
        let prepare = request.borrow();
        if prepare.destination() != ildcp::DESTINATION {
            return Either::B(self.next.call(request));
        }

        let peer_name = match request.peer_name() {
            Some(peer_name) => peer_name,
            None => return Either::A(err(self.make_reject(
                ilp::ErrorCode::F00_BAD_REQUEST,
                b"Missing ILP-Peer-Name header",
            ))),
        };

        let client_address = self.config.client_address();
        let mut client_address = client_address.as_ref().to_vec();
        client_address.push(b'.');
        client_address.extend_from_slice(peer_name);
        // If the generated address is invalid it is probably too long.
        let client_address = match ilp::Addr::try_from(&client_address) {
            Ok(addr) => addr,
            Err(_) => return Either::A(err(self.make_reject(
                ilp::ErrorCode::F00_BAD_REQUEST,
                b"Invalid parent address",
            ))),
        };

        Either::A(ok(ildcp::ResponseBuilder {
            client_address,
            asset_scale: self.config.asset_scale(),
            asset_code: self.config.asset_code(),
        }.build().into()))
    }
}

#[cfg(test)]
mod test_config_service {
    use std::borrow::Borrow;

    use futures::prelude::*;
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
        static ref CONFIG: ConfigService<MockService> = ConfigService::new(
            ILDCP_RESPONSE.build(),
            MockService::new(Ok(FULFILL.clone())),
        );
    }

    #[test]
    fn test_passthrough() {
        let request = TestRequest(PREPARE.clone(), None);
        assert_eq!(CONFIG.clone().call(request).wait().unwrap(), *FULFILL);
    }

    #[test]
    fn test_missing_peer_name() {
        let prepare = ilp::Prepare::from(ildcp::Request::new());
        let request = TestRequest(prepare, None);
        assert_eq!(
            CONFIG.clone().call(request).wait().unwrap_err().code(),
            ilp::ErrorCode::F00_BAD_REQUEST,
        );
    }

    #[test]
    fn test_ildcp_response() {
        let prepare = ilp::Prepare::from(ildcp::Request::new());
        let request = TestRequest(prepare, Some(b"bob"));
        let fulfill = CONFIG.clone().call(request).wait().unwrap();
        let response = ildcp::Response::try_from(fulfill).unwrap();
        assert_eq!(response.client_address(), ilp::Addr::new(b"test.carl.bob"));
        assert_eq!(response.asset_scale(), 9);
        assert_eq!(response.asset_code(), b"XRP");
    }

    #[derive(Debug)]
    struct TestRequest(ilp::Prepare, Option<&'static [u8]>);
    impl Request for TestRequest {}

    impl Borrow<ilp::Prepare> for TestRequest {
        fn borrow(&self) -> &ilp::Prepare {
            &self.0
        }
    }

    impl Into<ilp::Prepare> for TestRequest {
        fn into(self) -> ilp::Prepare {
            self.0
        }
    }

    impl RequestWithPeerName for TestRequest {
        fn peer_name(&self) -> Option<&[u8]> {
            self.1
        }
    }
}
