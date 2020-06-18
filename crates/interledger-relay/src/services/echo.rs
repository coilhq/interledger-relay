use std::io;
use std::time;

use futures::future::{Either, Ready, err};

use crate::{Request, Service};
use ilp::oer::BufOerExt;

// TODO: disabled this for now. To make it work, it needs to generate a
// `RequestFromPeer` instead of an `ilp::Prepare` so that it will play nice
// with the service chain.

const MIN_MESSAGE_WINDOW: time::Duration = time::Duration::from_secs(1);

static ECHO_REQUEST_PREFIX: &[u8] = b"ECHOECHOECHOECHO\x00";
static ECHO_RESPONSE: &[u8] = b"ECHOECHOECHOECHO\x01";

#[derive(Clone, Debug)]
pub struct EchoService<S> {
    address: ilp::Address,
    next: S,
}

impl<S> EchoService<S> {
    pub fn new(address: ilp::Address, next: S) -> Self {
        EchoService { address, next }
    }
}

impl<S, Req> Service<Req> for EchoService<S>
where
    S: 'static + Service<ilp::Prepare> + Send,
    Req: Request,
{
    type Future = Either<
        Ready<Result<ilp::Fulfill, ilp::Reject>>,
        S::Future,
    >;

    fn call(self, request: Req) -> Self::Future {
        let incoming_prepare = request.borrow();
        if self.address.as_addr() != incoming_prepare.destination() {
            return Either::Right(self.next.call(request.into()));
        }

        // TODO should this be validated to prevent loops/DOS?
        let from_addr = deserialize_echo_request(incoming_prepare.data());
        let from_addr = match from_addr {
            Ok(addr) => addr,
            Err(_) => return Either::Left(err(ilp::RejectBuilder {
                code: ilp::ErrorCode::F01_INVALID_PACKET,
                message: b"invalid echo request",
                triggered_by: Some(self.address.as_addr()),
                data: &[],
            }.build())),
        };

        let execution_condition = {
            let mut cond = [0; 32];
            cond.copy_from_slice(incoming_prepare.execution_condition());
            cond
        };

        let outgoing_prepare = ilp::PrepareBuilder {
            amount: incoming_prepare.amount(),
            expires_at: incoming_prepare.expires_at() - MIN_MESSAGE_WINDOW,
            execution_condition: &execution_condition,
            destination: from_addr,
            data: ECHO_RESPONSE,
        }.build();
        Either::Right(self.next.call(outgoing_prepare))
    }
}

fn deserialize_echo_request(mut reader: &[u8])
    -> Result<ilp::Addr, ilp::ParseError>
{
    if reader.starts_with(ECHO_REQUEST_PREFIX) {
        reader.skip(ECHO_REQUEST_PREFIX.len())?;
        reader
            .peek_var_octet_string()
            .map_err(ilp::ParseError::from)
            .and_then(|addr| Ok(ilp::Addr::try_from(addr)?))
    } else {
        Err(ilp::ParseError::from(io::Error::new(
            io::ErrorKind::InvalidData,
            "not an echo request",
        )))
    }
}

#[cfg(test)]
mod test_echo_service {
    use bytes::{BufMut, BytesMut};
    use futures::executor::block_on;
    use lazy_static::lazy_static;

    use crate::testing::{ADDRESS, FULFILL, MockService, PanicService, PREPARE};
    use ilp::oer::{self, MutBufOerExt};
    use super::*;

    lazy_static! {
        static ref ECHO_PREPARE_DATA: BytesMut =
            serialize_echo_request(b"test.origin");
        static ref INVALID_ECHO_PREPARE_DATA: BytesMut =
            serialize_echo_request(b"bad.address");

        static ref ECHO_PREPARE: ilp::PrepareBuilder<'static> =
            ilp::PrepareBuilder {
                amount: PREPARE.amount(),
                expires_at: PREPARE.expires_at(),
                execution_condition: &[0x11; 32],
                destination: ADDRESS,
                data: &ECHO_PREPARE_DATA,
            };

        static ref INVALID_ECHO_PREPARE: ilp::PrepareBuilder<'static> =
            ilp::PrepareBuilder {
                data: &INVALID_ECHO_PREPARE_DATA,
                ..*ECHO_PREPARE
            };
    }

    #[test]
    fn test_passthrough() {
        let receiver = MockService::new(Ok(FULFILL.clone()));
        let echo = EchoService::new(ADDRESS.to_address(), receiver.clone());

        let fulfill = block_on(echo.call(PREPARE.clone())).unwrap();
        assert_eq!(fulfill, FULFILL.clone());
        assert_eq!(
            receiver
                .requests()
                .collect::<Vec<_>>(),
            vec![PREPARE.clone()],
        );
    }

    #[test]
    fn test_valid_echo_request() {
        let receiver = MockService::new(Ok(FULFILL.clone()));
        let echo = EchoService::new(ADDRESS.to_address(), receiver.clone());

        let fulfill = block_on(echo.call(ECHO_PREPARE.build())).unwrap();
        assert_eq!(fulfill, FULFILL.clone());

        let echo_response = receiver
            .requests()
            .next()
            .unwrap();
        assert_eq!(
            echo_response,
            ilp::PrepareBuilder {
                expires_at: ECHO_PREPARE.expires_at - MIN_MESSAGE_WINDOW,
                destination: ilp::Addr::new(b"test.origin"),
                data: ECHO_RESPONSE,
                ..*ECHO_PREPARE
            }.build(),
        );
    }

    #[test]
    fn test_invalid_echo_request() {
        let echo = EchoService::new(ADDRESS.to_address(), PanicService);
        let reject = block_on(echo.call(INVALID_ECHO_PREPARE.build())).unwrap_err();
        assert_eq!(reject.code(), ilp::ErrorCode::F01_INVALID_PACKET);
    }

    #[test]
    fn test_deserialize_echo_request() {
        // Valid response.
        let valid_request = serialize_echo_request(&b"example.address"[..]);
        assert_eq!(
            deserialize_echo_request(&valid_request).unwrap(),
            ilp::Addr::new(b"example.address"),
        );

        // Empty.
        assert!(deserialize_echo_request(&[]).is_err());

        // Echo response.
        let echo_response = {
            let mut data = BytesMut::with_capacity(256);
            data.put_slice(ECHO_RESPONSE);
            data.put_var_octet_string(&b"example.address"[..]);
            data
        };
        assert!(deserialize_echo_request(&echo_response).is_err());

        // Missing source address.
        assert!(deserialize_echo_request(ECHO_REQUEST_PREFIX).is_err());

        // Invalid source address.
        let with_invalid_address = serialize_echo_request(&b"bad.address"[..]);
        assert!(deserialize_echo_request(&with_invalid_address).is_err());
    }

    fn serialize_echo_request(source_addr: &[u8]) -> BytesMut {
        let mut data = BytesMut::with_capacity({
            ECHO_REQUEST_PREFIX.len()
                + oer::predict_var_octet_string(source_addr.len())
        });
        data.put_slice(ECHO_REQUEST_PREFIX);
        data.put_var_octet_string(source_addr);
        data
    }
}
