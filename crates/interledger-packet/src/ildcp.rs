use std::time::{Duration, SystemTime};

use byteorder::ReadBytesExt;
use bytes::{BufMut, Bytes, BytesMut};

use crate::{Addr, Fulfill, FulfillBuilder, ParseError, Prepare, PrepareBuilder};
use crate::oer::{self, BufOerExt, MutBufOerExt};

pub static DESTINATION: Addr<'static> = unsafe {
    Addr::new_unchecked(b"peer.config")
};

static PEER_PROTOCOL_FULFILLMENT: &'static [u8; 32] = &[0; 32];
static PEER_PROTOCOL_CONDITION: &'static [u8; 32] = b"\
    \x66\x68\x7a\xad\xf8\x62\xbd\x77\x6c\x8f\xc1\x8b\x8e\x9f\x8e\x20\
    \x08\x97\x14\x85\x6e\xe2\x33\xb3\x90\x2a\x59\x1d\x0d\x5f\x29\x25\
";

const DEFAULT_EXPIRY_DURATION: Duration = Duration::from_secs(60);
const ASSET_SCALE_LEN: usize = 1;

#[derive(Debug, Default, PartialEq)]
pub struct Request {}

impl Request {
    pub fn new() -> Self {
        Request {}
    }

    pub fn try_from(prepare: Prepare) -> Result<Self, ParseError> {
        if prepare.destination() != DESTINATION {
            Err(ParseError::InvalidPacket("wrong ildcp destination".to_owned()))
        } else if prepare.execution_condition() != PEER_PROTOCOL_CONDITION {
            Err(ParseError::InvalidPacket("wrong ildcp condition".to_owned()))
        } else {
            Ok(Request {})
        }
    }

    pub fn to_prepare(&self) -> Prepare {
        PrepareBuilder {
            destination: DESTINATION,
            amount: 0,
            execution_condition: PEER_PROTOCOL_CONDITION,
            expires_at: SystemTime::now() + DEFAULT_EXPIRY_DURATION,
            data: &[],
        }.build()
    }
}

impl From<Request> for Prepare {
    fn from(request: Request) -> Self {
        request.to_prepare()
    }
}

#[derive(Debug, PartialEq)]
pub struct Response {
    buffer: Bytes,
    asset_scale: u8,
    asset_code_offset: usize,
}

impl From<Response> for Bytes {
    fn from(response: Response) -> Self {
        response.buffer
    }
}

impl From<Response> for Fulfill {
    fn from(response: Response) -> Self {
        FulfillBuilder {
            fulfillment: PEER_PROTOCOL_FULFILLMENT,
            data: &response.buffer[..],
        }.build()
    }
}

impl Response {
    pub fn try_from(fulfill: Fulfill) -> Result<Self, ParseError> {
        if fulfill.fulfillment() != PEER_PROTOCOL_FULFILLMENT {
            return Err(ParseError::InvalidPacket({
                "wrong ildcp fulfillment".to_owned()
            }));
        }

        let mut reader = &fulfill.data()[..];
        let buffer_len = reader.len();

        // Validate the client address.
        Addr::try_from(reader.read_var_octet_string()?)?;
        let asset_scale = reader.read_u8()?;

        let asset_code_offset = buffer_len - reader.len();
        reader.skip_var_octet_string()?;

        Ok(Response {
            buffer: fulfill.into_data().freeze(),
            asset_scale,
            asset_code_offset,
        })
    }

    pub fn client_address(&self) -> Addr {
        let addr_bytes = (&self.buffer[..]).peek_var_octet_string().unwrap();
        Addr::try_from(addr_bytes).unwrap()
    }

    pub fn asset_scale(&self) -> u8 {
        self.asset_scale
    }

    pub fn asset_code(&self) -> &[u8] {
        (&self.buffer[self.asset_code_offset..])
            .peek_var_octet_string()
            .unwrap()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResponseBuilder<'a> {
    pub client_address: Addr<'a>,
    pub asset_scale: u8,
    pub asset_code: &'a [u8],
}

impl<'a> ResponseBuilder<'a> {
    pub fn build(&self) -> Response {
        let address_size = oer::predict_var_octet_string(self.client_address.len());
        let asset_code_size = oer::predict_var_octet_string(self.asset_code.len());
        let buf_size = ASSET_SCALE_LEN + address_size + asset_code_size;
        let mut buffer = BytesMut::with_capacity(buf_size);

        buffer.put_var_octet_string(self.client_address.as_ref());
        buffer.put_u8(self.asset_scale);
        buffer.put_var_octet_string(self.asset_code);

        Response {
            buffer: buffer.freeze(),
            asset_scale: self.asset_scale,
            asset_code_offset: address_size + ASSET_SCALE_LEN,
        }
    }
}

#[cfg(test)]
mod test_request {
    use bytes::BytesMut;
    use lazy_static::lazy_static;

    use super::*;

    static REQUEST_BYTES: &'static [u8] = b"\
        \x0c\x46\x00\x00\x00\x00\x00\x00\x00\x00\x32\x30\x31\x35\x30\x36\x31\x36\
        \x30\x30\x30\x31\x30\x30\x30\x30\x30\x66\x68\x7a\xad\xf8\x62\xbd\x77\x6c\
        \x8f\xc1\x8b\x8e\x9f\x8e\x20\x08\x97\x14\x85\x6e\xe2\x33\xb3\x90\x2a\x59\
        \x1d\x0d\x5f\x29\x25\x0b\x70\x65\x65\x72\x2e\x63\x6f\x6e\x66\x69\x67\x00\
    ";

    lazy_static! {
        static ref WRONG_DESTINATION: PrepareBuilder<'static> = PrepareBuilder {
            amount: 0,
            expires_at: SystemTime::now(),
            execution_condition: PEER_PROTOCOL_CONDITION,
            destination: Addr::new(b"peer.config.not_ildcp"),
            data: b"",
        };

        static ref WRONG_CONDITION: PrepareBuilder<'static> = PrepareBuilder {
            amount: 0,
            expires_at: SystemTime::now(),
            execution_condition: &[0; 32],
            destination: DESTINATION,
            data: b"",
        };
    }

    #[test]
    fn test_try_from_prepare() {
        let prepare = Prepare::try_from(BytesMut::from(REQUEST_BYTES)).unwrap();
        let request = Request::try_from(prepare).unwrap();
        assert_eq!(request, Request {});

        assert!(Request::try_from(WRONG_DESTINATION.build()).is_err());
        assert!(Request::try_from(WRONG_CONDITION.build()).is_err());
    }

    #[test]
    fn test_to_prepare() {
        let request = Request::new();
        let prepare = request.to_prepare();
        assert_eq!(prepare.amount(), 0);
        assert_eq!(prepare.destination(), DESTINATION);
        assert_eq!(prepare.execution_condition(), PEER_PROTOCOL_CONDITION);
        assert_eq!(prepare.data(), b"");
    }
}

#[cfg(test)]
mod test_response {
    use bytes::BytesMut;

    use super::*;

    static RESPONSE_BYTES: &'static [u8] = b"\
        \x0d\x35\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\
        \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14\x0e\
        \x65\x78\x61\x6d\x70\x6c\x65\x2e\x63\x6c\x69\x65\x6e\x74\x0d\x03\x58\x41\
        \x4d\
    ";

    static WRONG_FULFILLMENT: &'static [u8] = b"\
        \x0d\x35\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\
        \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14\x0e\
        \x65\x78\x61\x6d\x70\x6c\x65\x2e\x63\x6c\x69\x65\x6e\x74\x0d\x03\x58\x41\
        \x4d\
    ";

    #[test]
    fn test_try_from_fulfill() {
        let fulfill = Fulfill::try_from(BytesMut::from(RESPONSE_BYTES)).unwrap();
        let response = Response::try_from(fulfill).unwrap();
        assert_eq!(response.client_address(), Addr::new(b"example.client"));
        assert_eq!(response.asset_scale(), 13);
        assert_eq!(response.asset_code(), b"XAM");

        let fulfill = Fulfill::try_from(BytesMut::from(WRONG_FULFILLMENT)).unwrap();
        assert!(Response::try_from(fulfill).is_err());
    }

    #[test]
    fn test_into_fulfill() {
        let response = ResponseBuilder {
            client_address: Addr::new(b"example.client"),
            asset_scale: 13,
            asset_code: b"XAM",
        }.build();
        assert_eq!(
            Fulfill::from(response).as_ref(),
            RESPONSE_BYTES,
        );
    }
}
