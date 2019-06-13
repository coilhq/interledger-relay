use std::str;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::future::{Either, err, ok};
use futures::prelude::*;
use hyper::{Response, StatusCode};
use hyper::client::HttpConnector;
use hyper_tls::HttpsConnector;
use log::warn;

use crate::combinators::LimitStream;

type HyperClient = hyper::Client<HttpsConnector<HttpConnector>, hyper::Body>;

// Use the size of a Reject, since they can be larger than Fulfills.
const MAX_RESPONSE_SIZE: usize = {
    const ENVELOPE: usize = 1 + 8;
    const CODE: usize = 3;
    const TRIGGERED_BY: usize = 8 + 1024;
    const MESSAGE: usize = 8 + (1 << 13);
    const DATA: usize = 8 + (1 << 15);
    ENVELOPE + CODE + TRIGGERED_BY + MESSAGE + DATA
};

static OCTET_STREAM: &'static [u8] = b"application/octet-stream";

#[derive(Clone, Debug)]
pub struct Client {
    address: ilp::Address,
    hyper: Arc<HyperClient>,
}

#[derive(Clone, Debug)]
pub struct RequestOptions {
    pub method: hyper::Method,
    pub uri: hyper::Uri,
    pub auth: Option<Bytes>,
    pub peer_name: Option<Bytes>,
}

impl RequestOptions {
    fn build(&self, prepare: Bytes) -> hyper::Request<hyper::Body> {
        let mut builder = hyper::Request::builder();
        builder.method(self.method.clone());
        builder.uri(&self.uri);
        if let Some(auth) = &self.auth {
            builder.header(hyper::header::AUTHORIZATION, auth.clone());
        }
        if let Some(peer_name) = &self.peer_name {
            builder.header("ILP-Peer-Name", peer_name.clone());
        }
        builder
            .header(hyper::header::CONTENT_TYPE, OCTET_STREAM)
            .body(hyper::Body::from(prepare))
            .expect("RequestOptions::build error")
    }
}

impl Client {
    pub fn new(address: ilp::Address) -> Self {
        let agent = hyper_tls::HttpsConnector::new(4)
            .expect("TLS initialization failed");
        let client = hyper::Client::builder().build(agent);
        Client::new_with_client(address, client)
    }

    pub fn new_with_client(address: ilp::Address, hyper: HyperClient) -> Self {
        Client {
            address,
            hyper: Arc::new(hyper),
        }
    }

    pub fn address(&self) -> &ilp::Address {
        &self.address
    }

    /// `req_builder` is the base request.
    /// The URI and method should be set, along with extra headers.
    /// `Content-Type` and `Content-Length` should not be set.
    pub fn request(self, req_opts: RequestOptions, prepare: ilp::Prepare)
        -> impl Future<Item = ilp::Fulfill, Error = ilp::Reject>
    {
        let prepare_bytes = BytesMut::from(prepare).freeze();
        let prepare_bytes2 = prepare_bytes.clone();
        let uri = req_opts.uri.clone();
        let hyper = Arc::clone(&self.hyper);

        self.hyper
            .request(req_opts.build(prepare_bytes.clone()))
            .and_then(move |res| {
                // When the first attempt to send the packet failed with a 502,
                // retry once. The 502 is probably caused by the hidden request/
                // connection limit described in <https://github.com/interledgerjs/ilp-plugin-http/pull/3>.
                if res.status() == hyper::StatusCode::BAD_GATEWAY {
                    warn!(
                        "remote error; retrying: uri=\"{}\" status={:?}",
                        req_opts.uri, res.status(),
                    );
                    Either::B(hyper.request(req_opts.build(prepare_bytes2)))
                } else {
                    Either::A(ok(res))
                }
            })
            .then(move |res| match res {
                Ok(res) => Either::A(self.decode_http_response(uri, res, prepare_bytes)),
                Err(error) => {
                    warn!(
                        "outgoing connection error: uri=\"{}\" error=\"{}\"",
                        uri, error,
                    );
                    Either::B(err(self.make_reject(
                        ilp::ErrorCode::T01_PEER_UNREACHABLE,
                        b"peer connection error",
                    )))
                },
            })
    }

    fn decode_http_response(
        self,
        uri: hyper::Uri,
        res: Response<hyper::Body>,
        prepare: Bytes,
    ) -> impl Future<Item = ilp::Fulfill, Error = ilp::Reject> {
        let status = res.status();
        let res_body = LimitStream::new(MAX_RESPONSE_SIZE, res.into_body());
        // TODO timeout if response takes too long?
        res_body.concat2().then(move |body| {
            let body = match body {
                Ok(body) => Bytes::from(body),
                Err(error) => {
                    warn!(
                        "remote response body error: uri=\"{}\" error={:?}",
                        uri, error,
                    );
                    return Either::B(err(self.make_reject(
                        ilp::ErrorCode::T00_INTERNAL_ERROR,
                        b"invalid response body from peer",
                    )));
                },
            };

            if status == StatusCode::OK {
                let body = BytesMut::from(body);
                return Either::A(self.decode_response(uri, body).into_future());
            }

            const TRUNCATE_BODY: usize = 32;
            let body_str = str::from_utf8(&body);
            let body_str = body_str.map(|s| truncate(s, TRUNCATE_BODY));
            let prepare_str = base64::encode(&prepare);

            if status.is_client_error() {
                warn!(
                    "remote client error: uri=\"{}\" status={:?} body={:?} prepare={:?}",
                    uri, status, body_str, prepare_str,
                );
                Either::B(err(self.make_reject(
                    ilp::ErrorCode::F00_BAD_REQUEST,
                    b"bad request to peer",
                )))
            } else if status.is_server_error() {
                warn!(
                    "remote server error: uri=\"{}\" status={:?} body={:?} prepare={:?}",
                    uri, status, body_str, prepare_str,
                );
                Either::B(err(self.make_reject(
                    ilp::ErrorCode::T01_PEER_UNREACHABLE,
                    b"peer internal error",
                )))
            } else {
                warn!(
                    "unexpected status code: uri=\"{}\" status={:?} body={:?} prepare={:?}",
                    uri, status, body_str, prepare_str,
                );
                Either::B(err(self.make_reject(
                    ilp::ErrorCode::T00_INTERNAL_ERROR,
                    b"unexpected response code from peer",
                )))
            }
        })
    }

    fn decode_response(&self, uri: hyper::Uri, bytes: BytesMut)
        -> Result<ilp::Fulfill, ilp::Reject>
    {
        match ilp::Packet::try_from(bytes) {
            Ok(ilp::Packet::Fulfill(fulfill)) => Ok(fulfill),
            Ok(ilp::Packet::Reject(reject)) => Err(reject),
            _ => {
                warn!("invalid response body: uri=\"{}\"", uri);
                Err(self.make_reject(
                    ilp::ErrorCode::T00_INTERNAL_ERROR,
                    b"invalid response body from peer",
                ))
            },
        }
    }

    fn make_reject(&self, code: ilp::ErrorCode, message: &[u8]) -> ilp::Reject {
        ilp::RejectBuilder {
            code,
            message,
            triggered_by: Some(self.address.as_addr()),
            data: b"",
        }.build()
    }
}

fn truncate(string: &str, size: usize) -> &str {
    if string.len() < size {
        string
    } else {
        &string[0..size]
    }
}

#[cfg(test)]
mod tests {
    use lazy_static::lazy_static;

    use crate::testing::{self, RECEIVER_ORIGIN};
    use super::*;

    static ADDRESS: ilp::Addr<'static> = unsafe {
        ilp::Addr::new_unchecked(b"example.connector")
    };

    lazy_static! {
        static ref CLIENT: Client = Client::new(ADDRESS.to_address());

        static ref CLIENT_HTTP2: Client = Client::new_with_client(
            ADDRESS.to_address(),
            hyper::Client::builder()
                .http2_only(true)
                .build(hyper_tls::HttpsConnector::new(4).unwrap()),
        );

        static ref REQUEST_OPTIONS: RequestOptions = RequestOptions {
            method: hyper::Method::POST,
            uri: hyper::Uri::from_static(RECEIVER_ORIGIN),
            auth: Some(Bytes::from("alice_auth")),
            peer_name: None,
        };
    }

    #[test]
    fn test_outgoing_request() {
        testing::MockServer::new()
            .test_request(|req| {
                assert_eq!(req.method(), hyper::Method::POST);
                assert_eq!(req.uri().path(), "/");
                assert_eq!(
                    req.headers().get("Authorization").unwrap(),
                    "alice_auth",
                );
                assert_eq!(
                    req.headers().get("Content-Type").unwrap(),
                    "application/octet-stream",
                );
                assert_eq!(
                    req.headers().get("Content-Length").unwrap(),
                    &testing::PREPARE.as_ref().len().to_string(),
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
                CLIENT.clone()
                    .request(REQUEST_OPTIONS.clone(), testing::PREPARE.clone())
                    .then(|result| -> Result<(), ()> {
                        assert_eq!(result.unwrap(), *testing::FULFILL);
                        Ok(())
                    })
            });
    }

    #[test]
    fn test_outgoing_http2_only() {
        testing::MockServer::new()
            .test_request(|req| {
                assert_eq!(req.version(), hyper::Version::HTTP_2);
            })
            .with_response(|| {
                hyper::Response::builder()
                    .status(200)
                    .body(hyper::Body::from(testing::FULFILL.as_ref()))
                    .unwrap()
            })
            .run({
                CLIENT_HTTP2.clone()
                    .request(REQUEST_OPTIONS.clone(), testing::PREPARE.clone())
                    .then(|result| -> Result<(), ()> {
                        assert_eq!(result.unwrap(), *testing::FULFILL);
                        Ok(())
                    })
            });
    }

    #[test]
    fn test_incoming_reject() {
        testing::MockServer::new()
            .with_response(|| {
                hyper::Response::builder()
                    .status(200)
                    .body(hyper::Body::from(testing::REJECT.as_ref()))
                    .unwrap()
            })
            .run({
                CLIENT.clone()
                    .request(REQUEST_OPTIONS.clone(), testing::PREPARE.clone())
                    .then(|result| -> Result<(), ()> {
                        assert_eq!(result.unwrap_err(), *testing::REJECT);
                        Ok(())
                    })
            });
    }

    #[test]
    fn test_incoming_invalid_packet() {
        let expect_reject = ilp::RejectBuilder {
            code: ilp::ErrorCode::T00_INTERNAL_ERROR,
            message: b"invalid response body from peer",
            triggered_by: Some(ADDRESS),
            data: b"",
        }.build();
        testing::MockServer::new()
            .with_response(|| {
                hyper::Response::builder()
                    .status(200)
                    .body(hyper::Body::from(&b"this is not a packet"[..]))
                    .unwrap()
            })
            .run({
                CLIENT.clone()
                    .request(REQUEST_OPTIONS.clone(), testing::PREPARE.clone())
                    .then(move |result| -> Result<(), ()> {
                        assert_eq!(result.unwrap_err(), expect_reject);
                        Ok(())
                    })
            });
    }

    macro_rules! make_test_incoming_error_code {
        ($(
            fn $fn:ident(
                status_code: $status_code:expr,
                error_code: $error_code:expr,
                error_message: $error_message:expr $(,)?
            );
        )+) => {$(
            #[test]
            fn $fn() {
                let expect_reject = ilp::RejectBuilder {
                    code: $error_code,
                    message: $error_message,
                    triggered_by: Some(ADDRESS),
                    data: b"",
                }.build();
                testing::MockServer::new()
                    .with_response(|| {
                        hyper::Response::builder()
                            .status($status_code)
                            .body(hyper::Body::from(testing::FULFILL.as_ref()))
                            .unwrap()
                    })
                    .run({
                        CLIENT.clone()
                            .request(REQUEST_OPTIONS.clone(), testing::PREPARE.clone())
                            .then(move |result| -> Result<(), ()> {
                                assert_eq!(result.unwrap_err(), expect_reject);
                                Ok(())
                            })
                    });
            }
        )*};
    }

    make_test_incoming_error_code! {
        fn test_incoming_300(
            status_code: 300,
            error_code: ilp::ErrorCode::T00_INTERNAL_ERROR,
            error_message: b"unexpected response code from peer",
        );

        fn test_incoming_400(
            status_code: 400,
            error_code: ilp::ErrorCode::F00_BAD_REQUEST,
            error_message: b"bad request to peer",
        );

        fn test_incoming_500(
            status_code: 500,
            error_code: ilp::ErrorCode::T01_PEER_UNREACHABLE,
            error_message: b"peer internal error",
        );
    }

    #[test]
    fn test_incoming_abort() {
        let expect_reject = ilp::RejectBuilder {
            code: ilp::ErrorCode::T01_PEER_UNREACHABLE,
            message: b"peer connection error",
            triggered_by: Some(ADDRESS),
            data: b"",
        }.build();
        testing::MockServer::new()
            .with_abort()
            .run({
                CLIENT.clone()
                    .request(REQUEST_OPTIONS.clone(), testing::PREPARE.clone())
                    .then(move |result| -> Result<(), ()> {
                        assert_eq!(result.unwrap_err(), expect_reject);
                        Ok(())
                    })
            });
    }

    #[test]
    fn test_truncate() {
        let tests = &[
            (0, ""),
            (1, "t"),
            (4, "test"),
            (8, "test 123"),
            (9, "test 123"),
        ];
        for (size, result) in tests {
            assert_eq!(truncate("test 123", *size), *result);
        }
    }
}
