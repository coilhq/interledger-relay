use std::str;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::future::{Either, err, ok, ready};
use futures::prelude::*;
use hyper::{Response, StatusCode};
use hyper::client::HttpConnector;
use hyper_tls::HttpsConnector;
use log::warn;

use crate::combinators;

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

static OCTET_STREAM: &[u8] = b"application/octet-stream";

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
    // This _shouldn't_ ever return an error.
    fn build(&self, prepare: Bytes)
        -> Result<hyper::Request<hyper::Body>, hyper::header::InvalidHeaderValue>
    {
        use hyper::header::HeaderValue;
        let mut builder = hyper::Request::builder()
            .method(self.method.clone())
            .uri(&self.uri);
        if let Some(auth) = &self.auth {
            builder = builder.header(
                hyper::header::AUTHORIZATION,
                HeaderValue::from_maybe_shared(auth.clone())?,
            );
        }
        if let Some(peer_name) = &self.peer_name {
            builder = builder.header(
                "ILP-Peer-Name",
                HeaderValue::from_maybe_shared(peer_name.clone())?,
            );
        }
        Ok(builder
            .header(hyper::header::CONTENT_TYPE, OCTET_STREAM)
            .body(hyper::Body::from(prepare))
            .expect("RequestOptions::build error"))
    }
}

impl Client {
    pub fn new(address: ilp::Address) -> Self {
        let agent = hyper_tls::HttpsConnector::new();
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
        -> impl Future<Output = Result<ilp::Fulfill, ilp::Reject>>
    {
        let prepare_bytes = BytesMut::from(prepare).freeze();
        let prepare_bytes2 = prepare_bytes.clone();
        let uri = req_opts.uri.clone();
        let hyper = Arc::clone(&self.hyper);

        let request =
            match req_opts.build(prepare_bytes.clone()) {
                Ok(request) => request,
                Err(_error) => return Either::Right(err({
                    self.make_invalid_header_value_reject()
                })),
            };
        // TODO await!
        Either::Left(self.hyper
            .request(request)
            .and_then(move |response| {
                // When the first attempt to send the packet failed with a 502,
                // retry once. The 502 is probably caused by the hidden request/
                // connection limit described in <https://github.com/interledgerjs/ilp-plugin-http/pull/3>.
                if response.status() == hyper::StatusCode::BAD_GATEWAY {
                    warn!(
                        "remote error; retrying: uri=\"{}\" status={:?}",
                        req_opts.uri, response.status(),
                    );
                    // TODO don't unwrap
                    let request = req_opts.build(prepare_bytes2).unwrap();
                    Either::Left(hyper.request(request))
                } else {
                    Either::Right(ok(response))
                }
            })
            .then(move |response| match response {
                Ok(response) => Either::Left({
                    self.decode_http_response(uri, response, prepare_bytes)
                }),
                Err(error) => {
                    warn!(
                        "outgoing connection error: uri=\"{}\" error=\"{}\"",
                        uri, error,
                    );
                    Either::Right(err(self.make_reject(
                        ilp::ErrorCode::T01_PEER_UNREACHABLE,
                        b"peer connection error",
                    )))
                },
            }))
    }

    fn decode_http_response(
        self,
        uri: hyper::Uri,
        response: Response<hyper::Body>,
        prepare: Bytes,
    ) -> impl Future<Output = Result<ilp::Fulfill, ilp::Reject>> {
        let status = response.status();
        let (parts, body) = response.into_parts();
        let res_body =
            combinators::collect_http_body(&parts.headers, body, MAX_RESPONSE_SIZE);
        // TODO timeout if response takes too long?
        res_body.then(move |body| {
            let body = match body {
                Ok(body) => body,
                Err(error) => {
                    warn!(
                        "remote response body error: uri=\"{}\" error={:?}",
                        uri, error,
                    );
                    return Either::Right(err(self.make_reject(
                        ilp::ErrorCode::T00_INTERNAL_ERROR,
                        b"invalid response body from peer",
                    )));
                },
            };

            if status == StatusCode::OK {
                let body = BytesMut::from(body);
                return Either::Left(ready(self.decode_response(uri, body)));
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
                Either::Right(err(self.make_reject(
                    ilp::ErrorCode::F00_BAD_REQUEST,
                    b"bad request to peer",
                )))
            } else if status.is_server_error() {
                warn!(
                    "remote server error: uri=\"{}\" status={:?} body={:?} prepare={:?}",
                    uri, status, body_str, prepare_str,
                );
                Either::Right(err(self.make_reject(
                    ilp::ErrorCode::T01_PEER_UNREACHABLE,
                    b"peer internal error",
                )))
            } else {
                warn!(
                    "unexpected status code: uri=\"{}\" status={:?} body={:?} prepare={:?}",
                    uri, status, body_str, prepare_str,
                );
                Either::Right(err(self.make_reject(
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

    fn make_invalid_header_value_reject(&self) -> ilp::Reject {
        self.make_reject(ilp::ErrorCode::F00_BAD_REQUEST, b"invalid header value")
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
                .build(hyper_tls::HttpsConnector::new()),
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
                    .map(|result| {
                        assert_eq!(result.unwrap(), *testing::FULFILL);
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
                    .map(|result| {
                        assert_eq!(result.unwrap(), *testing::FULFILL);
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
                    .map(|result| {
                        assert_eq!(result.unwrap_err(), *testing::REJECT);
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
                    .map(move |result| {
                        assert_eq!(result.unwrap_err(), expect_reject);
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
                            .map(move |result| {
                                assert_eq!(result.unwrap_err(), expect_reject);
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
                    .map(move |result| {
                        assert_eq!(result.unwrap_err(), expect_reject);
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
