use std::borrow::Borrow;

use bytes::{Bytes, BytesMut};
use futures::future::{Either, ok};
use futures::prelude::*;
use hyper::StatusCode;
use log::warn;

use crate::{Request, Service};
use crate::services;

static PEER_NAME: &'static str = "ILP-Peer-Name";

#[derive(Clone, Debug)]
pub struct Receiver<S> {
    next: S,
}

impl<S> hyper::service::Service for Receiver<S>
where
    S: Service<RequestWithHeaders> + 'static + Clone + Send,
{
    type ReqBody = hyper::Body;
    type ResBody = hyper::Body;
    type Error = hyper::Error;
    type Future = Box<dyn Future<
        Item = hyper::Response<hyper::Body>,
        Error = hyper::Error,
    > + Send + 'static>;

    fn call(&mut self, req: hyper::Request<Self::ReqBody>) -> Self::Future {
        Box::new(self.handle(req))
    }
}

impl<S> Receiver<S>
where
    S: Service<RequestWithHeaders> + 'static + Clone + Send,
{
    #[inline]
    pub fn new(next: S) -> Self {
        Receiver { next }
    }

    fn handle(&self, req: hyper::Request<hyper::Body>)
        -> impl Future<
            Item = hyper::Response<hyper::Body>,
            Error = hyper::Error,
        > + Send + 'static
    {
        let next = self.next.clone();
        let (parts, body) = req.into_parts();
        body
            .concat2()
            .and_then(move |chunk| {
                let buffer = Bytes::from(chunk);
                // `BytesMut::from(chunk)` calls `try_mut`, and only copies the
                // data if that fails (e.g. if the buffer is `KIND_STATIC`, which
                // probably only happens in the tests).
                let buffer = BytesMut::from(buffer);
                match ilp::Prepare::try_from(buffer) {
                    Ok(prepare) => Either::A({
                        next
                            .call(RequestWithHeaders {
                                prepare,
                                headers: parts.headers
                            })
                            .then(|res_packet| {
                                Ok(make_http_response(res_packet))
                            })
                    }),
                    Err(error) => Either::B(ok({
                        warn!("error parsing incoming prepare: {}", error);
                        hyper::Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(hyper::Body::from("Error parsing ILP Prepare"))
                            .expect("response builder error")
                    })),
                }
            })
    }
}

#[derive(Debug)]
pub struct RequestWithHeaders {
    prepare: ilp::Prepare,
    headers: hyper::HeaderMap,
}

impl Request for RequestWithHeaders {}

impl Into<ilp::Prepare> for RequestWithHeaders {
    fn into(self) -> ilp::Prepare {
        self.prepare
    }
}

impl Borrow<ilp::Prepare> for RequestWithHeaders {
    fn borrow(&self) -> &ilp::Prepare {
        &self.prepare
    }
}

impl services::RequestWithPeerName for RequestWithHeaders {
    fn peer_name(&self) -> Option<&[u8]> {
        // TODO I think this copies the name into a HeaderName every call, which isn't ideal
        self.headers.get(PEER_NAME)
            .map(|header| header.as_ref())
    }
}

fn make_http_response(packet: Result<ilp::Fulfill, ilp::Reject>)
    -> hyper::Response<hyper::Body>
{
    static OCTET_STREAM: &'static [u8] = b"application/octet-stream";
    let buffer = match packet {
        Ok(fulfill) => BytesMut::from(fulfill),
        Err(reject) => BytesMut::from(reject),
    };
    hyper::Response::builder()
        .status(StatusCode::OK)
        .header(hyper::header::CONTENT_TYPE, OCTET_STREAM)
        .header(hyper::header::CONTENT_LENGTH, buffer.len())
        .body(hyper::Body::from(buffer.freeze()))
        .expect("response builder error")
}

#[cfg(test)]
mod test_receiver {
    use crate::services::RequestWithPeerName;
    use crate::testing::{IlpResult, MockService, PanicService};
    use crate::testing::{PREPARE, FULFILL, REJECT};
    use super::*;

    static URI: &'static str = "http://example.com/ilp";

    #[test]
    fn test_prepare() {
        test_request_response(
            hyper::Request::post(URI)
                .body(hyper::Body::from(PREPARE.as_ref()))
                .unwrap(),
            Ok(FULFILL.clone()),
        );
        test_request_response(
            hyper::Request::post(URI)
                .body(hyper::Body::from(PREPARE.as_ref()))
                .unwrap(),
            Err(REJECT.clone()),
        );
    }

    fn test_request_response(
        request: hyper::Request<hyper::Body>,
        ilp_response: IlpResult,
    ) {
        let next = MockService::new(ilp_response.clone());
        let service = Receiver::new(next);

        let response = service.handle(request).wait().unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.headers().get("Content-Type").unwrap(),
            "application/octet-stream",
        );

        let next = service.next.clone();
        assert_eq!(
            next.prepares(),
            vec![PREPARE.clone()],
        );

        let content_len = response.headers()
            .get("Content-Length").unwrap()
            .to_str().unwrap()
            .parse::<usize>().unwrap();
        let body = response
            .into_body()
            .concat2()
            .wait().unwrap();

        assert_eq!(content_len, body.len());
        assert_eq!(
            body.as_ref(),
            match &ilp_response {
                Ok(ful) => ful.as_ref(),
                Err(rej) => rej.as_ref(),
            },
        );
    }

    #[test]
    fn test_bad_request() {
        let service = Receiver::new(PanicService);
        let response = service.handle(
            hyper::Request::post(URI)
                .body(hyper::Body::from(&b"this is not a prepare"[..]))
                .unwrap(),
        ).wait().unwrap();
        assert_eq!(response.status(), 400);

        let body = response
            .into_body()
            .concat2()
            .wait().unwrap();
        assert_eq!(
            body.as_ref(),
            b"Error parsing ILP Prepare",
        );
    }

    #[test]
    fn test_peer_name() {
        let service = Receiver::new(|req: RequestWithHeaders| {
            assert_eq!(req.peer_name(), Some(&b"alice"[..]));
            ok(FULFILL.clone())
        });

        let request = hyper::Request::post(URI)
            .header("ILP-Peer-Name", "alice")
            .body(hyper::Body::from(PREPARE.as_ref()))
            .unwrap();
        let response = service.handle(request).wait().unwrap();
        assert_eq!(response.status(), 200);
    }
}
