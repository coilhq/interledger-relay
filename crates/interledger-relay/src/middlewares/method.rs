use futures::future::{Either, FutureResult, ok};
use hyper::service::Service as HyperService;
use log::warn;

/// Respond with `405` to requests with the incorrect method.
#[derive(Clone, Debug)]
pub struct MethodFilter<S> {
    method: hyper::Method,
    next: S,
}

impl<S> MethodFilter<S>
where
    S: HyperService<
        ReqBody = hyper::Body,
        ResBody = hyper::Body,
        Error = hyper::Error,
    >,
{
    pub fn new(method: hyper::Method, next: S) -> Self {
        MethodFilter { method, next }
    }
}

impl<S> HyperService for MethodFilter<S>
where
    S: HyperService<
        ReqBody = hyper::Body,
        ResBody = hyper::Body,
        Error = hyper::Error,
    >,
{
    type ReqBody = hyper::Body;
    type ResBody = hyper::Body;
    type Error = hyper::Error;
    type Future = Either<
        S::Future,
        FutureResult<hyper::Response<hyper::Body>, hyper::Error>,
    >;

    fn call(&mut self, request: hyper::Request<hyper::Body>) -> Self::Future {
        if request.method() == self.method {
            Either::A(self.next.call(request))
        } else {
            warn!(
                "unexpected request method: method={} path={:?}",
                request.method(), request.uri().path(),
            );
            Either::B(ok(hyper::Response::builder()
                .status(hyper::StatusCode::METHOD_NOT_ALLOWED)
                .body(hyper::Body::empty())
                .expect("response builder error")))
        }
    }
}

#[cfg(test)]
mod test_method_filter {
    use futures::prelude::*;
    use hyper::service::service_fn;

    use super::*;

    #[test]
    fn test_service() {
        let next = service_fn(|_req| {
            ok(hyper::Response::builder()
                .status(200)
                .body(hyper::Body::empty())
                .unwrap())
        });
        let mut service = MethodFilter::new(hyper::Method::PATCH, next);

        // Correct method.
        assert_eq!(
            service.call({
                hyper::Request::patch("/")
                    .body(hyper::Body::empty())
                    .unwrap()
            }).wait().unwrap().status(),
            200,
        );

        // Incorrect method.
        assert_eq!(
            service.call({
                hyper::Request::post("/")
                    .body(hyper::Body::empty())
                    .unwrap()
            }).wait().unwrap().status(),
            405,
        );
    }
}
