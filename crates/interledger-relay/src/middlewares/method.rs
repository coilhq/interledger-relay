use futures::future::{Either, Ready, ok};
use futures::task::{Context, Poll};
use hyper::service::Service as HyperService;
use log::warn;

type HTTPRequest = http::Request<hyper::Body>;

/// Respond with `405` to requests with the incorrect method.
#[derive(Clone, Debug)]
pub struct MethodFilter<S> {
    method: hyper::Method,
    next: S,
}

impl<S> MethodFilter<S>
where
    S: HyperService<HTTPRequest>,
{
    pub fn new(method: hyper::Method, next: S) -> Self {
        MethodFilter { method, next }
    }
}

impl<S> HyperService<HTTPRequest> for MethodFilter<S>
where
    S: HyperService<
        HTTPRequest,
        Response = hyper::Response<hyper::Body>,
        Error = hyper::Error,
    >,
{
    type Response = http::Response<hyper::Body>;
    type Error = hyper::Error;
    type Future = Either<
        S::Future,
        Ready<Result<Self::Response, Self::Error>>,
    >;

    fn poll_ready(&mut self, context: &mut Context<'_>)
        -> Poll<Result<(), Self::Error>>
    {
       self.next.poll_ready(context)
    }

    fn call(&mut self, request: hyper::Request<hyper::Body>) -> Self::Future {
        if request.method() == self.method {
            Either::Left(self.next.call(request))
        } else {
            warn!(
                "unexpected request method: method={} path={:?}",
                request.method(), request.uri().path(),
            );
            Either::Right(ok(hyper::Response::builder()
                .status(hyper::StatusCode::METHOD_NOT_ALLOWED)
                .body(hyper::Body::empty())
                .expect("response builder error")))
        }
    }
}

#[cfg(test)]
mod test_method_filter {
    use futures::executor::block_on;
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
            block_on(service.call({
                hyper::Request::patch("/")
                    .body(hyper::Body::empty())
                    .unwrap()
            })).unwrap().status(),
            200,
        );

        // Incorrect method.
        assert_eq!(
            block_on(service.call({
                hyper::Request::post("/")
                    .body(hyper::Body::empty())
                    .unwrap()
            })).unwrap().status(),
            405,
        );
    }
}
