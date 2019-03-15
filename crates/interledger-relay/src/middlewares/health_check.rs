use futures::future::{Either, FutureResult, ok};
use hyper::service::Service as HyperService;

/// Respond with `200: OK` to `GET` requests.
#[derive(Clone, Debug)]
pub struct HealthCheckFilter<S> {
    next: S,
}

impl<S> HealthCheckFilter<S>
where
    S: HyperService<
        ReqBody = hyper::Body,
        ResBody = hyper::Body,
        Error = hyper::Error,
    >,
{
    pub fn new(next: S) -> Self {
        HealthCheckFilter { next }
    }
}

impl<S> HyperService for HealthCheckFilter<S>
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
        FutureResult<hyper::Response<hyper::Body>, hyper::Error>,
        S::Future,
    >;

    fn call(&mut self, request: hyper::Request<hyper::Body>) -> Self::Future {
        static BODY: &'static [u8] = b"OK";
        if request.method() == hyper::Method::GET {
            Either::A(ok(hyper::Response::builder()
                .status(hyper::StatusCode::OK)
                .header(hyper::header::CONTENT_LENGTH, BODY.len())
                .body(hyper::Body::from(BODY))
                .expect("response builder error")))
        } else {
            Either::B(self.next.call(request))
        }
    }
}

#[cfg(test)]
mod test_health_check_filter {
    use futures::prelude::*;
    use hyper::service::service_fn;

    use super::*;

    #[test]
    fn test_service() {
        let next = service_fn(|_req| {
            ok(hyper::Response::builder()
                .status(500)
                .body(hyper::Body::empty())
                .unwrap())
        });
        let mut service = HealthCheckFilter::new(next);

        // GET
        assert_eq!(
            service.call({
                hyper::Request::get("/")
                    .body(hyper::Body::empty())
                    .unwrap()
            }).wait().unwrap().status(),
            200,
        );

        // POST
        assert_eq!(
            service.call({
                hyper::Request::post("/")
                    .body(hyper::Body::empty())
                    .unwrap()
            }).wait().unwrap().status(),
            500,
        );
    }
}
