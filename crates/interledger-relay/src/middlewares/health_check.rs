use futures::future::{Either, Ready, ok};
use futures::task::{Context, Poll};
use hyper::service::Service as HyperService;

type HTTPRequest = http::Request<hyper::Body>;

/// Respond with `200: OK` to `GET` requests.
#[derive(Clone, Debug)]
pub struct HealthCheckFilter<S> {
    next: S,
}

impl<S> HealthCheckFilter<S>
where
    S: HyperService<HTTPRequest>,
{
    pub fn new(next: S) -> Self {
        HealthCheckFilter { next }
    }
}

impl<S> HyperService<HTTPRequest> for HealthCheckFilter<S>
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
        Ready<Result<Self::Response, Self::Error>>,
        S::Future,
    >;

    fn poll_ready(&mut self, context: &mut Context<'_>)
        -> Poll<Result<(), Self::Error>>
    {
       self.next.poll_ready(context)
    }

    fn call(&mut self, request: hyper::Request<hyper::Body>) -> Self::Future {
        static BODY: &[u8] = b"OK";
        if request.method() == hyper::Method::GET {
            Either::Left(ok(hyper::Response::builder()
                .status(hyper::StatusCode::OK)
                .header(hyper::header::CONTENT_LENGTH, BODY.len())
                .body(hyper::Body::from(BODY))
                .expect("response builder error")))
        } else {
            Either::Right(self.next.call(request))
        }
    }
}

#[cfg(test)]
mod test_health_check_filter {
    use futures::executor::block_on;
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
            block_on(service.call({
                hyper::Request::get("/")
                    .body(hyper::Body::empty())
                    .unwrap()
            })).unwrap().status(),
            200,
        );

        // POST
        assert_eq!(
            block_on(service.call({
                hyper::Request::post("/")
                    .body(hyper::Body::empty())
                    .unwrap()
            })).unwrap().status(),
            500,
        );
    }
}
