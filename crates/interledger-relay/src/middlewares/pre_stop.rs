use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time;

use futures::prelude::*;
use futures::task::{Context, Poll};
use hyper::service::Service as HyperService;
use log::{info, trace};

type HTTPRequest = http::Request<hyper::Body>;

type StopFn = Box<
    dyn Fn() -> Pin<Box<
        dyn Future<Output = ()> + Send + 'static
    >> + Send + Sync + 'static
>;

// TODO test middleware

/// When the server receives a `GET` to the configured `pre_stop_path`, this
/// middleware will:
///
/// * Stop accepting requests.
/// * Flush all of the `BigQueryService` logger queues.
/// * Respond to the `GET` request once the queues are flushed
///   (or it has taken too long).
#[derive(Clone)]
pub struct PreStopFilter<S> {
    data: Arc<FilterData>,
    next: S,
}

struct FilterData {
    path: Option<String>,
    stop: StopFn,
    stopping: RwLock<bool>,
}

impl<S> PreStopFilter<S>
where
    S: HyperService<HTTPRequest>,
{
    pub fn new(
        path: Option<String>,
        stop: StopFn,
        next: S,
    ) -> Self {
        PreStopFilter {
            data: Arc::new(FilterData {
                path,
                stop,
                stopping: RwLock::new(false),
            }),
            next,
        }
    }
}

impl<S> HyperService<HTTPRequest> for PreStopFilter<S>
where
    S: Clone + 'static + HyperService<
        HTTPRequest,
        Response = hyper::Response<hyper::Body>,
        Error = hyper::Error,
    >,
    S::Future: Send + 'static,
{
    type Response = http::Response<hyper::Body>;
    type Error = hyper::Error;
    type Future = Pin<Box<
        dyn Future<Output = Result<Self::Response, Self::Error>>
            + Send + 'static
    >>;

    fn poll_ready(&mut self, context: &mut Context<'_>)
        -> Poll<Result<(), Self::Error>>
    {
       self.next.poll_ready(context)
    }

    fn call(&mut self, request: hyper::Request<hyper::Body>) -> Self::Future {
        let path = match &self.data.path {
            Some(path) => path,
            None => return Box::pin(self.next.call(request)),
        };

        {
            let stopping = self.data.stopping.read().unwrap();
            if *stopping {
                trace!("relay is stopping; dropping request");
                return Box::pin(future::ok(hyper::Response::builder()
                    .status(hyper::StatusCode::SERVICE_UNAVAILABLE) // 503
                    .body(hyper::Body::from("service stopping"))
                    .expect("response builder error")));
            }
        }

        let is_pre_stop =
            request.method() == hyper::Method::GET
                && request.uri().path() == path;
        if is_pre_stop {
            {
                let mut stopping = self.data.stopping.write().unwrap();
                *stopping = true;
            }
            info!("relay stopping");
            let start = time::Instant::now();
            let data = Arc::clone(&self.data);
            return Box::pin({
                (data.stop)().map(move |_| {
                    info!("relay stopped: duration={:?}", time::Instant::now() - start);
                    Ok(hyper::Response::builder()
                        .status(hyper::StatusCode::OK)
                        .body(hyper::Body::empty())
                        .expect("response builder error"))
                })
            });
        }

        Box::pin(self.next.call(request))
    }
}
