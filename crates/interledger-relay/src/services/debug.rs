use futures::prelude::*;
use log::debug;

use crate::{Request, Service};

/// Prints the requests and responses to stdout.
#[derive(Clone, Debug)]
pub struct DebugService<S> {
    prefix: &'static str,
    next: S,
}

impl<S> DebugService<S> {
    #[allow(dead_code)]
    #[inline]
    pub fn new(prefix: &'static str, next: S) -> Self {
        DebugService { prefix, next }
    }
}

impl<S, Req> Service<Req> for DebugService<S>
where
    S: 'static + Service<Req> + Send,
    Req: Request,
{
    type Future = Box<
        dyn Future<
            Item = ilp::Fulfill,
            Error = ilp::Reject,
        > + Send + 'static,
    >;

    fn call(self, request: Req) -> Self::Future {
        debug!("{}: {:?}", self.prefix, request.borrow().clone());

        let prefix = self.prefix;
        Box::new(self.next.call(request)
            .then(move |response| {
                match &response {
                    Ok(fulfill) => debug!("{}: {:?}", prefix, fulfill),
                    Err(reject) => debug!("{}: {:?}", prefix, reject),
                }
                response
            }))
    }
}
