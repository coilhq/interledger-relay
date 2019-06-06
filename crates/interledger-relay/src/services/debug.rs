use futures::prelude::*;
use log::debug;
use serde::Deserialize;

use crate::{Request, Service};

/// Prints the requests and responses to stdout.
#[derive(Clone, Debug)]
pub struct DebugService<S> {
    prefix: &'static str,
    options: DebugServiceOptions,
    next: S,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct DebugServiceOptions {
    pub log_prepare: bool,
    pub log_fulfill: bool,
    pub log_reject: bool,
}

impl<S> DebugService<S> {
    #[inline]
    pub fn new(
        prefix: &'static str,
        options: DebugServiceOptions,
        next: S,
    ) -> Self {
        DebugService { prefix, options, next }
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
        let prefix = self.prefix;
        let options = self.options.clone();
        if options.log_prepare {
            debug!("{}: {:?}", prefix, request.borrow());
        }

        Box::new(self.next.call(request)
            .then(move |response| {
                match &response {
                    Ok(fulfill) => if options.log_fulfill {
                        debug!("{}: {:?}", prefix, fulfill)
                    },
                    Err(reject) => if options.log_reject {
                        debug!("{}: {:?}", prefix, reject)
                    },
                }
                response
            }))
    }
}

impl Default for DebugServiceOptions {
    fn default() -> Self {
        DebugServiceOptions {
            log_prepare: false,
            log_fulfill: false,
            log_reject: false,
        }
    }
}
