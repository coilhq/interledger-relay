use std::pin::Pin;

use futures::prelude::*;
use log::{debug, warn};
use serde::Deserialize;

use crate::{Request, Service};

/// These errors are more unusual, so they should be logged as warnings rather
/// than just debug.
static WARNINGS: &[ilp::ErrorCode] = &[
    ilp::ErrorCode::F01_INVALID_PACKET,
    ilp::ErrorCode::F02_UNREACHABLE,
    ilp::ErrorCode::F05_WRONG_CONDITION,
    ilp::ErrorCode::F06_UNEXPECTED_PAYMENT,
    ilp::ErrorCode::F07_CANNOT_RECEIVE,
    ilp::ErrorCode::T03_CONNECTOR_BUSY,
    ilp::ErrorCode::T05_RATE_LIMITED,
    ilp::ErrorCode::R00_TRANSFER_TIMED_OUT,
    ilp::ErrorCode::R01_INSUFFICIENT_SOURCE_AMOUNT,
    ilp::ErrorCode::R02_INSUFFICIENT_TIMEOUT,
];

/// Prints the requests and responses to stdout.
#[derive(Clone, Debug)]
pub struct DebugService<S> {
    prefix: &'static str,
    options: DebugServiceOptions,
    next: S,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
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
    type Future = Pin<Box<
        dyn Future<
            Output = Result<ilp::Fulfill, ilp::Reject>,
        > + Send + 'static,
    >>;

    fn call(self, request: Req) -> Self::Future {
        let prefix = self.prefix;
        let options = self.options.clone();
        if options.log_prepare {
            debug!("{}: {:?}", prefix, request.borrow());
        }

        Box::pin(self.next.call(request)
            .inspect(move |response| {
                match response {
                    Ok(fulfill) => if options.log_fulfill {
                        debug!("{}: {:?}", prefix, fulfill)
                    },
                    Err(reject) => if options.log_reject {
                        if WARNINGS.contains(&reject.code()) {
                            warn!("{}: {:?}", prefix, reject)
                        } else {
                            debug!("{}: {:?}", prefix, reject)
                        }
                    },
                }
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
