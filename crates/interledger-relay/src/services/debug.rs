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

const ADDRESS_PREFIX_SIZE: usize = 64;

/// Prints the requests and responses to stdout.
#[derive(Clone, Debug)]
pub struct DebugService<S> {
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
        options: DebugServiceOptions,
        next: S,
    ) -> Self {
        DebugService { options, next }
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
        let options = self.options.clone();
        if options.log_prepare {
            debug!("request: {:?}", request.borrow());
        }

        // Store a fixed-length prefix of the destination address on the stack
        // so that it can be logged.
        let destination = request.borrow().destination();
        let mut destination_prefix = [0_u8; ADDRESS_PREFIX_SIZE];
        let len = std::cmp::min(ADDRESS_PREFIX_SIZE, destination.as_ref().len());
        destination_prefix[..len].copy_from_slice({
            &destination.as_ref()[..len]
        });

        Box::pin(self.next.call(request)
            .inspect(move |response| {
                let destination_prefix = std::str::from_utf8(&destination_prefix)
                    .unwrap_or("[invalid]");
                match response {
                    Ok(fulfill) => if options.log_fulfill {
                        debug!(
                            "response: destination[..{}]={} {:?}",
                            ADDRESS_PREFIX_SIZE, destination_prefix, fulfill,
                        );
                    },
                    Err(reject) => if options.log_reject {
                        if WARNINGS.contains(&reject.code()) {
                            warn!(
                                "response: destination[..{}]={} {:?}",
                                ADDRESS_PREFIX_SIZE, destination_prefix, reject,
                            );
                        } else {
                            debug!(
                                "response: destination[..{}]={} {:?}",
                                ADDRESS_PREFIX_SIZE, destination_prefix, reject,
                            );
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

#[cfg(test)]
mod test_debug_service {
    use futures::executor::block_on;

    use crate::testing;
    use super::*;

    #[test]
    fn test_call() {
        let receiver = testing::MockService::new(Ok(testing::FULFILL.clone()));
        let service = DebugService::new(DebugServiceOptions {
            log_prepare: true,
            log_fulfill: true,
            log_reject: true,
        }, receiver);
        assert_eq!(
            block_on(service.call(testing::PREPARE.clone())),
            Ok(testing::FULFILL.clone()),
        );
    }
}
