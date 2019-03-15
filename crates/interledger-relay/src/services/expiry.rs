use std::cmp;
use std::time;

use futures::future::err;
use futures::prelude::*;
use tokio::util::FutureExt;

use crate::{Request, Service};

/// Reject expired Prepares, and time out requests that take too long.
#[derive(Clone, Debug)]
pub struct ExpiryService<S> {
    address: ilp::Address,
    max_timeout: time::Duration,
    next: S,
}

impl<S> ExpiryService<S> {
    pub fn new(
        address: ilp::Address,
        max_timeout: time::Duration,
        next: S,
    ) -> Self {
        ExpiryService { address, max_timeout, next }
    }

    fn make_reject(&self, code: ilp::ErrorCode, message: &[u8])
        -> ilp::Reject
    {
        ilp::RejectBuilder {
            code,
            message,
            triggered_by: self.address.as_addr(),
            data: &[],
        }.build()
    }
}

impl<S, Req> Service<Req> for ExpiryService<S>
where
    S: Service<Req> + Send + 'static,
    Req: Request,
{
    type Future = Box<
        dyn Future<
            Item = ilp::Fulfill,
            Error = ilp::Reject,
        > + Send + 'static,
    >;

    fn call(self, request: Req) -> Self::Future {
        let prepare = request.borrow();
        let expires_at = prepare.expires_at();
        let expires_in = expires_at.duration_since(time::SystemTime::now());

        let expires_in = match expires_in {
            Ok(expires_in) => expires_in,
            Err(_) => return Box::new(err(self.make_reject(
                ilp::ErrorCode::R02_INSUFFICIENT_TIMEOUT,
                b"insufficient timeout",
            ))),
        };

        let next = self.next.clone();
        Box::new(next
            .call(request)
            .timeout(cmp::min(self.max_timeout, expires_in))
            .map_err(move |error| match (error.is_elapsed(), error.into_inner()) {
                (_, Some(reject)) => reject,
                (true, None) => self.make_reject(
                    ilp::ErrorCode::R00_TRANSFER_TIMED_OUT,
                    b"request timed out",
                ),
                (false, None) => self.make_reject(
                    ilp::ErrorCode::T00_INTERNAL_ERROR,
                    b"timer error",
                ),
            })
        )
    }
}

#[cfg(test)]
mod test_expiry_service {
    use std::fmt;
    use std::sync::Mutex;

    use futures::future::{FutureResult, ok};
    use lazy_static::lazy_static;

    use crate::testing::{DelayService, FULFILL, MockService, PanicService, PREPARE};
    use super::*;

    lazy_static! {
        static ref ADDRESS: ilp::Address = ilp::Address::new(b"test.alice");
        /// Ensure that only one test runs at a time. Because they are dependent
        /// on the timers, parallel tests can cause unexpected failures.
        static ref TEST_MUTEX: Mutex<()> = Mutex::new(());
    }

    const MAX_TIMEOUT: time::Duration = time::Duration::from_secs(60);
    const MARGIN: time::Duration = time::Duration::from_millis(10);

    #[test]
    fn test_ok() {
        let receiver = MockService::new(Ok(FULFILL.clone()));
        let expiry = ExpiryService::new(ADDRESS.clone(), MAX_TIMEOUT, receiver);

        let future = expiry.call(PREPARE.clone())
            .map(|fulfill| {
                assert_eq!(fulfill, FULFILL.clone());
            });
        tokio_run(future);
    }

    #[test]
    fn test_insufficient_timeout() {
        let mut prepare = PREPARE.clone();
        prepare.set_expires_at(time::SystemTime::now());

        let receiver = PanicService;
        let expiry = ExpiryService::new(ADDRESS.clone(), MAX_TIMEOUT, receiver);

        let future = expiry.call(prepare)
            .then(|response| -> FutureResult<(), ()> {
                let reject = response.expect_err("expeceted Reject");
                assert_eq!(reject.code(), ilp::ErrorCode::R02_INSUFFICIENT_TIMEOUT);
                assert_eq!(reject.message(), b"insufficient timeout");
                ok(())
            });
        tokio_run(future);
    }

    #[test]
    fn test_timed_out() {
        const SOON: time::Duration = time::Duration::from_millis(100);
        let mut prepare = PREPARE.clone();
        prepare.set_expires_at(time::SystemTime::now() + SOON);

        let receiver = MockService::new(Ok(FULFILL.clone()));
        let receiver = DelayService::new(SOON + MARGIN, receiver);
        let expiry = ExpiryService::new(ADDRESS.clone(), MAX_TIMEOUT, receiver);

        let future = expiry.call(prepare)
            .then(|response| -> FutureResult<(), ()> {
                let reject = response.expect_err("expeceted Reject");
                assert_eq!(reject.code(), ilp::ErrorCode::R00_TRANSFER_TIMED_OUT);
                assert_eq!(reject.message(), b"request timed out");
                ok(())
            });
        tokio_run(future);
    }

    #[test]
    fn test_max_timeout() {
        const MAX_TIMEOUT: time::Duration = time::Duration::from_millis(15);
        let receiver = MockService::new(Ok(FULFILL.clone()));
        let receiver = DelayService::new(MAX_TIMEOUT + MARGIN, receiver);
        let expiry = ExpiryService::new(ADDRESS.clone(), MAX_TIMEOUT, receiver);

        let future = expiry.call(PREPARE.clone())
            .then(|response| -> FutureResult<(), ()> {
                let reject = response.expect_err("expected Reject");
                assert_eq!(reject.code(), ilp::ErrorCode::R00_TRANSFER_TIMED_OUT);
                assert_eq!(reject.message(), b"request timed out");
                ok(())
            });
        tokio_run(future);
    }

    /// This helper is needed because `tokio::run` ignores panics (see
    /// <https://github.com/tokio-rs/tokio/issues/495>).
    fn tokio_run<F, R, E>(future: F)
    where
        F: Send + 'static + Future<Item = R, Error = E>,
        R: Send + 'static,
        E: Send + 'static + fmt::Debug,
    {
        let _guard = TEST_MUTEX.lock().unwrap();
        tokio::runtime::Runtime::new()
            .expect("new runtime")
            .block_on(future)
            .unwrap();
    }
}
