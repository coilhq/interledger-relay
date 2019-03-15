use std::borrow::Borrow;
use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::{Either, FutureResult, ok};
use hyper::service::Service as HyperService;
use log::warn;
use serde::de::{Deserialize, Deserializer, Error as _};

/// Verify that incoming requests have a valid token in the `Authorization` header.
#[derive(Clone, Debug)]
pub struct AuthTokenFilter<S> {
    tokens: Arc<HashSet<AuthToken>>,
    next: S,
}

impl<S> AuthTokenFilter<S>
where
    S: HyperService<
        ReqBody = hyper::Body,
        ResBody = hyper::Body,
        Error = hyper::Error,
    >,
{
    pub fn new(tokens: Vec<AuthToken>, next: S) -> Self {
        AuthTokenFilter {
            tokens: Arc::new({
                tokens
                    .into_iter()
                    .collect::<HashSet<_>>()
            }),
            next,
        }
    }
}

impl<S> HyperService for AuthTokenFilter<S>
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
        // TODO this Future never fails
        FutureResult<hyper::Response<hyper::Body>, hyper::Error>,
    >;

    fn call(&mut self, request: hyper::Request<hyper::Body>) -> Self::Future {
        let auth = request.headers().get(hyper::header::AUTHORIZATION);
        match auth {
            Some(token) if self.tokens.contains(token.as_ref()) => {
                Either::A(self.next.call(request))
            },
            _ => Either::B(ok({
                warn!("invalid authorization: authorization={:?}", auth);
                hyper::Response::builder()
                    .status(hyper::StatusCode::UNAUTHORIZED)
                    .body(hyper::Body::empty())
                    .expect("response builder error")
            })),
        }
    }
}

/// `AuthToken`s must be valid HTTP header values.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AuthToken(Bytes);

impl AuthToken {
    /// # Panics
    ///
    /// Panics if the string is not a valid auth token.
    #[cfg(test)]
    pub fn new(string: &'static str) -> Self {
        AuthToken::try_from(Bytes::from(string))
            .expect("invalid auth token")
    }

    pub fn try_from(bytes: Bytes) -> Result<Self, http::Error> {
        http::header::HeaderValue::from_shared(bytes.clone())?;
        Ok(AuthToken(bytes))
    }
}

impl Borrow<[u8]> for AuthToken {
    fn borrow(&self) -> &[u8] {
        self.0.borrow()
    }
}

impl From<AuthToken> for Bytes {
    fn from(token: AuthToken) -> Self {
        token.0
    }
}

impl<'de> Deserialize<'de> for AuthToken {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let token_str = <&str>::deserialize(deserializer)?;
        AuthToken::try_from(Bytes::from(token_str))
            .map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod test_auth_token_filter {
    use futures::prelude::*;
    use hyper::service::service_fn;

    use super::*;

    #[test]
    fn test_service() {
        let next = service_fn(|_req| ok({
            hyper::Response::builder()
                .status(200)
                .body(hyper::Body::empty())
                .unwrap()
        }));
        let mut service = AuthTokenFilter::new(
            vec![
                AuthToken::new("token_1"),
                AuthToken::new("token_2"),
            ],
            next,
        );

        // Correct token.
        assert_eq!(
            service
                .call({
                    hyper::Request::post("/")
                        .header("Authorization", "token_1")
                        .body(hyper::Body::empty())
                        .unwrap()
                })
                .wait()
                .unwrap()
                .status(),
            200,
        );

        // No token.
        assert_eq!(
            service
                .call({
                    hyper::Request::post("/")
                        .body(hyper::Body::empty())
                        .unwrap()
                })
                .wait()
                .unwrap()
                .status(),
            401,
        );

        // Incorrect token.
        assert_eq!(
            service
                .call({
                    hyper::Request::post("/")
                        .header("Authorization", "not_a_token")
                        .body(hyper::Body::empty())
                        .unwrap()
                })
                .wait()
                .unwrap()
                .status(),
            401,
        );
    }
}

#[cfg(test)]
mod test_auth_token {
    use super::*;

    #[test]
    fn test_try_from() {
        let valid_bytes = Bytes::from("test_token");
        let invalid_bytes = Bytes::from("test\ntoken");

        assert_eq!(
            AuthToken::try_from(valid_bytes.clone()).unwrap(),
            AuthToken(valid_bytes),
        );
        assert!(AuthToken::try_from(invalid_bytes).is_err());
    }
}
