use std::borrow::Borrow;
use std::collections::HashSet;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::future::{Either, Ready, ok};
use futures::task::{Context, Poll};
use hyper::service::Service as HyperService;
use log::warn;
use serde::de::{Deserialize, Deserializer, Error as _};

type HTTPRequest = http::Request<hyper::Body>;

/// Verify that incoming requests have a valid token in the `Authorization` header.
#[derive(Clone, Debug)]
pub struct AuthTokenFilter<S> {
    tokens: Arc<HashSet<AuthToken>>,
    next: S,
}

impl<S> AuthTokenFilter<S>
where
    S: HyperService<HTTPRequest>,
{
    pub fn new<I>(tokens: I, next: S) -> Self
    where
        I: IntoIterator<Item = AuthToken>,
    {
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

impl<S> HyperService<HTTPRequest> for AuthTokenFilter<S>
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
        // This Future never fails.
        Ready<Result<Self::Response, Self::Error>>,
    >;

    fn poll_ready(&mut self, context: &mut Context<'_>)
        -> Poll<Result<(), Self::Error>>
    {
       self.next.poll_ready(context)
    }

    fn call(&mut self, request: hyper::Request<hyper::Body>) -> Self::Future {
        static BEARER_PREFIX: &[u8] = b"Bearer ";
        let auth = request.headers()
            .get(hyper::header::AUTHORIZATION)
            .map(|token| {
                let token = token.as_bytes();
                if token.starts_with(BEARER_PREFIX) {
                    &token[BEARER_PREFIX.len()..]
                } else {
                    token
                }
            });
        match auth {
            Some(token) if self.tokens.contains(token) => {
                Either::Left(self.next.call(request))
            },
            _ => Either::Right(ok({
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
        // Verify that the `AuthToken` can be used an an HTTP header value.
        http::header::HeaderValue::from_maybe_shared(bytes.clone())?;
        Ok(AuthToken(bytes))
    }

    pub fn as_bytes(&self) -> Bytes {
        self.0.clone()
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
        AuthToken::try_from(BytesMut::from(token_str).freeze())
            .map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod test_auth_token_filter {
    use futures::executor::block_on;
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
            block_on(service.call({
                hyper::Request::post("/")
                    .header("Authorization", "token_1")
                    .body(hyper::Body::empty())
                    .unwrap()
            })).unwrap().status(),
            200,
        );

        // Correct token with "Bearer " prefix.
        assert_eq!(
            block_on(service.call({
                hyper::Request::post("/")
                    .header("Authorization", "Bearer token_1")
                    .body(hyper::Body::empty())
                    .unwrap()
            })).unwrap().status(),
            200,
        );

        // No token.
        assert_eq!(
            block_on({
                service.call({
                    hyper::Request::post("/")
                        .body(hyper::Body::empty())
                        .unwrap()
                })
            }).unwrap().status(),
            401,
        );

        // Incorrect token.
        assert_eq!(
            block_on(service.call({
                hyper::Request::post("/")
                    .header("Authorization", "not_a_token")
                    .body(hyper::Body::empty())
                    .unwrap()
            })).unwrap().status(),
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
