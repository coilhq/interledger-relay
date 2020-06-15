use std::error;
use std::fmt;
use std::sync::Arc;
use std::time;

use bytes::{BufMut, Bytes, BytesMut};
use http::uri::InvalidUri;
use hyper::Uri;
use serde::Deserialize;

use crate::AuthToken;
use crate::serde::deserialize_uri;

#[derive(Clone, Debug, PartialEq)]
pub struct StaticRoute {
    pub target_prefix: Bytes,
    pub next_hop: NextHop,
    pub account: Arc<String>,
    pub failover: Option<RouteFailover>,
    /// Positive shares of the packets. For example, given the following routes
    /// to a destination.
    /// - *A*: `partition: 2.0`
    /// - *B*: `partition: 1.0`
    /// - *C*: `partition: 1.0`
    /// Assuming all three routes are available:
    /// - Route *A* receives 50% of all Prepares.
    /// - Route *B* receives 25% of all Prepares.
    /// - Route *C* receives the remaining 25% of all prepares.
    ///
    /// If the partitions of all hops to a destination sum to `1.0`, the individual
    /// partition values can be interpreted as the fraction of packets assigned.
    pub partition: f64,
}

/// Explanation of multilateral mode:
/// <https://forum.interledger.org/t/describe-multilateral-mode-in-ilp-plugin-http/456/2>
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "type")]
pub enum NextHop {
    Bilateral {
        #[serde(deserialize_with = "deserialize_uri")]
        endpoint: Uri,
        auth: Option<AuthToken>,
    },
    Multilateral {
        endpoint_prefix: Bytes,
        endpoint_suffix: Bytes,
        auth: Option<AuthToken>,
    },
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteFailover {
    pub window_size: usize,
    /// A route is marked as unhealthy when
    /// `fail_ratio <= number of failures per window / window_size`.
    pub fail_ratio: f64,
    // <https://docs.serde.rs/serde/de/trait.Deserialize.html#impl-Deserialize%3C%27de%3E-for-Duration>
    pub fail_duration: time::Duration,
}

impl StaticRoute {
    #[cfg(test)]
    pub fn new(target_prefix: Bytes, account: &str, next_hop: NextHop) -> Self {
        Self::new_with_partition(target_prefix, account, next_hop, 1.0)
    }

    #[cfg(test)]
    pub fn new_with_partition(
        target_prefix: Bytes,
        account: &str,
        next_hop: NextHop,
        partition: f64,
    ) -> Self {
        StaticRoute {
            target_prefix,
            account: Arc::new(account.to_owned()), // XXX
            next_hop,
            failover: None,
            partition,
        }
    }

    pub(crate) fn endpoint(
        &self,
        connector_addr: ilp::Addr,
        destination_addr: ilp::Addr,
    ) -> Result<Uri, RouterError> {
        match &self.next_hop {
            // `hyper::Uri` is built from `bytes::Bytes`, so this clone doesn't
            // actually allocate.
            NextHop::Bilateral { endpoint, .. } => Ok(endpoint.clone()),
            NextHop::Multilateral { endpoint_prefix, endpoint_suffix, .. } => {
                debug_assert!({
                    let dst = destination_addr.as_ref();
                    dst.starts_with(connector_addr.as_ref())
                        && dst[connector_addr.len()] == b'.'
                });
                // TODO or use the route's target prefix instead of the connector address?
                let destination_segment = match parse_address_segment(
                    &self.target_prefix,
                    destination_addr.as_ref(),
                ) {
                    Some(segment) => segment,
                    None => return Err(RouterError(ErrorKind::InvalidDestination)),
                };

                // TODO dont allocate every time (maybe have a cache of segment => uri)
                let mut uri = BytesMut::with_capacity({
                    endpoint_prefix.len()
                    + destination_segment.len()
                    + endpoint_suffix.len()
                });
                uri.put_slice(endpoint_prefix);
                uri.put_slice(destination_segment);
                uri.put_slice(endpoint_suffix);
                Ok(Uri::from_maybe_shared(uri.freeze())?)
            },
        }
    }

    #[inline]
    pub(crate) fn auth(&self) -> Option<&AuthToken> {
        match &self.next_hop {
            NextHop::Bilateral { auth, .. } => auth.as_ref(),
            NextHop::Multilateral { auth, .. } => auth.as_ref(),
        }
    }
}

#[derive(Debug)]
pub struct RouterError(ErrorKind);

#[derive(Debug)]
enum ErrorKind {
    InvalidDestination,
    InvalidUri(InvalidUri),
}

impl error::Error for RouterError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match &self.0 {
            ErrorKind::InvalidDestination => None,
            ErrorKind::InvalidUri(inner) => Some(inner),
        }
    }
}

impl fmt::Display for RouterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self.0 {
            ErrorKind::InvalidDestination => "InvalidDestination",
            ErrorKind::InvalidUri(_) => "InvalidUri",
        })
    }
}

impl From<InvalidUri> for RouterError {
    fn from(inner: InvalidUri) -> Self {
        RouterError(ErrorKind::InvalidUri(inner))
    }
}

fn parse_address_segment<'a>(target_prefix: &[u8], destination: &'a [u8])
    -> Option<&'a [u8]>
{
    debug_assert!(destination.starts_with(target_prefix));
    let segment_offset = target_prefix.len();
    destination[segment_offset..]
        .split(|&byte| byte == b'.')
        .next()
        .filter(|&segment| validate_address_segment(segment))
}

fn validate_address_segment(segment: &[u8]) -> bool {
    !segment.is_empty() && segment.iter().all(|&byte| {
        byte == b'-' || byte == b'_'
            || (b'A' <= byte && byte <= b'Z')
            || (b'a' <= byte && byte <= b'z')
            || (b'0' <= byte && byte <= b'9')
    })
}

#[cfg(test)]
mod test_static_route {
    use lazy_static::lazy_static;

    use super::*;

    lazy_static! {
        static ref BI_URI: Uri =
            "http://example.com/alice".parse::<Uri>().unwrap();

        static ref BI: StaticRoute = StaticRoute::new(
            Bytes::from("test.alice."),
            "account1",
            NextHop::Bilateral {
                endpoint: BI_URI.clone(),
                auth: Some(AuthToken::new("alice_auth")),
            },
        );

        static ref MULTI: StaticRoute = StaticRoute::new(
            Bytes::from("test.relay."),
            "account2",
            NextHop::Multilateral {
                endpoint_prefix: Bytes::from("http://example.com/bob/"),
                endpoint_suffix: Bytes::from("/ilp"),
                auth: Some(AuthToken::new("bob_auth")),
            },
        );
    }

    #[test]
    fn test_endpoint() {
        assert_eq!(
            BI.endpoint(
                ilp::Addr::new(b"test.relay"),
                ilp::Addr::new(b"test.whatever.123"),
            ).unwrap(),
            *BI_URI,
        );
        assert_eq!(
            MULTI.endpoint(
                ilp::Addr::new(b"test.relay"),
                ilp::Addr::new(b"test.relay.123.456"),
            ).unwrap(),
            "http://example.com/bob/123/ilp".parse::<Uri>().unwrap(),
        );
        assert!(MULTI.endpoint(
            ilp::Addr::new(b"test.relay"),
            ilp::Addr::new(b"test.relay.123~.456"),
        ).is_err());
    }

    #[test]
    fn test_auth() {
        assert_eq!(BI.auth(), Some(&AuthToken::new("alice_auth")));
        assert_eq!(MULTI.auth(), Some(&AuthToken::new("bob_auth")));
    }
}

#[cfg(test)]
mod test_helpers {
    use super::*;

    #[test]
    fn test_parse_address_segment() {
        assert_eq!(
            parse_address_segment(b"test.cx.", b"test.cx.alice"),
            Some(&b"alice"[..]),
        );
        assert_eq!(
            parse_address_segment(b"test.cx.", b"test.cx.alice.bob"),
            Some(&b"alice"[..]),
        );
        assert_eq!(
            parse_address_segment(b"test.cx.", b"test.cx.alice!"),
            None,
        );
    }

    #[test]
    fn test_validate_address_segment() {
        let valid = &["test", "Test_1-2-3"];
        let invalid = &["", "peer.config", "test!", "test?"];

        for segment in valid {
            assert_eq!(validate_address_segment(segment.as_bytes()), true);
        }
        for segment in invalid {
            assert_eq!(validate_address_segment(segment.as_bytes()), false);
        }
    }
}
