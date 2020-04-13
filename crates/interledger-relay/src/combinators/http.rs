use bytes::BytesMut;
use futures::prelude::*;

use super::{LimitStream, LimitStreamError};

pub fn collect_http_body(
    headers: &hyper::HeaderMap<hyper::header::HeaderValue>,
    body: hyper::Body,
    max_capacity: usize,
) -> impl Future<Output =
    Result<BytesMut, LimitStreamError<hyper::Error>>
> + Send + 'static {
    // TODO should this return an error if the Content-Length is too large instead of just truncating?
    let capacity = std::cmp::min(
        max_capacity,
        get_content_length(headers).unwrap_or(std::usize::MAX),
    );

    collect_body(body, capacity)
}

/// Missing or invalid `Content-Length`s return `0`.
fn get_content_length(headers: &hyper::HeaderMap<hyper::header::HeaderValue>)
    -> Option<usize>
{
    headers.get(hyper::header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse::<usize>()
        .ok()
}

async fn collect_body(body: hyper::Body, capacity: usize)
    -> Result<BytesMut, LimitStreamError<hyper::Error>>
{
    let mut body = LimitStream::new(capacity, body);
    let mut accum =
        if capacity == std::usize::MAX {
            BytesMut::new()
        } else {
            BytesMut::with_capacity(capacity)
        };
    while let Some(chunk) = body.try_next().await? {
        accum.extend(chunk);
    }
    Ok(accum)
}

/// Test helper.
#[cfg(test)]
pub fn collect_http_request(request: http::Request<hyper::Body>)
    -> impl Future<Output =
        Result<BytesMut, LimitStreamError<hyper::Error>>
    > + Send + 'static
{
    let (parts, body) = request.into_parts();
    collect_http_body(&parts.headers, body, std::usize::MAX)
}

/// Test helper.
#[cfg(test)]
pub fn collect_http_response(response: http::Response<hyper::Body>)
    -> impl Future<Output =
        Result<BytesMut, LimitStreamError<hyper::Error>>
    > + Send + 'static
{
    let (parts, body) = response.into_parts();
    collect_http_body(&parts.headers, body, std::usize::MAX)
}

#[cfg(test)]
mod test_http {
    use futures::executor::block_on;

    use super::*;

    #[test]
    fn test_collect_http_body() {
        let data = BytesMut::from("1234567890").freeze();

        assert_eq!(
            block_on(collect_http_body(
                &make_headers("10"),
                hyper::Body::from(data.clone()),
                1000,
            )).unwrap().freeze(),
            data,
        );

        // Exceeded Content-Length.
        assert!(matches!(
            block_on(collect_http_body(
                &make_headers("9"),
                hyper::Body::from(data.clone()),
                1000,
            )),
            Err(LimitStreamError::LimitExceeded)
        ));

        // Exceeded `max_capacity`.
        assert!(matches!(
            block_on(collect_http_body(
                &make_headers("10"),
                hyper::Body::from(data.clone()),
                9,
            )),
            Err(LimitStreamError::LimitExceeded)
        ));
    }

    #[test]
    fn test_get_content_length() {
        let valid_header = make_headers("123");
        let invalid_header = make_headers("foo");
        let empty_header = hyper::HeaderMap::new();

        assert_eq!(get_content_length(&valid_header), Some(123));
        assert_eq!(get_content_length(&invalid_header), None);
        assert_eq!(get_content_length(&empty_header), None);
    }

    fn make_headers(content_length: &str)
        -> hyper::HeaderMap<hyper::header::HeaderValue>
    {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(
            hyper::header::CONTENT_LENGTH,
            content_length.parse().unwrap(),
        );
        headers
    }
}
