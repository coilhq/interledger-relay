use bytes::BytesMut;
use futures::prelude::*;

use super::{LimitStream, LimitStreamError};

pub fn collect_http_body(
    headers: &hyper::HeaderMap<hyper::header::HeaderValue>,
    body: hyper::Body,
    max_capacity: usize,
//) -> Result<BytesMut, LimitStreamError<hyper::Error>> {
) -> impl Future<Output =
    Result<BytesMut, LimitStreamError<hyper::Error>>
> + Send + 'static {
    // TODO return an error if the Content-Length is too large instead of just truncating?
    let capacity = std::cmp::min(
        max_capacity,
        get_content_length(headers).unwrap_or(std::usize::MAX),
    );

    collect_body(body, capacity)
}

#[cfg(test)]
pub fn collect_http_request(request: http::Request<hyper::Body>)
    -> impl Future<Output =
        Result<BytesMut, LimitStreamError<hyper::Error>>
    > + Send + 'static
{
    let (parts, body) = request.into_parts();
    collect_http_body(&parts.headers, body, std::usize::MAX)
}

#[cfg(test)]
pub fn collect_http_response(response: http::Response<hyper::Body>)
    -> impl Future<Output =
        Result<BytesMut, LimitStreamError<hyper::Error>>
    > + Send + 'static
{
    let (parts, body) = response.into_parts();
    collect_http_body(&parts.headers, body, std::usize::MAX)
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
