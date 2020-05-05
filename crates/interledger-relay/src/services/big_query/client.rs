use futures::future::Either;
use futures::prelude::*;

use crate::combinators::{self, LimitStreamError};

type HyperClient = hyper::Client<
    hyper_tls::HttpsConnector<hyper::client::HttpConnector>,
    hyper::Body,
>;

#[derive(Debug)]
pub struct BigQueryClient {
    hyper: HyperClient,
}

#[derive(Debug)]
pub enum BigQueryError {
    HTTP(http::Error),
    Hyper(hyper::Error),
    StatusCode(hyper::StatusCode),
    ResponseTooLarge,
    Serde(serde_json::Error),
    PartialError,
}

impl BigQueryClient {
    pub fn new() -> Self {
        let agent = hyper_tls::HttpsConnector::new();
        let client = hyper::Client::builder().build(agent);
        BigQueryClient {
            hyper: client,
        }
    }

    pub fn request<Resp>(&self, request: hyper::Request<hyper::Body>)
        -> impl Future<Output = Result<Resp, BigQueryError>>
            + Send + 'static
    where
        Resp: for<'q> serde::Deserialize<'q> + Send + 'static,
    {
        self.hyper
            .request(request)
            .map_err(BigQueryError::Hyper)
            .and_then(|response| {
                if response.status() != hyper::StatusCode::OK {
                    return Either::Right(future::err({
                        BigQueryError::StatusCode(response.status())
                    }));
                }

                let (parts, body) = response.into_parts();
                Either::Left(combinators::collect_http_body(
                    &parts.headers,
                    body,
                    std::usize::MAX,
                ).map_err(limit_to_big_query_error))
            })
            .and_then(|body| future::ready({
                serde_json::from_slice::<Resp>(&body)
                    .map_err(BigQueryError::Serde)
            }))
    }
}

fn limit_to_big_query_error(limit_error: LimitStreamError<hyper::Error>)
    -> BigQueryError
{
    match limit_error {
        LimitStreamError::LimitExceeded =>
            BigQueryError::ResponseTooLarge,
        LimitStreamError::StreamError(inner) =>
            BigQueryError::Hyper(inner),
    }
}

//impl std::fmt::Display for BigQueryError {
//}
