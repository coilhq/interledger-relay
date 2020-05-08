use futures::future::Either;
use futures::prelude::*;
use yup_oauth2 as oauth2;

use crate::combinators::{self, LimitStreamError};

type HyperClient = hyper::Client<
    hyper_tls::HttpsConnector<hyper::client::HttpConnector>,
    hyper::Body,
>;

type Authenticator = oauth2::authenticator::Authenticator<
    <yup_oauth2::authenticator::DefaultHyperClient
        as yup_oauth2::authenticator::HyperClientBuilder>::Connector
>;

pub struct BigQueryClient {
    hyper: HyperClient,
    authenticator: Option<Authenticator>,
}

#[derive(Debug)]
pub enum BigQueryError {
    HTTP(http::Error),
    Hyper(hyper::Error),
    StatusCode(hyper::StatusCode),
    ResponseTooLarge,
    Serde(serde_json::Error),
    PartialError,
    OAuth(oauth2::Error),
}

impl BigQueryClient {
    pub fn new(authenticator: Option<Authenticator>) -> Self {
        let agent = hyper_tls::HttpsConnector::new();
        let client = hyper::Client::builder().build(agent);
        BigQueryClient {
            hyper: client,
            authenticator,
        }
    }

/*
    pub fn set_authenticator(
        &mut self,
        authenticator: oauth2::ServiceAccountAuthenticator,
    ) {
        debug_assert!(self.authenticator.is_none());
        self.authenticator = Some(authenticator);
    }
*/

    pub async fn token(&self) -> Result<Option<oauth2::AccessToken>, oauth2::Error> {
        static SCOPES: &[&str] =
            &["https://www.googleapis.com/auth/bigquery"];
        Ok(if let Some(authenticator) = &self.authenticator {
            let token = authenticator.token(SCOPES).await?;
            Some(token)
        } else {
            None
        })
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

impl std::fmt::Debug for BigQueryClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter
            .debug_struct("BigQueryClient")
            .field("hyper", &self.hyper)
            .finish()
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
