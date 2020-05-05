use std::sync::Arc;
use std::time;

use futures::future::Either;
use futures::prelude::*;
use log::{trace, warn};

use super::{BigQueryClient, BigQueryError};

/// See: <https://cloud.google.com/bigquery/docs/reference/rest/>
#[derive(Clone, Debug)]
pub struct BigQueryTable {
    client: Arc<BigQueryClient>,
    api_key: Arc<String>,
    //get_table_uri: hyper::Uri,
    insert_all_uri: hyper::Uri,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BigQueryConfig {
    #[serde(default = "default_origin")]
    pub origin: String,
    pub api_key: String,
    pub project_id: String,
    pub dataset_id: String,
    pub table_id: String,
    //pub queue_capacity: usize,
}

fn default_origin() -> String { "https://bigquery.googleapis.com".to_owned() }

impl BigQueryTable {
    pub fn new(config: &BigQueryConfig) -> Self {
        BigQueryTable {
            client: Arc::new(BigQueryClient::new()),
            api_key: Arc::new(config.api_key.clone()),
            //get_table_uri: config.get_table_uri().unwrap(),
            // XXX unwrap
            insert_all_uri: config.insert_all_uri().unwrap(),
        }
    }

    /*
    pub async fn exists(&self) -> Result<bool, BigQueryError> {
        let request = hyper::Request::builder()
            .method(hyper::Method::GET)
            .uri(&self.get_table_uri)
            .header(hyper::header::ACCEPT, "application/json")
            .body(hyper::Body::empty())
            .map_err(BigQueryError::HTTP)?;
        self.client
            .request::<GetTableResponse>(request)
            .map_ok(|_response| true)
            .await
    }
    */
}

/*
/// <https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/get>
#[derive(Debug, serde::Deserialize)]
struct GetTableResponse {
    id: String,
    // And many more... see <https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table>
}
*/

/// <https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll#request-body>
#[derive(Debug, PartialEq, serde::Serialize)]
pub(super) struct InsertAllRequest<'a, D> {
    pub rows: &'a [Row<D>]
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Row<D> {
    pub insert_id: uuid::Uuid,
    pub json: D,
}

/// <https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll#response-body>
#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct InsertAllResponse {
    pub insert_errors: Vec<InsertError>,
}

#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub(super) struct InsertError {
    pub index: u32,
    pub errors: Vec<ErrorProto>,
}

/// <https://cloud.google.com/bigquery/docs/reference/rest/v2/ErrorProto>
#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub(super) struct ErrorProto {
    pub reason: String,
    //location: String,
    //debug_info: String,
    pub message: String,
}

#[derive(Debug)]
pub struct InsertAllError<D> {
    pub retries: Vec<Row<D>>,
    pub error: BigQueryError,
}

macro_rules! try_insert_all {
    ($rows:expr, $future:expr) => {
        match $future {
            Ok(ok) => ok,
            Err(error) => return Either::Right(future::err({
                InsertAllError::new($rows, error)
            })),
        }
    };
}

impl BigQueryTable {
    /// See:
    ///
    ///   * <https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll>
    ///   * <https://github.com/googleapis/nodejs-bigquery/blob/ea3d7afe18f8f22c6541043c92c26625ae9e0e85/src/table.ts#L1905>
    ///
    pub fn insert_all<D>(self, rows: Vec<Row<D>>)
        -> impl Future<Output = Result<(), InsertAllError<D>>>
            + Send + 'static
    where
        D: serde::Serialize + Clone + Send + Sync + 'static,
    {
        trace!("insert_all begin: rows={}", rows.len());
        let json = try_insert_all!(rows,
            serde_json::to_string(&InsertAllRequest { rows: &rows })
                .map_err(BigQueryError::Serde));
        let request = try_insert_all!(rows, hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(&self.insert_all_uri)
            .header(hyper::header::ACCEPT, "application/json")
            .header(hyper::header::CONTENT_LENGTH, json.len())
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(hyper::Body::from(json))
            .map_err(BigQueryError::HTTP));
        let start = time::Instant::now();
        Either::Left(self.client
            .request::<InsertAllResponse>(request)
            .then(move |response_result| {
                let elapsed = time::Instant::now() - start;
                let response = match response_result {
                    Ok(response) => response,
                    Err(error) => {
                        warn!(
                            "insert_all error: elapsed={:?} error={:?} rows={}",
                            elapsed, error, rows.len(),
                        );
                        return future::err(InsertAllError::new(rows, error));
                    },
                };
                if response.insert_errors.is_empty() {
                    trace!(
                        "insert_all success: elapsed={:?} rows={:?}",
                        elapsed, rows.len(),
                    );
                    return future::ok(());
                }

                warn!(
                    "insert_all partial error: elapsed={:?} errors={} errors[0]={:?}",
                    elapsed,
                    response.insert_errors.len(),
                    &response.insert_errors[0],
                );
                let mut retries = Vec::with_capacity(response.insert_errors.len());
                retries.extend({
                    response.insert_errors
                        .iter()
                        .map(|error| rows[error.index as usize].clone())
                });
                future::err(InsertAllError::new(retries, BigQueryError::PartialError))
            }))
    }
}

impl BigQueryConfig {
    /*
    pub(crate) fn get_table_uri(&self)
        -> Result<hyper::Uri, http::uri::InvalidUri>
    {
        format!(
            "{}/bigquery/v2/projects/{}/datasets/{}/tables/{}",
            self.origin,
            self.project_id,
            self.dataset_id,
            self.table_id,
        ).parse()
    }
    */

    pub(crate) fn insert_all_uri(&self)
        -> Result<hyper::Uri, http::uri::InvalidUri>
    {
        use percent_encoding::{NON_ALPHANUMERIC, percent_encode};
        const CHARS: &percent_encoding::AsciiSet = &NON_ALPHANUMERIC.remove(b'_');
        format!(
            "{}/bigquery/v2/projects/{}/datasets/{}/tables/{}/insertAll?key={}",
            self.origin,
            percent_encode(self.project_id.as_bytes(), CHARS),
            percent_encode(self.dataset_id.as_bytes(), CHARS),
            percent_encode(self.table_id.as_bytes(), CHARS),
            percent_encode(self.api_key.as_bytes(), CHARS),
        ).parse()
    }
}

impl<D> InsertAllError<D> {
    fn new(retries: Vec<Row<D>>, error: BigQueryError) -> Self {
        InsertAllError { retries, error }
    }
}

impl<D> Row<D> {
    pub fn new(json: D) -> Self {
        Row { insert_id: uuid::Uuid::new_v4(), json }
    }
}

#[cfg(test)]
mod test_big_query_table {
    use lazy_static::lazy_static;

    use crate::testing;
    use super::*;

    lazy_static! {
        static ref CONFIG: BigQueryConfig = BigQueryConfig {
            origin: testing::RECEIVER_ORIGIN.to_owned(),
            api_key: "API_KEY".to_owned(),
            project_id: "PROJECT_ID".to_owned(),
            dataset_id: "DATASET_ID".to_owned(),
            table_id: "TABLE_ID".to_owned(),
            //batch_capacity: 3,
            //queue_capacity: 6,
        };

        static ref ROWS: Vec<Row<i32>> =
            vec![Row::new(1), Row::new(2), Row::new(3)];
    }

    #[test]
    fn test_insert_all_ok() {
        let table = BigQueryTable::new(&CONFIG);
        testing::MockServer::new()
            .test_request(|request| {
                assert_eq!(request.method(), hyper::Method::POST);
                assert_eq!(
                    request.uri().path(),
                    "/bigquery/v2/projects/PROJECT_ID/datasets/DATASET_ID/tables/TABLE_ID/insertAll",
                );
                assert_eq!(request.uri().query(), Some("key=API_KEY"));
            })
            .test_body(|body| {
                assert_eq!(
                    body.as_ref(),
                    serde_json::to_vec(&InsertAllRequest { rows: &ROWS })
                        .unwrap()
                        .as_slice(),
                );
            })
            .with_response(|| {
                hyper::Response::builder()
                    .status(200)
                    .body(hyper::Body::from({
                        serde_json::to_vec(&InsertAllResponse {
                            insert_errors: vec![],
                        }).unwrap()
                    }))
                    .unwrap()
            })
            .run({
                table
                    .insert_all(ROWS.clone())
                    .map(Result::unwrap)
            });
    }

    #[test]
    fn test_insert_all_partial_error() {
        let table = BigQueryTable::new(&CONFIG);
        testing::MockServer::new()
            .with_response(|| {
                hyper::Response::builder()
                    .status(200)
                    .body(hyper::Body::from({
                        serde_json::to_vec(&InsertAllResponse {
                            insert_errors: vec![
                                InsertError { index: 1, errors: vec![] },
                            ],
                        }).unwrap()
                    }))
                    .unwrap()
            })
            .run({
                table
                    .insert_all(ROWS.clone())
                    .map(|result| {
                        assert_eq!(
                            result.unwrap_err().retries,
                            vec![ROWS[1].clone()],
                        );
                    })
            });
    }

    #[test]
    fn test_insert_all_total_error() {
        let table = BigQueryTable::new(&CONFIG);
        testing::MockServer::new()
            .with_response(|| {
                hyper::Response::builder()
                    .status(500)
                    .body(hyper::Body::empty())
                    .unwrap()
            })
            .run({
                table
                    .insert_all(ROWS.clone())
                    .map(|result| {
                        assert_eq!(
                            result.unwrap_err().retries,
                            ROWS.clone(),
                        );
                    })
            });
    }
}
