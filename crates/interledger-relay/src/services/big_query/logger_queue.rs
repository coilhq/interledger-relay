use std::sync::{Arc, Mutex};

use log::{trace, warn};

use super::{BigQueryTable, LoggerConfig};
use super::table::Row;

#[derive(Clone, Debug)]
pub struct LoggerQueue<D> {
    config: Arc<LoggerConfig>,
    table: BigQueryTable,
    data: Arc<Mutex<LoggerData<D>>>,
}

/// There is a hard maximum of 10,000 rows-per-request.
///
/// See: <https://cloud.google.com/bigquery/quotas#streaming_inserts>
const MAXIMUM_BATCH_CAPACITY: usize = 10_000;

#[derive(Debug)]
struct LoggerData<D> {
    queue: Vec<Row<D>>,
    insert: Option<tokio::task::JoinHandle<()>>,
}

impl<D> LoggerQueue<D>
where
    D: 'static + Clone + Send + Sync + serde::Serialize,
{
    pub fn new(config: Arc<LoggerConfig>, table: BigQueryTable) -> Self {
        debug_assert!(config.batch_capacity <= MAXIMUM_BATCH_CAPACITY);
        let queue = Vec::with_capacity(config.batch_capacity);
        LoggerQueue {
            config,
            table,
            data: Arc::new(Mutex::new(LoggerData {
                queue,
                insert: None,
            })),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.data.try_lock()
            .map(|data| data.insert.is_none())
            .unwrap_or(false)
    }

    /// Returns an error when the queue is busy.
    pub fn try_write(&self, row: Row<D>) -> Result<(), Row<D>> {
        let mut data = match self.data.try_lock() {
            Ok(data) => data,
            Err(_error) => return Err(row),
        };
        if data.insert.is_some() {
            return Err(row);
        }

        data.queue.push(row);
        if self.is_queue_full(data.queue.len()) {
            data.insert = Some(tokio::spawn({
                self.clone().flush(std::mem::take(&mut data.queue))
            }));
        }
        Ok(())
    }

    pub fn flush_now(self) {
        let mut data = self.data.lock().unwrap();
        if data.insert.is_some() { return; }
        if data.queue.is_empty() { return; }
        data.insert = Some(tokio::spawn({
            self.clone().flush(std::mem::take(&mut data.queue))
        }));
    }

    async fn flush(self, rows: Vec<Row<D>>) {
        let count = rows.len();
        trace!("flush start: total_rows={}", count);
        let self_2 = self.clone();
        let result = self.table.clone()
            .insert_all(rows)
            .await;
        let mut data = self_2.data.lock().unwrap();
        debug_assert!(data.queue.is_empty());
        data.insert = None;
        // TODO maybe retry immediately if all failed?

        match result {
            Ok(()) => {},
            Err(error) => {
                warn!(
                    "flush insert_all error: error={:?} retries={} total_rows={}",
                    error.error, error.retries.len(), count,
                );
                debug_assert!(!error.retries.is_empty());
                debug_assert!(data.queue.is_empty());
                data.queue = error.retries;
            },
        }
    }

    fn is_queue_full(&self, queue_len: usize) -> bool {
        self.config.batch_capacity <= queue_len
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.data
            .lock()
            .unwrap()
            .queue
            .len()
    }

    pub fn is_idle(&self) -> bool {
        let data = self.data.lock().unwrap();
        data.queue.is_empty() && data.insert.is_none()
    }
}

#[cfg(test)]
mod test_logger_queue {
    use std::time;

    use futures::prelude::*;
    use lazy_static::lazy_static;

    use crate::testing;
    use super::*;
    use super::super::{BigQueryClient, BigQueryConfig};
    use super::super::table::{InsertAllRequest, InsertAllResponse, InsertError};

    lazy_static! {
        static ref CONFIG: Arc<LoggerConfig> = Arc::new(LoggerConfig {
            queue_count: 2,
            batch_capacity: 3,
            flush_interval: time::Duration::from_secs(1),
            big_query: BigQueryConfig {
                origin: testing::RECEIVER_ORIGIN.to_owned(),
                project_id: "PROJECT_ID".to_owned(),
                dataset_id: "DATASET_ID".to_owned(),
                table_id: "TABLE_ID".to_owned(),
                service_account_key_file: None,
            },
        });

        static ref TABLE: BigQueryTable = BigQueryTable::new(
            &CONFIG.big_query,
            Arc::new(BigQueryClient::new(None)),
        );

        static ref ROWS: Vec<Row<i32>> = (0..7)
            .map(|i| Row::new(i))
            .collect::<Vec<_>>();
    }

    #[test]
    fn test_is_ready() {
        let queue = LoggerQueue::<i32>::new(CONFIG.clone(), TABLE.clone());
        assert!(queue.is_ready());
    }

    #[test]
    fn test_flush_no_retries() {
        let queue = LoggerQueue::new(CONFIG.clone(), TABLE.clone());
        testing::MockServer::new()
            .test_body(|body| test_body(body, &[0, 1, 2]))
            .with_response(|| make_response(&[]))
            .run(futures::future::ready(()).then(move |_| {
                // The `futures::ready().then` is to ensure that `try_write` runs
                // within `MockServer`'s tokio `Runtime`.
                for i in 0..3 {
                    queue.try_write(ROWS[i].clone()).unwrap();
                }
                assert!(!queue.is_ready());
                assert_eq!(
                    queue.try_write(ROWS[3].clone()).unwrap_err(),
                    ROWS[3].clone(),
                );
                queue.data
                    .lock()
                    .unwrap()
                    .insert
                    .take()
                    .unwrap()
                    .map(|result| result.unwrap())
            }));
    }

    #[test]
    fn test_flush_with_retries() {
        let queue = LoggerQueue::new(CONFIG.clone(), TABLE.clone());
        testing::MockServer::new()
            .test_body(|body| test_body(body, &[0, 1, 2]))
            .with_response(|| make_response(&[1]))
            .run(futures::future::ready(()).then(move |_| {
                for i in 0..3 {
                    queue.try_write(ROWS[i].clone()).unwrap();
                }
                let insert = queue.data
                    .lock()
                    .unwrap()
                    .insert
                    .take()
                    .unwrap();
                insert.map(move |_| {
                    assert!(queue.is_ready());
                    let data = queue.data.lock().unwrap();
                    assert_eq!(data.queue.len(), 1);
                    assert!(data.insert.is_none());
                })
            }))
    }

    fn test_body(body: bytes::Bytes, rows: &[usize]) {
        assert_eq!(
            body.as_ref(),
            serde_json::to_vec(&InsertAllRequest {
                rows: rows.iter()
                    .map(|index| ROWS[*index].clone())
                    .collect::<Vec<_>>()
                    .as_slice(),
            }).unwrap().as_slice(),
        );
    }

    fn make_response(retries: &[u32]) -> hyper::Response<hyper::Body> {
        hyper::Response::builder()
            .status(200)
            .body(hyper::Body::from({
                serde_json::to_vec(&InsertAllResponse {
                    insert_errors: retries
                        .iter()
                        .copied()
                        .map(|index| InsertError {
                            index,
                            errors: Vec::new(),
                        })
                        .collect::<Vec<_>>(),
                }).unwrap()
            }))
            .unwrap()
    }
}
