// `batch_in_progress` (see note on field)
#![allow(clippy::mutex_atomic)]

use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time;

use log::{trace, warn};

use super::{BigQueryConfig, BigQueryTable};
use super::table::Row;

#[derive(Clone, Debug)]
pub struct Logger<D> {
    data: Arc<LoggerData<D>>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggerConfig {
    /// 500 rows/request recommended in
    /// <https://cloud.google.com/bigquery/quotas#streaming_inserts>.
    #[serde(default = "default_batch_capacity")]
    pub batch_capacity: usize,
    //#[serde(default = "default_retry_interval")]
    //pub retry_interval: time::Duration,
    #[serde(default = "default_flush_interval")]
    pub flush_interval: time::Duration,
    #[serde(flatten)]
    pub big_query: BigQueryConfig,
}

fn default_batch_capacity() -> usize { 500 }
//fn default_retry_interval() -> time::Duration { time::Duration::from_secs(5) }
fn default_flush_interval() -> time::Duration { time::Duration::from_secs(1) }

/// There is a hard maximum of 10,000 rows-per-request.
///
/// See: <https://cloud.google.com/bigquery/quotas#streaming_inserts>
const MAXIMUM_BATCH_CAPACITY: usize = 10_000;

#[derive(Debug)]
struct LoggerData<D> {
    config: LoggerConfig,
    queue: RwLock<Vec<Row<D>>>,
    batch: Mutex<Vec<Row<D>>>,
    /// Note that even when this field is `true`, `batch` may be empty while the
    /// `insert_all` is in flight. When the `insert_all` finishes, it will either:
    ///
    ///   * Succeed: `batch_in_progress` is set to `false`.
    ///   * Fail: The rows that need retrying are added to `batch`.
    ///
    /// Clippy suggests that this should be either an `AtomicBool` or a `Mutex<()>`.
    /// `AtomicBool` doesn't work because the flag is used to manage a `Condvar`.
    /// `Mutex<()>` doesn't work because `batch`'s length doesn't necessarily
    /// indicate whether an insert is in progress (see above).
    batch_in_progress: Mutex<bool>,
    batch_signal: Condvar,
    table: BigQueryTable,
}

impl<D> Logger<D>
where
    D: 'static + Clone + Send + Sync + serde::Serialize,
{
    pub fn new(config: LoggerConfig) -> Self {
        debug_assert!(config.batch_capacity <= MAXIMUM_BATCH_CAPACITY);
        let queue = Vec::with_capacity(config.batch_capacity);
        let batch = Vec::with_capacity(config.batch_capacity);
        let table = BigQueryTable::new(&config.big_query);
        Logger {
            data: Arc::new(LoggerData {
                config,
                queue: RwLock::new(queue),
                batch: Mutex::new(batch),
                batch_in_progress: Mutex::new(false),
                batch_signal: Condvar::new(),
                table,
            }),
        }
    }

    pub fn is_available(&self) -> bool {
        !self.is_queue_full(self.data.queue.read().unwrap().len())
    }

    // Be sure to verify `is_available` before calling `write`.
    pub fn write(&self, row: Row<D>) {
        let mut queue = self.data.queue.write().unwrap();
        queue.push(row);
        let is_queue_full = self.is_queue_full(queue.len());
        if is_queue_full {
            let mut batch_in_progress = self.data.batch_in_progress.lock().unwrap();
            if *batch_in_progress {
                warn!("queue is full while batch in progress");
                return;
            }
            *batch_in_progress = true;

            let mut batch = self.data.batch.lock().unwrap();
            // `batch` should always be empty at this point. But if for some
            // reason it isn't, don't swap those rows back to the queue.
            debug_assert!(batch.is_empty());
            if batch.is_empty() {
                std::mem::swap(&mut *queue, &mut *batch);
            } else {
                while let Some(row) = queue.pop() {
                    batch.push(row);
                }
            }
            self.data.batch_signal.notify_one();
        }
    }

    pub fn start(&self) -> tokio::task::JoinHandle<()> {
        let self_2 = self.clone();
        tokio::spawn(async move {
            loop {
                self_2.clone().flush().await;
            }
        })
    }

    async fn flush(self) {
        let batch = self.get_next_batch();
        let count = batch.len();
        trace!("flush: total_rows={}", count);
        let self_2 = self.clone();
        let self_3 = self.clone();
        let result = self.data.table.clone()
            .insert_all(batch)
            .await;
        match result {
            Ok(()) => {
                let mut batch_in_progress =
                    self_2.data.batch_in_progress.lock().unwrap();
                *batch_in_progress = false;
            },
            Err(error) => {
                warn!(
                    "flush: BigQuery::insert_all error: error={:?} retries={} total_rows={}",
                    error.error, error.retries.len(), count,
                );
                let mut batch = self_3.data.batch.lock().unwrap();
                debug_assert!(!error.retries.is_empty());
                // `batch` should still be empty since `batch_in_progress` is
                // `true`, which prevents `write` from modifying it.
                debug_assert!(batch.is_empty());
                *batch = error.retries;
            },
        }
    }

    /// That this will never return an empty batch. Instead, it will stall until
    /// there is at least one row.
    fn get_next_batch(&self) -> Vec<Row<D>> {
        loop {
            let mut ready = self.data.batch_in_progress.lock().unwrap();
            while !*ready {
                let result = self.data.batch_signal.wait_timeout(
                    ready,
                    self.data.config.flush_interval,
                ).unwrap();
                ready = result.0;
                if *ready || result.1.timed_out() { break; }
            }
            let mut batch = self.data.batch.lock().unwrap();
            if batch.is_empty() { continue; }
            return std::mem::take(&mut batch);
        }
    }

    fn is_queue_full(&self, queue_len: usize) -> bool {
        self.data.config.batch_capacity <= queue_len
    }

    #[cfg(test)]
    fn is_batch_in_progress(&self) -> bool {
        *self.data.batch_in_progress.lock().unwrap()
    }
}

#[cfg(test)]
mod test_logger {
    use lazy_static::lazy_static;

    use crate::testing;
    use super::*;
    use super::super::table::{InsertAllRequest, InsertAllResponse, InsertError};

    lazy_static! {
        static ref CONFIG: LoggerConfig = LoggerConfig {
            batch_capacity: 3,
            flush_interval: time::Duration::from_secs(1),
            big_query: BigQueryConfig {
                origin: testing::RECEIVER_ORIGIN.to_owned(),
                api_key: "API_KEY".to_owned(),
                project_id: "PROJECT_ID".to_owned(),
                dataset_id: "DATASET_ID".to_owned(),
                table_id: "TABLE_ID".to_owned(),
            },
        };

        static ref ROWS: Vec<Row<i32>> = (0..7)
            .map(|i| Row::new(i))
            .collect::<Vec<_>>();
    }

    #[test]
    fn test_is_available() {
        let logger = Logger::new(CONFIG.clone());
        for i in 0..5 {
            logger.write(ROWS[i].clone());
            assert!(logger.is_available());
        }
        logger.write(ROWS[5].clone());
        assert!(!logger.is_available());
    }

    #[test]
    fn test_flush_no_retries() {
        let logger = Logger::new(CONFIG.clone());
        for i in 0..3 {
            logger.write(ROWS[i].clone());
        }
        assert!(logger.is_available());
        assert!(logger.data.queue.read().unwrap().is_empty());
        assert_eq!(logger.data.batch.lock().unwrap().len(), 3);
        testing::MockServer::new()
            .test_body(|body| test_body(body, &[0, 1, 2]))
            .with_response(|| make_response(&[]))
            .run(logger.clone().flush());
        assert!(logger.data.batch.lock().unwrap().is_empty());
    }

    #[test]
    fn test_flush_with_retries() {
        let logger = Logger::new(CONFIG.clone());
        for i in 0..3 {
            logger.write(ROWS[i].clone());
        }
        testing::MockServer::new()
            .test_body(|body| test_body(body, &[0, 1, 2]))
            .with_response(|| make_response(&[1]))
            .run(logger.clone().flush());
        assert!(logger.is_batch_in_progress());
        assert_eq!(&*logger.data.batch.lock().unwrap(), &[ROWS[1].clone()]);

        // There is a retry queued in `batch`, so only `queue` can be filled up
        // before the logger becomes unavailable.
        logger.write(ROWS[3].clone());
        logger.write(ROWS[4].clone());
        assert!(logger.is_available());
        logger.write(ROWS[5].clone());
        assert!(!logger.is_available());

        // Filling the `queue` doesn't fill the batch because a retry is still
        // in progress.
        assert_eq!(logger.data.queue.read().unwrap().len(), 3);
        assert_eq!(logger.data.batch.lock().unwrap().len(), 1);
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
