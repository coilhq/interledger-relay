// `batch_in_progress` (see note on field)
#![allow(clippy::mutex_atomic)]

use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time;

use log::{trace, warn};

use super::{BigQueryConfig, BigQueryTable};
use super::table::Row;

#[derive(Clone, Debug)]
pub struct Logger<D> {
    config: LoggerConfig,
    table: BigQueryTable,
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
    queue: Vec<Row<D>>,
    insert: Option<tokio::task::JoinHandle<()>>,
    last_flash: time::Instant,
}

impl<D> Logger<D>
where
    D: 'static + Clone + Send + Sync + serde::Serialize,
{
    pub fn new(config: LoggerConfig, table: ) -> Self {
        debug_assert!(config.batch_capacity <= MAXIMUM_BATCH_CAPACITY);
        let queue = Vec::with_capacity(config.batch_capacity);
        //let batch = Vec::with_capacity(config.batch_capacity);
        //let table = BigQueryTable::new(&config.big_query);
        Logger {
            config,
            table,
            data: Arc::new(LoggerData {
                queue: RwLock::new(queue),
                insert: None,
                //batch: Mutex::new(batch),
                //batch_in_progress: Mutex::new(false),
                //batch_signal: Condvar::new(),
            }),
        }
    }

    /// Returns an error when the queue is busy.
    pub fn try_write(&self, row: Row<D>) -> Result<(), ()> {
        let mut data = self.try_lock().map_err(|_err| ())?;
        //let mut queue = self.queue.try_write().map_err(|| ())?;
        data.queue.push(row);
        if self.is_queue_full(data.queue.len()) {
            data.insert = Some(tokio::spawn({
                self.clone().flush(std::mem::take(&mut data.queue))
            }));
            //async move {
            //    self.flush().await
            //}));
        }
        Ok(())
    }

    pub fn start(&self) -> tokio::task::JoinHandle<()> {
        let self_2 = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::delay_until({
                    time::Instant::now() + self.config.flush_interval
                }).await;

                //self_2.clone().flush().await;
                let mut data = self_2.data.lock().unwrap();
                if data.insert.is_empty() { continue; }

                let flush_at = data.last_flash + self_2.config.flush_interval;
                if flush_at < time::Instant::now() {
                    data.insert = Some(tokio::spawn({
                        self.flush(std::mem::take(&mut data.queue))
                    }));
                }
            }
        })
    }

    async fn flush(self, rows: Vec<Row<D>>) {
        let count = rows.len();
        trace!("flush: total_rows={}", count);
        let self_2 = self.clone();
        //let self_3 = self.clone();
        let result = self.table.clone()
            .insert_all(rows)
            .await;
        let mut data = self_2.data.lock().unwrap();
        debug_assert!(data.queue.is_empty());
        data.insert = None;
        data.flush_at = time::Instant::now();
        // TODO retry immediately if all failed?

        match result {
            Ok(()) => {},
            Err(error) => {
                warn!(
                    "flush: insert_all error: error={:?} retries={} total_rows={}",
                    error.error, error.retries.len(), count,
                );
                debug_assert!(!error.retries.is_empty());
                debug_assert!(batch.is_empty());
                data.queue = error.retries;
            },
        }
    }

/*
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
*/

    fn is_queue_full(&self, queue_len: usize) -> bool {
        self.data.config.batch_capacity <= queue_len
    }

    /*
    #[cfg(test)]
    fn is_batch_in_progress(&self) -> bool {
        *self.data.batch_in_progress.lock().unwrap()
    }
    */
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
