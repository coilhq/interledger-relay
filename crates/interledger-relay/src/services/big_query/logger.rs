use std::sync::{Arc, Mutex};

use log::info;
use yup_oauth2 as oauth2;

use super::{BigQueryClient, BigQueryConfig, BigQueryTable, LoggerQueue};
use super::table::Row;

#[derive(Debug)]
pub struct Logger<D> {
    queues: Vec<LoggerQueue<D>>,
    /// The overflow is only used when `is_available` returns `true` before the
    /// write, but all of the sub-queues refuse the row, so it needs somewhere to go.
    overflow: Mutex<Vec<Row<D>>>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggerConfig {
    pub queue_count: usize,
    /// 500 rows/request recommended in
    /// <https://cloud.google.com/bigquery/quotas#streaming_inserts>.
    #[serde(default = "default_batch_capacity")]
    pub batch_capacity: usize,
    #[serde(flatten)]
    pub big_query: BigQueryConfig,
}

fn default_batch_capacity() -> usize { 500 }
//fn default_retry_interval() -> time::Duration { time::Duration::from_secs(5) }
//fn default_flush_interval() -> time::Duration { time::Duration::from_secs(1) }

impl<D> Logger<D>
where
    D: 'static + Clone + Send + Sync + serde::Serialize,
{
    pub async fn new(config: LoggerConfig) -> Result<Self, oauth2::Error> {
        debug_assert_ne!(config.queue_count, 0);

        let authenticator = match &config.big_query.service_account_key_file {
            Some(sa_key_file) => Some({
                let sa_key =
                    oauth2::read_service_account_key(sa_key_file).await?;
                oauth2::ServiceAccountAuthenticator::builder(sa_key)
                    .build()
                    .await?
            }),
            None => None,
        };
        let client = BigQueryClient::new(authenticator);
        let client = Arc::new(client);

        let table = BigQueryTable::new(&config.big_query, client);
        let config = Arc::new(config);
        let queues = (0..config.queue_count)
            .map(|_i| LoggerQueue::new(config.clone(), table.clone()))
            .collect::<Vec<_>>();
        Ok(Logger {
            queues,
            overflow: Mutex::new(Vec::new()),
        })
    }

    pub fn queues(&self) -> &[LoggerQueue<D>] {
        &self.queues
    }

    pub fn is_dummy(&self) -> bool {
        self.queues.is_empty()
    }

    pub fn is_available(&self) -> bool {
        if self.is_dummy() { return true; }
        self.queues
            .iter()
            .any(LoggerQueue::is_ready)
    }

    pub fn write(&self, row: Row<D>) {
        if self.is_dummy() { return; }
        if let Err(row) = self.try_write(row) {
            let mut overflow = self.overflow.lock().unwrap();
            overflow.push(row);
        }
    }

    /// Move as many rows as possible from the overflow to queues.
    pub fn clean(&self) {
        let mut overflow = self.overflow.lock().unwrap();
        while let Some(row) = overflow.pop() {
            match self.try_write(row) {
                Ok(()) => continue,
                Err(row) => {
                    overflow.push(row);
                    break;
                },
            }
        }
        if !overflow.is_empty() {
            info!("non-empty overflow: len={}", overflow.len());
        }
    }

    fn try_write(&self, mut row: Row<D>) -> Result<(), Row<D>> {
        for queue in &self.queues {
            let result = queue.try_write(row);
            match result {
                Ok(_) => return Ok(()),
                Err(row2) => row = row2,
            }
        }
        Err(row)
    }
}

impl<D> Default for Logger<D> {
    fn default() -> Self {
        Logger {
            queues: Vec::new(),
            overflow: Mutex::new(Vec::new()),
        }
    }
}

#[cfg(test)]
mod test_logger {
    use futures::executor::block_on;
    use lazy_static::lazy_static;

    use crate::testing;
    use super::*;

    lazy_static! {
        static ref CONFIG: LoggerConfig = LoggerConfig {
            queue_count: 2,
            batch_capacity: 3,
            big_query: BigQueryConfig {
                origin: testing::RECEIVER_ORIGIN.to_owned(),
                project_id: "PROJECT_ID".to_owned(),
                dataset_id: "DATASET_ID".to_owned(),
                table_id: "TABLE_ID".to_owned(),
                service_account_key_file: None,
            },
        };

        static ref ROWS: Vec<Row<i32>> = (0..7)
            .map(|i| Row::new(i))
            .collect::<Vec<_>>();
    }

    #[test]
    fn test_default() {
        let logger = Logger::default();
        assert!(logger.is_dummy());
        assert!(logger.is_available());
        logger.write(ROWS[0].clone());
        assert!(logger.overflow.lock().unwrap().is_empty());
        logger.clean();
    }

    #[test]
    fn test_new() {
        let logger = block_on(Logger::new(CONFIG.clone())).unwrap();
        assert!(!logger.is_dummy());
        assert!(logger.is_available());
        assert_eq!(logger.queues.len(), CONFIG.queue_count);
        logger.write(ROWS[0].clone());
        assert!(logger.overflow.lock().unwrap().is_empty());
        logger.clean();
    }

    #[test]
    fn test_write() {
        let logger = block_on(Logger::new(CONFIG.clone())).unwrap();
        logger.write(ROWS[0].clone());
        logger.write(ROWS[1].clone());
        assert_eq!(logger.queues[0].len(), 2);
        assert_eq!(logger.queues[1].len(), 0);
    }

    #[test]
    fn test_clean() {
        let logger = block_on(Logger::new(CONFIG.clone())).unwrap();
        logger.overflow
            .lock()
            .unwrap()
            .push(ROWS[0].clone());
        logger.clean();
        assert!(logger.overflow.lock().unwrap().is_empty());
        assert_eq!(logger.queues[0].len(), 1);
        assert_eq!(logger.queues[1].len(), 0);
    }
}
