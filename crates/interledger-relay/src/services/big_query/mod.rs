mod client;
mod logger;
mod logger_queue;
mod table;

use std::pin::Pin;
use std::sync::Arc;
use std::time;

use futures::prelude::*;
use log::warn;

pub use self::table::BigQueryConfig;
use crate::Service;
use crate::services::RequestWithFrom;
use self::client::{BigQueryClient, BigQueryError};
use self::logger::{Logger, LoggerConfig};
use self::logger_queue::LoggerQueue;
use self::table::BigQueryTable;

pub type BigQueryServiceConfig = LoggerConfig;

type Row = self::table::Row<RowData>;

const OVERFLOW_INTERVAL: time::Duration = time::Duration::from_secs(4);
const FLUSH_INTERVAL: time::Duration = time::Duration::from_secs(1);

#[derive(Clone, Debug, serde::Serialize)]
pub struct RowData {
    pub account: Arc<String>,
    pub destination: ilp::Address,
    pub amount: u64,
    #[serde(serialize_with = "serialize_timestamp")]
    pub fulfill_time: time::SystemTime,
}

/// This service logs batches of packets to BigQuery. It will cease to route packets
/// when it detects that BigQuery is unavailable.
#[derive(Clone, Debug)]
pub struct BigQueryService<S> {
    address: ilp::Address,
    next: S,
    logger: Arc<Logger<RowData>>,
}

impl<S> BigQueryService<S>
where
    S: 'static + Clone + Send + Sync,
{
    #[inline]
    pub fn new(address: ilp::Address, config: Option<LoggerConfig>, next: S) -> Self {
        BigQueryService {
            address,
            next,
            logger: Arc::new(config.map(Logger::new).unwrap_or_default()),
        }
    }

    pub fn setup(&self) {
        // TODO verify table.exists()?

        let self_2 = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::delay_for(OVERFLOW_INTERVAL).await;
                self_2.logger.clean();
            }
        });

        let self_3 = self.clone();
        tokio::spawn(async move {
            // Stagger the logger flushes to avoid latency spikes.
            let queue_count = self_3.logger.queues().len();
            let flush_interval = FLUSH_INTERVAL / queue_count as u32;
            let mut index = 0;
            loop {
                tokio::time::delay_for(flush_interval).await;
                let logger = &self_3.logger.queues()[index];
                logger.clone().flush_now();
                index = (index + 1) % queue_count;
            }
        });
    }
}

impl<S, Req> Service<Req> for BigQueryService<S>
where
    S: 'static + Service<Req> + Send + Sync,
    Req: RequestWithFrom + Send + 'static,
{
    type Future = Pin<Box<
        dyn Future<
            Output = Result<ilp::Fulfill, ilp::Reject>,
        > + Send + 'static,
    >>;

    fn call(self, request: Req) -> Self::Future {
        let prepare = request.borrow();
        let account = Arc::clone(request.from_account());
        let destination = prepare.destination().to_address();
        let amount = prepare.amount();

        Box::pin(async move {
            if self.logger.is_dummy() {
                return self.next.clone().call(request).await;
            }

            if !self.logger.is_available() {
                warn!(
                    "BigQuery unavailable, dropping packet: account={} destination={} amount={}",
                    account, destination, amount,
                );
                return Err(ilp::RejectBuilder {
                    code: ilp::ErrorCode::T03_CONNECTOR_BUSY,
                    message: b"backend is unavailable",
                    triggered_by: Some(self.address.as_addr()),
                    data: b"",
                }.build());
            }

            let fulfill = self.next.clone().call(request).await?;
            self.logger.write(Row::new(RowData {
                account,
                destination,
                amount,
                fulfill_time: time::SystemTime::now(),
            }));
            Ok(fulfill)
        })
    }
}

/// Serialize a `SystemTime` to a BigQuery `TIMESTAMP`.
///
/// <https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#timestamp_type>
fn serialize_timestamp<S>(time: &time::SystemTime, serializer: S)
    -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let datetime = chrono::DateTime::<chrono::Utc>::from(*time);

    serializer.collect_str({
        &datetime.format("%Y-%m-%dT%H:%M:%S.%6fZ")
    })
}

#[cfg(test)]
mod test_big_query_service {
    use chrono::TimeZone;

    use crate::testing;
    use super::*;

    #[test]
    fn test_serialize_row_data() {
        const EXPECT: &str = r#"{
  "account": "ACCOUNT",
  "destination": "test.relay",
  "amount": 123,
  "fulfill_time": "2020-05-06T07:08:09.000000Z"
}"#;
        let fulfill_time = time::SystemTime::from({
            chrono::Utc.ymd(2020, 05, 06).and_hms(07, 08, 09)
        });
        assert_eq!(
            serde_json::to_string_pretty(&RowData {
                account: Arc::new("ACCOUNT".to_owned()),
                destination: testing::ADDRESS.to_address(),
                amount:  123,
                fulfill_time,
            }).unwrap(),
            EXPECT,
        );
    }
}
