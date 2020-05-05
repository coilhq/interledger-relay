mod client;
mod logger2;
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
use self::logger2::{Logger, LoggerConfig};
use self::table::BigQueryTable;

pub type BigQueryServiceConfig = LoggerConfig;

type Row = self::table::Row<RowData>;

#[derive(Clone, Debug, serde::Serialize)]
pub struct RowData {
    pub account: Arc<String>,
    pub destination: ilp::Address,
    pub amount: u64,
    pub fulfill_time: time::SystemTime,
}

/// This service logs batches of packets to BigQuery. It will cease to route packets
/// when it detects that BigQuery is unavailable.
#[derive(Clone, Debug)]
pub struct BigQueryService<S> {
    address: ilp::Address,
    next: S,
    logger: Option<Logger<RowData>>,
}

impl<S> BigQueryService<S> {
    #[inline]
    pub fn new(address: ilp::Address, config: Option<LoggerConfig>, next: S) -> Self {
        BigQueryService {
            address,
            next,
            logger: config.map(Logger::new),
        }
    }

    pub fn setup(&self) {
        // TODO run table.exists()?
        self.logger
            .as_ref()
            .map(Logger::start);
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
            let logger = match self.logger {
                Some(logger) => logger,
                None => return self.next.call(request).await,
            };

            let is_available = logger.is_available();
            if !is_available {
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
            logger.write(Row::new(RowData {
                account,
                destination,
                amount,
                fulfill_time: time::SystemTime::now(),
            }));
            Ok(fulfill)
        })
    }
}
