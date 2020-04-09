use std::env;
use std::io::Write;
use std::net::SocketAddr;
use std::process;

use futures::prelude::*;
use log::{error, info};

use interledger_relay::app;

// TODO filter path?

fn main() {
    env_logger::builder()
        .format(|fmt, record| {
            writeln!(
                fmt, "{} {} {} {}",
                fmt.timestamp_micros(),
                record.target(),
                record.level(),
                record.args(),
            )
        })
        .init();

    let bind_addr = env::var("RELAY_BIND")
        .unwrap_or_else(|_| {
            eprintln!("missing env.RELAY_BIND");
            process::exit(1);
        })
        .parse::<SocketAddr>()
        .unwrap_or_else(|error| {
            eprintln!("invalid env.RELAY_BIND: {}", error);
            process::exit(1);
        });

    let config = env::var("RELAY_CONFIG")
        .unwrap_or_else(|_| {
            eprintln!("missing env.RELAY_CONFIG");
            process::exit(1);
        });
    let config: app::Config = serde_json::from_str(&config)
        .unwrap_or_else(|error| {
            eprintln!("invalid env.RELAY_CONFIG: {}", error);
            process::exit(1);
        });

    let run_server = config
        .start()
        .map_err(|error| {
            error!("error starting connector: {}", error);
        })
        .and_then(move |connector| {
            info!("listening at: addr={}", bind_addr);
            hyper::Server::bind(&bind_addr)
                // This never actually returns an error, so the closure needs a
                // semi-explicit return type.
                .serve(hyper::service::make_service_fn(move |_socket| {
                    future::ok::<_, std::convert::Infallible>(connector.clone())
                }))
                .map_err(|error| {
                    error!("server error: {}", error);
                })
        });

    tokio::runtime::Builder::new()
        .enable_all()
        .threaded_scheduler()
        .build()
        .unwrap()
        .block_on(run_server)
        .unwrap();
}
