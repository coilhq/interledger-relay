use std::env;
use std::net::SocketAddr;
use std::process;

use futures::prelude::*;
use log::{error, info};

use interledger_relay::app;

// TODO filter path?

fn main() {
    env_logger::init();

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
                // NOTE: this never actually returns an error (which is also why
                // the closure needs a semi-explicit return type>
                .serve(move || -> Result<_, hyper::Error> {
                    Ok(connector.clone())
                })
                .map_err(|error| {
                    error!("server error: {}", error)
                })
        });

    hyper::rt::run(run_server);
}
