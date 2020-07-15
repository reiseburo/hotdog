/**
 * hotdog's main
 */
extern crate async_trait;
extern crate chrono;
extern crate clap;
extern crate config;
extern crate dipstick;
extern crate handlebars;
extern crate jmespath;
extern crate regex;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_regex;
#[cfg(feature = "simd")]
extern crate simd_json;
extern crate strum;
extern crate syslog_loose;
extern crate syslog_rfc5424;
#[macro_use]
extern crate strum_macros;

use async_std::{sync::Arc, task};
use clap::{App, Arg};
use dipstick::{Input, Prefixed, Statsd};
use log::*;

mod connection;
mod errors;
mod kafka;
mod merge;
mod parse;
mod rules;
mod serve;
mod serve_plain;
mod serve_tls;
mod settings;
mod status;

use serve::*;
use settings::*;

#[async_std::main]
async fn main() -> Result<(), errors::HotdogError> {
    pretty_env_logger::init();

    info!("Starting hotdog version {}", env!["CARGO_PKG_VERSION"]);

    let matches = App::new("Hotdog")
        .version(env!("CARGO_PKG_VERSION"))
        .author("R Tyler Croy <rtyler+hotdog@brokenco.de")
        .about("Forward syslog over to Kafka with ease")
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
                .default_value("hotdog.yml")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("test")
                .short("t")
                .long("test")
                .value_name("TEST_FILE")
                .help("Test a log file against the configured rules")
                .takes_value(true),
        )
        .get_matches();

    let settings_file = matches.value_of("config").unwrap_or("hotdog.yml");
    let settings = Arc::new(settings::load(settings_file));
    let metrics = Arc::new(
        Statsd::send_to(&settings.global.metrics.statsd)
            .expect("Failed to create Statsd recorder")
            .named("hotdog")
            .metrics(),
    );

    let stats = Arc::new(status::StatsHandler::new(metrics.clone()));
    let stats_sender = stats.tx.clone();

    if let Some(st) = &settings.global.status {
        task::spawn(status::status_server(
            format!("{}:{}", st.address, st.port),
            stats.clone(),
        ));
    }

    task::spawn(async move {
        stats.runloop().await;
    });

    if let Some(test_file) = matches.value_of("test") {
        return rules::test_rules(&test_file, settings).await;
    }

    let addr = format!(
        "{}:{}",
        settings.global.listen.address, settings.global.listen.port
    );
    info!("Listening on: {}", addr);

    let state = ServerState {
        settings: settings.clone(),
        stats: stats_sender,
    };

    match &settings.global.listen.tls {
        TlsType::CertAndKey {
            cert: _,
            key: _,
            ca: _,
        } => {
            info!("Serving in TLS mode");
            let mut server = crate::serve_tls::TlsServer::new(&state);
            server.accept_loop(&addr, state).await
        }
        _ => {
            info!("Serving in plaintext mode");
            let mut server = crate::serve_plain::PlaintextServer {};
            server.accept_loop(&addr, state).await
        }
    }
}
