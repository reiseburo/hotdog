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
extern crate syslog_loose;
extern crate syslog_rfc5424;

use async_std::{sync::Arc, task};
use clap::{App, Arg};
use dipstick::{Input, Prefixed, Statsd, StatsdScope};
use log::*;
use std::collections::HashMap;

mod connection;
mod kafka;
mod merge;
mod parse;
mod rules;
mod serve;
mod serve_plain;
mod serve_tls;
mod settings;

use serve::*;
use settings::*;

type HDResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/**
 * RuleState exists to help carry state into merge/replacement functions and exists only during the
 * processing of rules
 */
struct RuleState<'a> {
    variables: &'a HashMap<String, String>,
    hb: &'a handlebars::Handlebars<'a>,
    metrics: Arc<StatsdScope>,
}

fn main() -> HDResult<()> {
    pretty_env_logger::init();

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

    if let Some(test_file) = matches.value_of("test") {
        return task::block_on(rules::test_rules(&test_file, settings));
    }

    let metrics = Arc::new(
        Statsd::send_to(&settings.global.metrics.statsd)
            .expect("Failed to create Statsd recorder")
            .named("hotdog")
            .metrics(),
    );

    let addr = format!(
        "{}:{}",
        settings.global.listen.address, settings.global.listen.port
    );
    info!("Listening on: {}", addr);

    let state = ServerState {
        settings: settings.clone(),
        metrics,
    };

    match &settings.global.listen.tls {
        TlsType::CertAndKey {
            cert: _,
            key: _,
            ca: _,
        } => {
            info!("Serving in TLS mode");
            let mut server = crate::serve_tls::TlsServer::new(&state);
            task::block_on(server.accept_loop(&addr, state));
            // TODO: bubble up Result properly
            Ok(())
        }
        _ => {
            info!("Serving in plaintext mode");
            let mut server = crate::serve_plain::PlaintextServer {};
            task::block_on(server.accept_loop(&addr, state));
            // TODO: bubble up Result properly
            Ok(())
        }
    }
}
