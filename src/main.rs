/**
 * hotdog's main
 */
extern crate config;
extern crate handlebars;
extern crate regex;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate syslog_rfc5424;

use async_std::{
    io::BufReader,
    net::{TcpListener, TcpStream, ToSocketAddrs},
    prelude::*,
    sync::Arc,
    task,
};
use handlebars::Handlebars;
use log::*;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use regex::Regex;
use std::collections::HashMap;
use syslog_rfc5424::parse_message;

mod settings;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn main() -> Result<()> {
    pretty_env_logger::init();

    let settings = Arc::new(settings::load());

    let addr = format!(
        "{}:{}",
        settings.global.listen.address, settings.global.listen.port
    );
    info!("Listening on: {}", addr);

    let fut = accept_loop(addr, settings.clone());
    task::block_on(fut)
}

/**
 * accept_loop will simply create the socket listener and dispatch newly accepted connections to
 * the connection_loop function
 */
async fn accept_loop(addr: impl ToSocketAddrs, settings: Arc<settings::Settings>) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        let stream = stream?;
        debug!("Accepting from: {}", stream.peer_addr()?);
        let _handle = task::spawn(connection_loop(stream, settings.clone()));
    }
    Ok(())
}

/**
 * connection_loop is responsible for handling incoming syslog streams connections
 *
 */
async fn connection_loop(stream: TcpStream, settings: Arc<settings::Settings>) -> Result<()> {
    let reader = BufReader::new(&stream);
    let mut lines = reader.lines();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &settings.global.kafka.brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let hb = Handlebars::new();

    while let Some(line) = lines.next().await {
        let line = line?;
        let msg = parse_message(line)?;
        // The output buffer that we will ultimately send along to the Kafka service
        let mut output = String::new();

        for rule in settings.rules.iter() {
            let re = Regex::new(&rule.regex).expect("Failed to compile a regex");
            let mut rule_matches = false;
            let mut hash = HashMap::new();
            hash.insert("msg", String::from(&msg.msg));

            match rule.field {
                settings::Field::Msg => {
                    if let Some(captures) = re.captures(&msg.msg) {
                        rule_matches = true;

                        for name in re.capture_names() {
                            if let Some(name) = name {
                                if let Some(value) = captures.name(name) {
                                    hash.insert(name, String::from(value.as_str()));
                                }
                            }
                        }
                    }
                },
                _ => {
                },
            }

            if rule_matches == false {
                break;
            }

            /*
             * Process the actions one the rule has matched
             */
            for action in rule.actions.iter() {
                match action {
                    settings::Action::Replace { template } => {
                        if let Ok(rendered) = hb.render_template(template, &hash) {
                            output = rendered;
                        }
                    },
                    settings::Action::Forward { topic } => {
                        if let Ok(rendered) = hb.render_template(topic, &hash) {
                            info!("action is forward {:?}", rendered);
                            producer.send(
                                FutureRecord::to(&rendered)
                                    .payload(&output)
                                    .key(&output), 0).await;

                        }
                    },
                }
            }
        }
    }

    Ok(())
}
