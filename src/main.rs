/**
 * hotdog's main
 */
extern crate clap;
extern crate chrono;
extern crate config;
extern crate dipstick;
extern crate handlebars;
extern crate jmespath;
extern crate regex;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_regex;
#[macro_use]
extern crate serde_json;
extern crate syslog_rfc5424;

use async_std::{
    fs::File,
    io::BufReader,
    net::{TcpListener, TcpStream, ToSocketAddrs},
    prelude::*,
    sync::Arc,
    task,
};
use clap::{Arg, App};
use chrono::prelude::*;
use dipstick::*;
use handlebars::Handlebars;
use log::*;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::collections::HashMap;
use syslog_rfc5424::parse_message;

mod merge;
mod settings;

use settings::*;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn main() -> Result<()> {
    pretty_env_logger::init();

    let matches = App::new("Hotdog")
                          .version(env!("CARGO_PKG_VERSION"))
                          .author("R Tyler Croy <rtyler@brokenco.de")
                          .about("Forward syslog over to Kafka with ease")
                          .arg(Arg::with_name("config")
                               .short("c")
                               .long("config")
                               .value_name("FILE")
                               .help("Sets a custom config file")
                               .default_value("hotdog.yml")
                               .takes_value(true))
                          .arg(Arg::with_name("test")
                              .short("t")
                              .long("test")
                              .value_name("TEST_FILE")
                              .help("Test a log file against the configured rules")
                              .takes_value(true))
                          .get_matches();

    let settings_file = matches.value_of("config").unwrap_or("hotdog.yml");
    let settings = Arc::new(settings::load(settings_file));

    if let Some(test_file) = matches.value_of("test") {
        return task::block_on(test_rules(&test_file, settings.clone()));
    }

    let metrics = Arc::new(Statsd::send_to(&settings.global.metrics.statsd)
        .expect("Failed to create Statsd recorder")
        .named("hotdog")
        .metrics());

    let addr = format!(
        "{}:{}",
        settings.global.listen.address, settings.global.listen.port
    );
    info!("Listening on: {}", addr);

    task::block_on(
        accept_loop(addr, settings.clone(), metrics.clone()))
}

async fn test_rules(file_name: &str, settings: Arc<Settings>) -> Result<()> {
    let file = File::open(file_name).await.expect("Failed to open the file");
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut number: u64 = 0;

    while let Some(line) = lines.next().await {
        let line = line?;
        debug!("Testing the line: {}", line);
        number = number + 1;
        let mut matches: Vec<&str> = vec![];

        for rule in settings.rules.iter() {
            match rule.field {
                Field::Msg => {
                    if rule.jmespath.len() > 0 {
                        let expr = jmespath::compile(&rule.jmespath).unwrap();
                        if let Ok(data) = jmespath::Variable::from_json(&line) {
                            // Search the data with the compiled expression
                            if let Ok(result) = expr.search(data) {
                                if ! result.is_null() {
                                    matches.push(&rule.jmespath);
                                }
                            }
                        }
                    }
                    else if let Some(captures) = rule.regex.captures(&line) {
                        matches.push(&rule.regex.as_str());
                    }
                },
                _ => {
                },
            }
        }

        if matches.len() > 0 {
            println!("Line {} matches on:", number);
            for m in matches.iter() {
                println!("\t - {}", m);
            }
        }

    }

    Ok(())
}

/**
 * accept_loop will simply create the socket listener and dispatch newly accepted connections to
 * the connection_loop function
 */
async fn accept_loop(addr: impl ToSocketAddrs, settings: Arc<Settings>, metrics: Arc<LockingOutput>) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let mut incoming = listener.incoming();
    let connection_count = metrics.counter("connections");

    while let Some(stream) = incoming.next().await {
        connection_count.count(1);
        let stream = stream?;
        debug!("Accepting from: {}", stream.peer_addr()?);
        let _handle = task::spawn(connection_loop(stream, settings.clone(), metrics.clone()));
    }
    Ok(())
}

/**
 * connection_loop is responsible for handling incoming syslog streams connections
 *
 */
async fn connection_loop(stream: TcpStream, settings: Arc<Settings>, metrics: Arc<LockingOutput>) -> Result<()> {
    debug!("Connection received: {}", stream.peer_addr()?);
    let reader = BufReader::new(&stream);
    let mut lines = reader.lines();
    let lines_count = metrics.counter("lines");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &settings.global.kafka.brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let hb = Handlebars::new();

    while let Some(line) = lines.next().await {
        let line = line?;
        debug!("log: {}", line);

        let msg = parse_message(line)?;
        lines_count.count(1);

        let mut continue_rules = true;

        for rule in settings.rules.iter() {
            /*
             * If we have been told to stop processing rules, then it's time to bail on this log
             * message
             */
            if ! continue_rules {
                break;
            }

            // The output buffer that we will ultimately send along to the Kafka service
            let mut output = String::new();
            let mut rule_matches = false;
            let mut hash = HashMap::new();
            hash.insert("msg".to_string(), String::from(&msg.msg));
            hash.insert("version".to_string(), env!["CARGO_PKG_VERSION"].to_string());
            hash.insert("iso8601".to_string(), Utc::now().to_rfc3339());

            match rule.field {
                Field::Msg => {
                    /*
                     * Check to see if we have a jmespath first
                     */
                    if rule.jmespath.len() > 0 {
                        let expr = jmespath::compile(&rule.jmespath).unwrap();
                        if let Ok(data) = jmespath::Variable::from_json(&msg.msg) {
                            // Search the data with the compiled expression
                            if let Ok(result) = expr.search(data) {
                                if ! result.is_null() {
                                    rule_matches = true;
                                    /* TODO: need to find a way to extrac tmatches */
                                }
                            }
                        }
                    } else if let Some(captures) = rule.regex.captures(&msg.msg) {
                        rule_matches = true;

                        for name in rule.regex.capture_names() {
                            if let Some(name) = name {
                                if let Some(value) = captures.name(name) {
                                    hash.insert(name.to_string(), String::from(value.as_str()));
                                }
                            }
                        }
                    }
                },
                _ => {
                    debug!("unhandled `field` for this rule: {}", rule.regex);
                },
            }

            /*
             * This specific didn't match, so onto the next one
             */
            if ! rule_matches {
                continue;
            }

            let state = RuleState {
                hb: &hb,
                variables: &hash,
            };

            /*
             * Process the actions one the rule has matched
             */
            for action in rule.actions.iter() {
                match action {
                    Action::Forward { topic } => {
                        send_to_kafka(output, topic, &producer, &state);
                        break;
                    },
                    Action::Merge { json } => {
                        if let Ok(buffer) = perform_merge(&msg.msg, json, &state) {
                            output = buffer;
                        }
                        else {
                            continue_rules = false;
                        }
                    },
                    Action::Replace { template } => {
                        if let Ok(rendered) = hb.render_template(template, &hash) {
                            output = rendered;
                        }
                    },
                    Action::Stop => {
                        continue_rules = false;
                    },
                }
            }
        }
    }

    debug!("Connection terminating for {}", stream.peer_addr()?);
    Ok(())
}

/**
 * Send the given output message to the desired Kafka topic
 */
async fn send_to_kafka(output: String,
    topic: &str,
    producer: &FutureProducer,
    state: &RuleState<'_>) {

    if let Ok(rendered) = state.hb.render_template(topic, &state.variables) {
        info!("action is forward {:?}", rendered);
        let wait: i64 = 0;
        producer.send(
            FutureRecord::to(&rendered)
                .payload(&output)
                .key(&output), wait).await;

    }
}

/**
 * perform_merge will generate the buffer resulting of the JSON merge
 */
fn perform_merge(buffer: &str,
    to_merge: &serde_json::Value,
    state: &RuleState) -> std::result::Result<String, String> {

    if let Ok(mut msg_json) = serde_json::from_str::<serde_json::Value>(buffer) {
        Ok(buffer.to_string())
    }
    else {
        error!("Failed to parse as JSON, stopping actions: {}", buffer);

        Err("Not JSON".to_string())
    }
}

/**
 * RuleState exists to help curry state into merge/replacement functions
 */
struct RuleState<'a> {
    variables: &'a HashMap<String, String>,
    hb: &'a handlebars::Handlebars<'a>,
}

/**
 * merge_and_render will take care of merging the two values and manage the
 * rendering of variable substitutions
 */
fn merge_and_render<'a>(mut left: &mut serde_json::Value,
    right: &serde_json::Value,
    state: &RuleState<'a>) -> String {
    merge::merge(&mut left, &right);

    let output = serde_json::to_string(&left).unwrap();

    /*
     * This is a bit inefficient, but until I can figure out a better way
     * to render the variables that are being substituted in a merged JSON
     * object, hotdog will just render the JSON object and then render it
     * as a template.
     *
     * what could possibly go wrong
     */
    if let Ok(rendered) = state.hb.render_template(&output, &state.variables) {
        return rendered;
    }
    return output;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_with_empty() {
        let hb = Handlebars::new();
        let hash = HashMap::<String, String>::new();
        let state = RuleState {
            hb: &hb,
            variables: &hash,
        };

        let to_merge = json!({});
        let output = perform_merge("{}", &to_merge, &state);
        assert_eq!(output, Ok("{}".to_string()));
    }

    /**
     * merge without a JSON object, this should return the original buffer
     */
    #[test]
    fn merge_with_non_object() -> std::result::Result<(), String> {
        let hb = Handlebars::new();
        let hash = HashMap::<String, String>::new();
        let state = RuleState {
            hb: &hb,
            variables: &hash,
        };

        let to_merge = json!([]);
        let output = perform_merge("{}", &to_merge, &state)?;
        assert_eq!(output, "{}".to_string());
        Ok(())
    }

    /**
     * merging without a JSON buffer should return an error
     */
    #[test]
    fn merge_without_json_buffer() {
        let hb = Handlebars::new();
        let hash = HashMap::<String, String>::new();
        let state = RuleState {
            hb: &hb,
            variables: &hash,
        };

        let to_merge = json!({});
        let output = perform_merge("invalid", &to_merge, &state);
        let expected = Err("Not JSON".to_string());
        assert_eq!(output, expected);
    }

    #[test]
    fn test_merge_and_render() {
        let mut hash = HashMap::<String, String>::new();
        hash.insert("value".to_string(), "hi".to_string());

        let hb = Handlebars::new();
        let state = RuleState {
            hb: &hb,
            variables: &hash,
        };

        let mut origin = json!({"rust" : true});
        let config = json!({"test" : "{{value}}"});

        let buf = merge_and_render(&mut origin, &config, &state);
        assert_eq!(buf, r#"{"rust":true,"test":"hi"}"#);
    }

}
