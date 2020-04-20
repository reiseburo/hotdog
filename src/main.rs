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
    io::BufReader,
    net::{TcpListener, ToSocketAddrs},
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
mod rules;
mod settings;
mod serve_tls;

use settings::*;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/**
 * RuleState exists to help curry state into merge/replacement functions
 */
struct RuleState<'a> {
    variables: &'a HashMap<String, String>,
    hb: &'a handlebars::Handlebars<'a>,
}

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
        return task::block_on(rules::test_rules(&test_file, settings.clone()));
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

    match &settings.global.listen.tls {
        TlsType::CertAndKey { cert: _, key: _ } => {
            info!("Serving in TLS mode");
            task::block_on(
                crate::serve_tls::accept_loop(addr, settings.clone(), metrics.clone()))
        },
        _ => {
            info!("Serving in plaintext mode");
            task::block_on(
                accept_loop(addr, settings.clone(), metrics.clone()))
        },
    }
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
        let reader = BufReader::new(stream);
        let settings = settings.clone();
        let metrics = metrics.clone();

        task::spawn(async move {
            read_logs(reader, settings, metrics).await;
        });
    }
    Ok(())
}

/**
 * connection_loop is responsible for handling incoming syslog streams connections
 *
 */

pub async fn read_logs<R: async_std::io::Read + std::marker::Unpin>(reader: BufReader<R>, settings: Arc<Settings>, metrics: Arc<LockingOutput>) -> Result<()> {
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

        let parsed = parse_message(line);

        match &parsed {
            Err(e) => {
                error!("failed to parse message: {}", e);
            },
            _ => {},
        }

        /*
         * Now that we've logged the error, let's unpack and bubble the error anyways
         */
        let msg = parsed?;
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
                                    debug!("jmespath rule matched, value: {}", result);
                                    if let Some(value) = result.as_string() {
                                        hash.insert("value".to_string(), value.to_string());
                                    }
                                    else {
                                        warn!("Unable to parse out the string value for {}, the `value` variable substitution will not be available,", result);
                                    }
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
                        /*
                         * If a custom output was never defined, just take the
                         * raw message and pass that along.
                         */
                        if output.len() == 0 {
                            output = String::from(&msg.msg);
                        }

                        if let Ok(actual) = hb.render_template(&topic, &hash) {
                            debug!("Forwarding to the topic: `{}`", actual);
                            send_to_kafka(output, &actual, &producer, &state).await;
                            /*
                             * `output` is consumed by send_to_kafka, so the rest of the rules
                             * should be skipped.
                             */
                            continue_rules = false;
                        }
                        else {
                            error!("Failed to process the configured topic: `{}`", topic);
                        }
                        break;
                    },
                    Action::Merge { json } => {
                        debug!("merging JSON content: {}", json);
                        if let Ok(buffer) = perform_merge(&msg.msg, json, &state) {
                            output = buffer;
                        }
                        else {
                            continue_rules = false;
                        }
                    },
                    Action::Replace { template } => {
                        debug!("replacing content with template: {}", template);
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
        debug!("action is forward {:?}", rendered);
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

    /*
     * If the administrator configured the merge incorrectly, just pass the buffer along un-merged
     */
    if ! to_merge.is_object() {
        error!("Merge requested was not a JSON object: {}", to_merge);
        return Ok(buffer.to_string());
    }

    if let Ok(mut msg_json) = serde_json::from_str::<serde_json::Value>(buffer) {

        merge::merge(&mut msg_json, to_merge);
        if let Ok(output) = serde_json::to_string(&msg_json) {
            if let Ok(rendered) = state.hb.render_template(&output, &state.variables) {
                return Ok(rendered);
            }
            return Ok(output);
        }
        else {
            Err("Failed to render".to_string())
        }
    }
    else {
        error!("Failed to parse as JSON, stopping actions: {}", buffer);

        Err("Not JSON".to_string())
    }
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

        let to_merge = json!([1]);
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

    /**
     * merging with a JSON buffer should return Ok with the right result
     */
    #[test]
    fn merge_with_json_buffer() {
        let hb = Handlebars::new();
        let hash = HashMap::<String, String>::new();
        let state = RuleState {
            hb: &hb,
            variables: &hash,
        };

        let to_merge = json!({"hello" : 1});
        let output = perform_merge("{}", &to_merge, &state);
        assert_eq!(output, Ok("{\"hello\":1}".to_string()));
    }

    /**
     * Ensure that merging with a JSON buffer that it renders variable substitutions
     */
    #[test]
    fn merge_with_json_buffer_and_vars() {
        let hb = Handlebars::new();
        let mut hash = HashMap::<String, String>::new();
        hash.insert("name".to_string(), "world".to_string());

        let state = RuleState {
            hb: &hb,
            variables: &hash,
        };

        let to_merge = json!({"hello" : "{{name}}"});
        let output = perform_merge("{}", &to_merge, &state);
        assert_eq!(output, Ok("{\"hello\":\"world\"}".to_string()));
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
