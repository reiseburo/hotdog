extern crate chrono;
/**
 * hotdog's main
 */
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
#[cfg(test)]
#[macro_use]
extern crate serde_json;
extern crate syslog_rfc5424;
extern crate syslog_loose;

use async_std::{
    io::BufReader,
    net::{TcpListener, ToSocketAddrs},
    prelude::*,
    sync::Arc,
    task,
};
use chrono::prelude::*;
use clap::{App, Arg};
use crossbeam::channel::Sender;
use dipstick::*;
use handlebars::Handlebars;
use log::*;
use std::collections::HashMap;

mod kafka;
mod merge;
mod rules;
mod serve_tls;
mod settings;

use kafka::{Kafka, KafkaMessage};
use settings::*;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/**
 * ConnectionState carries the necessary types of state into new tasks for handling connections
 */
pub struct ConnectionState {
    settings: Arc<Settings>,
    metrics: Arc<LockingOutput>,
    sender: Sender<KafkaMessage>,
}

/**
 * RuleState exists to help carry state into merge/replacement functions and exists only during the
 * processing of rules
 */
struct RuleState<'a> {
    variables: &'a HashMap<String, String>,
    hb: &'a handlebars::Handlebars<'a>,
    metrics: Arc<LockingOutput>,
}

fn main() -> Result<()> {
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

    match &settings.global.listen.tls {
        TlsType::CertAndKey {
            cert: _,
            key: _,
            ca: _,
        } => {
            info!("Serving in TLS mode");
            task::block_on(crate::serve_tls::accept_loop(
                addr,
                settings.clone(),
                metrics,
            ))
        }
        _ => {
            info!("Serving in plaintext mode");
            task::block_on(accept_loop(addr, settings.clone(), metrics))
        }
    }
}

/**
 * accept_loop will simply create the socket listener and dispatch newly accepted connections to
 * the connection_loop function
 */
async fn accept_loop(
    addr: impl ToSocketAddrs,
    settings: Arc<Settings>,
    metrics: Arc<LockingOutput>,
) -> Result<()> {
    let mut kafka = Kafka::new(settings.global.kafka.buffer);

    if !kafka.connect(
        &settings.global.kafka.conf,
        Some(settings.global.kafka.timeout_ms),
    ) {
        error!("Cannot start hotdog without a workable broker connection");
        return Ok(());
    }
    kafka.with_metrics(metrics.clone());
    let sender = kafka.get_sender();

    task::spawn(async move {
        debug!("starting sendloop");
        kafka.sendloop();
    });

    let listener = TcpListener::bind(addr).await?;
    let mut incoming = listener.incoming();

    while let Some(stream) = incoming.next().await {
        let stream = stream?;
        debug!("Accepting from: {}", stream.peer_addr()?);
        let reader = BufReader::new(stream);
        let state = ConnectionState {
            settings: settings.clone(),
            metrics: metrics.clone(),
            sender: sender.clone(),
        };

        task::spawn(async move {
            if let Err(e) = read_logs(reader, state).await {
                error!("Failed to read logs: {:?}", e);
            }
            debug!("Connection dropped");
        });
    }
    Ok(())
}

#[derive(Debug)]
enum SyslogErrors {
    UnknownFormat,
}

#[derive(Debug)]
struct SyslogMessage {
    msg: String,
}

/**
 * Attempt to parse a given line either as RFC 5424 or RFC 3164
 */
fn parse_line(line: String) -> std::result::Result<SyslogMessage, SyslogErrors> {
    match syslog_rfc5424::parse_message(&line) {
        Ok(msg) => {
            return Ok(SyslogMessage {
                msg: msg.msg,
            })
        },
        Err(_) => {
            let parsed = syslog_loose::parse_message(&line);

            /*
             * Since syslog_loose doesn't give a Result, the only way to tell if themessage wasn't
             * parsed properly is if some fields are None'd out.
             */
            if parsed.timestamp != None {
                return Ok(SyslogMessage{
                    msg: parsed.msg.to_string(),
                })
            }
            Err(SyslogErrors::UnknownFormat)
        },
    }
}

/**
 * connection_loop is responsible for handling incoming syslog streams connections
 *
 */
pub async fn read_logs<R: async_std::io::Read + std::marker::Unpin>(
    reader: BufReader<R>,
    state: ConnectionState,
) -> Result<()> {
    let mut lines = reader.lines();
    let lines_count = state.metrics.counter("lines");

    let hb = Handlebars::new();

    while let Some(line) = lines.next().await {
        let line = line?;
        debug!("log: {}", line);

        let parsed = parse_line(line);

        if let Err(_e) = &parsed {
            state.metrics.counter("error.log_parse").count(1);
            error!("failed to parse messad");
            continue;
        }
        /*
         * Now that we've logged the error, let's unpack and bubble the error anyways
         */
        let msg = parsed.unwrap();
        lines_count.count(1);
        let mut continue_rules = true;
        debug!("parsed as: {}", msg.msg);


        for rule in state.settings.rules.iter() {
            /*
             * If we have been told to stop processing rules, then it's time to bail on this log
             * message
             */
            if !continue_rules {
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
                    if !rule.jmespath.is_empty() {
                        let expr = jmespath::compile(&rule.jmespath).unwrap();
                        if let Ok(data) = jmespath::Variable::from_json(&msg.msg) {
                            // Search the data with the compiled expression
                            if let Ok(result) = expr.search(data) {
                                if !result.is_null() {
                                    rule_matches = true;
                                    debug!("jmespath rule matched, value: {}", result);
                                    if let Some(value) = result.as_string() {
                                        hash.insert("value".to_string(), value.to_string());
                                    } else {
                                        warn!("Unable to parse out the string value for {}, the `value` variable substitution will not be available,", result);
                                    }
                                }
                            }
                        }
                    } else if let Some(regex) = &rule.regex {
                        if let Some(captures) = regex.captures(&msg.msg) {
                            rule_matches = true;

                            for name in regex.capture_names() {
                                if let Some(name) = name {
                                    if let Some(value) = captures.name(name) {
                                        hash.insert(name.to_string(), String::from(value.as_str()));
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {
                    warn!("unhandled `field` for rule");
                }
            }

            /*
             * This specific didn't match, so onto the next one
             */
            if !rule_matches {
                continue;
            }

            let rule_state = RuleState {
                hb: &hb,
                variables: &hash,
                metrics: state.metrics.clone(),
            };

            /*
             * Process the actions one the rule has matched
             */
            for action in rule.actions.iter() {
                match action {
                    Action::Forward { topic } => {
                        /*
                         * quick check on our internal queue, if it's full, skip all the processing
                         * and move onto the next message
                         */
                        if state.sender.is_full() {
                            error!("Internal Kafka queue is full! Dropping 1 message");
                            state.metrics.counter("error.full_internal_queue").count(1);
                            continue_rules = false;
                            break;
                        }

                        /*
                         * If a custom output was never defined, just take the
                         * raw message and pass that along.
                         */
                        if output.is_empty() {
                            output = String::from(&msg.msg);
                        }

                        if let Ok(actual_topic) = hb.render_template(&topic, &hash) {
                            debug!("Enqueueing for topic: `{}`", actual_topic);
                            /*
                             * `output` is consumed by send_to_kafka, so the rest of the rules
                             * should be skipped.
                             */
                            let kmsg = KafkaMessage::new(actual_topic, output);
                            match state.sender.try_send(kmsg) {
                                Err(err) => {
                                    error!("Failed to push a message onto our internal Kafka queue: {:?}", err);
                                    state.metrics.counter("error.internal_push_failed").count(1);
                                }
                                Ok(_) => {
                                    debug!("Message enqueued");
                                }
                            }
                            continue_rules = false;
                        } else {
                            error!("Failed to process the configured topic: `{}`", topic);
                            state.metrics.counter("error.topic_parse_failed").count(1);
                        }
                        break;
                    }
                    Action::Merge { json } => {
                        debug!("merging JSON content: {}", json);
                        if let Ok(buffer) = perform_merge(&msg.msg, json, &rule_state) {
                            output = buffer;
                        } else {
                            continue_rules = false;
                        }
                    }
                    Action::Replace { template } => {
                        debug!("replacing content with template: {}", template);
                        if let Ok(rendered) = hb.render_template(template, &hash) {
                            output = rendered;
                        }
                    }
                    Action::Stop => {
                        continue_rules = false;
                    }
                }
            }
        }
    }

    Ok(())
}

/**
 * perform_merge will generate the buffer resulting of the JSON merge
 */
fn perform_merge(
    buffer: &str,
    to_merge: &serde_json::Value,
    state: &RuleState,
) -> std::result::Result<String, String> {
    /*
     * If the administrator configured the merge incorrectly, just pass the buffer along un-merged
     */
    if !to_merge.is_object() {
        error!("Merge requested was not a JSON object: {}", to_merge);
        state.metrics.counter("error.merge_of_invalid_json").count(1);
        return Ok(buffer.to_string());
    }

    if let Ok(mut msg_json) = serde_json::from_str::<serde_json::Value>(buffer) {
        merge::merge(&mut msg_json, to_merge);
        if let Ok(output) = serde_json::to_string(&msg_json) {
            if let Ok(rendered) = state.hb.render_template(&output, &state.variables) {
                return Ok(rendered);
            }
            Ok(output)
        } else {
            Err("Failed to render".to_string())
        }
    } else {
        error!("Failed to parse as JSON, stopping actions: {}", buffer);
        state.metrics.counter("error.merge_target_not_json").count(1);
        Err("Not JSON".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /**
     * Generating a test RuleState for consistent states in test
     */
    fn rule_state<'a>(hb : &'a handlebars::Handlebars<'a>,
                hash : &'a HashMap<String, String>) -> RuleState<'a> {

        let metrics = Arc::new(
            Statsd::send_to("example.com:8125")
                .expect("Failed to create Statsd recorder")
                .named("test")
                .metrics(),
        );

        RuleState {
            hb: &hb,
            variables: &hash,
            metrics: metrics,
        }
    }

    #[test]
    fn merge_with_empty() {
        let hb = Handlebars::new();
        let hash = HashMap::<String, String>::new();
        let state = rule_state(&hb, &hash);

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
        let state = rule_state(&hb, &hash);

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
        let state = rule_state(&hb, &hash);

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
        let state = rule_state(&hb, &hash);

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
        let state = rule_state(&hb, &hash);

        let to_merge = json!({"hello" : "{{name}}"});
        let output = perform_merge("{}", &to_merge, &state);
        assert_eq!(output, Ok("{\"hello\":\"world\"}".to_string()));
    }

    #[test]
    fn test_parsing_invalid() {
        let buffer = "blah".to_string();
        let parsed = parse_line(buffer);
        if let Ok(msg) = &parsed {
            println!("msg: {}", msg.msg);
        }
        assert!(parsed.is_err());
    }

    #[test]
    fn test_5424() {
        let buffer = r#"<13>1 2020-04-18T15:16:09.956153-07:00 coconut tyler - - [timeQuality tzKnown="1" isSynced="1" syncAccuracy="505061"] hi"#.to_string();
        let parsed = parse_line(buffer);
        assert!(parsed.is_ok());
        if let Ok(msg) = parsed {
            assert_eq!("hi", msg.msg);
        }
    }

    #[test]
    fn test_3164() {
        let buffer = r#"<190>May 13 21:45:18 coconut hotdog: hi"#.to_string();
        let parsed = parse_line(buffer);
        assert!(parsed.is_ok());
        if let Ok(msg) = parsed {
            assert_eq!("hi", msg.msg);
        }
    }
}
