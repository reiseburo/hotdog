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

use async_std::{
    io::BufReader,
    prelude::*,
    sync::Arc,
    task,
};
use chrono::prelude::*;
use clap::{App, Arg};
use dipstick::{Input, InputScope, Prefixed, StatsdScope, Statsd};
use handlebars::Handlebars;
use log::*;
use std::collections::HashMap;

mod kafka;
mod merge;
mod parse;
mod rules;
mod serve;
mod serve_plain;
mod serve_tls;
mod settings;

use kafka::{KafkaMessage};
use serve::ConnectionState;
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
            let server = crate::serve_plain::PlaintextServer { };
            task::block_on(server.accept_loop(addr, settings.clone(), metrics));;
            // TODO: bubble up Result properly
            Ok(())
        }
    }
}


fn template_id_for(rule: &Rule, index: usize) -> String {
    format!("{}-{}", rule.uuid, index)
}

/**
 * precompile_templates will register templates for all the Merge and Replace actions from the
 * settings
 *
 * Will usually return a true, unless some setting parse failure occurred which is a critical
 * failure for the daemon
 */
fn precompile_templates(hb: &mut Handlebars, settings: Arc<Settings>) -> bool {
    for rule in settings.rules.iter() {
        for index in 0..rule.actions.len() {
            match &rule.actions[index] {
                Action::Merge { json: _, json_str } => {
                    let template_id = template_id_for(rule, index);

                    if let Some(template) = json_str {
                        if let Err(e) = hb.register_template_string(&template_id, &template) {
                            error!("Failed to register template! {}\n{}", e, template);
                            return false;
                        }
                    } else {
                        error!("Could not look up the json_str for a Merge action");
                        return false;
                    }
                }
                Action::Replace { template } => {
                    let template_id = format!("{}-{}", rule.uuid, index);
                    if let Err(e) = hb.register_template_string(&template_id, &template) {
                        error!("Failed to register template! {}\n{}", e, template);
                        return false;
                    }
                }
                _ => {}
            }
        }
    }
    true
}

/**
 * connection_loop is responsible for handling incoming syslog streams connections
 *
 */
pub async fn read_logs<R: async_std::io::Read + std::marker::Unpin>(
    reader: BufReader<R>,
    state: ConnectionState,
) -> HDResult<()> {
    let mut lines = reader.lines();
    let lines_count = state.metrics.counter("lines");

    let mut hb = Handlebars::new();
    if !precompile_templates(&mut hb, state.settings.clone()) {
        error!("Failing to precompile templates is a fatal error, not going to parse logs since the configuration is broken");
        // TODO fix the Err types
        return Ok(());
    }

    while let Some(line) = lines.next().await {
        let line = line?;
        debug!("log: {}", line);

        let parsed = parse::parse_line(line);

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
            for index in 0..rule.actions.len() {
                let action = &rule.actions[index];

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

                    Action::Merge { json, json_str: _ } => {
                        debug!("merging JSON content: {}", json);
                        if let Ok(buffer) =
                            perform_merge(&msg.msg, &template_id_for(&rule, index), &rule_state)
                        {
                            output = buffer;
                        } else {
                            continue_rules = false;
                        }
                    }

                    Action::Replace { template } => {
                        let template_id = template_id_for(&rule, index);

                        debug!(
                            "replacing content with template: {} ({})",
                            template, template_id
                        );
                        if let Ok(rendered) = hb.render(&template_id, &hash) {
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
    template_id: &str,
    state: &RuleState,
) -> Result<String, String> {
    if let Ok(mut msg_json) = serde_json::from_str(&buffer) {
        if let Ok(rendered) = state.hb.render(template_id, &state.variables) {
            let to_merge: serde_json::Value = serde_json::from_str(&rendered)
                .expect("Failed to deserialize our rendered to_merge_str");

            /*
             * If the administrator configured the merge incorrectly, just pass the buffer along un-merged
             */
            if !to_merge.is_object() {
                error!("Merge requested was not a JSON object: {}", to_merge);
                state
                    .metrics
                    .counter("error.merge_of_invalid_json")
                    .count(1);
                return Ok(buffer.to_string());
            }

            merge::merge(&mut msg_json, &to_merge);

            if let Ok(output) = serde_json::to_string(&msg_json) {
                return Ok(output);
            }
        }
        Err("Failed to merge and serialize".to_string())
    } else {
        error!("Failed to parse as JSON, stopping actions: {}", buffer);
        state
            .metrics
            .counter("error.merge_target_not_json")
            .count(1);
        Err("Not JSON".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /**
     * Generating a test RuleState for consistent states in test
     */
    fn rule_state<'a>(
        hb: &'a handlebars::Handlebars<'a>,
        hash: &'a HashMap<String, String>,
    ) -> RuleState<'a> {
        let metrics = Arc::new(
            Statsd::send_to("example.com:8125")
                .expect("Failed to create Statsd recorder")
                .named("test")
                .metrics(),
        );

        RuleState {
            hb: &hb,
            variables: &hash,
            metrics,
        }
    }

    #[test]
    fn merge_with_empty() {
        let mut hb = Handlebars::new();
        let template_id = "1";
        hb.register_template_string(&template_id, "{}");

        let hash = HashMap::<String, String>::new();
        let state = rule_state(&hb, &hash);

        let output = perform_merge("{}", template_id, &state);
        assert_eq!(output, Ok("{}".to_string()));
    }

    /**
     * merge without a JSON object, this should return the original buffer
     */
    #[test]
    fn merge_with_non_object() -> std::result::Result<(), String> {
        let mut hb = Handlebars::new();
        let template_id = "1";
        hb.register_template_string(&template_id, "[1]");

        let hash = HashMap::<String, String>::new();
        let state = rule_state(&hb, &hash);

        let output = perform_merge("{}", template_id, &state)?;
        assert_eq!(output, "{}".to_string());
        Ok(())
    }

    /**
     * merging without a JSON buffer should return an error
     */
    #[test]
    fn merge_without_json_buffer() {
        let mut hb = Handlebars::new();
        let template_id = "1";
        hb.register_template_string(&template_id, "{}");

        let hash = HashMap::<String, String>::new();
        let state = rule_state(&hb, &hash);

        let output = perform_merge("invalid", template_id, &state);
        let expected = Err("Not JSON".to_string());
        assert_eq!(output, expected);
    }

    /**
     * merging with a JSON buffer should return Ok with the right result
     */
    #[test]
    fn merge_with_json_buffer() {
        let mut hb = Handlebars::new();
        let template_id = "1";
        hb.register_template_string(&template_id, r#"{"hello":1}"#);

        let hash = HashMap::<String, String>::new();
        let state = rule_state(&hb, &hash);

        let output = perform_merge("{}", template_id, &state);
        assert_eq!(output, Ok("{\"hello\":1}".to_string()));
    }

    /**
     * Ensure that merging with a JSON buffer that it renders variable substitutions
     */
    #[test]
    fn merge_with_json_buffer_and_vars() {
        let mut hb = Handlebars::new();
        let template_id = "1";
        hb.register_template_string(&template_id, r#"{"hello":"{{name}}"}"#);

        let mut hash = HashMap::<String, String>::new();
        hash.insert("name".to_string(), "world".to_string());
        let state = rule_state(&hb, &hash);

        let output = perform_merge("{}", template_id, &state);
        assert_eq!(output, Ok("{\"hello\":\"world\"}".to_string()));
    }

    #[test]
    fn test_precompile_templates_merge() {
        let mut hb = Handlebars::new();
        let settings = Arc::new(load("test/configs/single-rule-with-merge.yml"));
        // Assuming that we're going to register the template with this id
        let template_id = format!("{}-{}", settings.rules[0].uuid, 0);

        let result = precompile_templates(&mut hb, settings.clone());
        assert!(result);
        assert!(hb.has_template(&template_id));
    }

    #[test]
    fn test_precompile_templates_replace() {
        let mut hb = Handlebars::new();
        let settings = Arc::new(load("test/configs/single-rule-with-replace.yml"));
        // Assuming that we're going to register the template with this id
        let template_id = format!("{}-{}", settings.rules[0].uuid, 0);

        let result = precompile_templates(&mut hb, settings.clone());
        assert!(result);
        assert!(hb.has_template(&template_id));
    }
}
