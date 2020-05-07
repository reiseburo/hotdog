/**
 * The settings module contains the necessary structs and code to process the
 * hotdog.yml file format
 */
use async_std::path::Path;
use log::*;
use regex;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

pub fn load(file: &str) -> Settings {
    let conf = load_configuration(file);
    conf.try_into()
        .expect("Failed to parse the configuration file")
}

fn load_configuration(file: &str) -> config::Config {
    let file_path = Path::new(file);

    if file_path.extension().unwrap() != "yml" {
        panic!("The configuration file must end with .yml");
    }

    debug!("Loading configuration from {}", file);

    /*
     * Load our settings in the priority order of:
     *
     *   - yaml file
     *   - environment variables
     *
     * Each layer overriding properties from the last
     */
    let mut conf = config::Config::default();
    conf.merge(config::File::with_name(file))
        .unwrap()
        .merge(config::Environment::with_prefix("HOTDOG"))
        .unwrap();

    let _port: u64 = conf
        .get("global.listen.port")
        .expect("Configuration had no `global.listen.port` setting");
    return conf;
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Field {
    Severity,
    Facility,
    Timestamp,
    Hostname,
    Appname,
    Msg,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum Action {
    Forward { topic: String },
    Merge { json: Value },
    Replace { template: String },
    Stop,
}

#[derive(Debug, Deserialize)]
pub struct Rule {
    pub field: Field,
    pub actions: Vec<Action>,
    #[serde(with = "serde_regex", default = "empty_regex")]
    pub regex: regex::Regex,
    #[serde(default = "empty_str")]
    pub jmespath: String,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum TlsType {
    None,

    /**
     * The CertsAndKey struct will contain PathBuf values if the tls section was
     * included
     */
    CertAndKey {
        cert: std::path::PathBuf,
        key: std::path::PathBuf,
    },
}

impl Default for TlsType {
    fn default() -> TlsType {
        TlsType::None
    }
}

#[derive(Debug, Deserialize)]
pub struct Listen {
    pub address: String,
    pub port: u64,
    pub tls: TlsType,
}

#[derive(Debug, Deserialize)]
pub struct Kafka {
    #[serde(default = "kafka_buffer_default")]
    pub buffer: usize,
    #[serde(default = "kafka_timeout_default")]
    pub timeout_ms: Duration,
    pub conf: HashMap<String, String>,
    pub topic: String,
}

#[derive(Debug, Deserialize)]
pub struct Metrics {
    pub statsd: String,
}

#[derive(Debug, Deserialize)]
pub struct Global {
    pub listen: Listen,
    pub kafka: Kafka,
    pub metrics: Metrics,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub global: Global,
    pub rules: Vec<Rule>,
}

/*
 * Default functions
 */

/**
 * Return an empty regular expression
 */
fn empty_regex() -> regex::Regex {
    return regex::Regex::new("").unwrap();
}

/**
 * Allocate an return an empty string
 */
fn empty_str() -> String {
    return String::new();
}

/**
 * Return the default size used for the Kafka buffer
 */
fn kafka_buffer_default() -> usize {
    1024
}

fn kafka_timeout_default() -> Duration {
    Duration::from_secs(30)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_tls() {
        assert_eq!(TlsType::None, TlsType::default());
    }

    #[test]
    fn test_empty_str() {
        assert_eq!("".to_string(), empty_str());
    }

    #[test]
    fn test_kafka_buffer_default() {
        assert_eq!(1024, kafka_buffer_default());
    }
}
