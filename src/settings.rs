/**
 * The settings module contains the necessary structs and code to process the
 * hotdog.yml file format
 */


use async_std::path::Path;
use serde_json::Value;
use regex;

pub fn load(file: &str) -> Settings {
    let conf = load_configuration(file);
    conf.try_into().expect("Failed to parse the configuration file")
}

fn load_configuration(file: &str) -> config::Config {
    let file_path = Path::new(file);

    if file_path.extension().unwrap() != "yml" {
        panic!("The configuration file must end with .yml");
    }

    /*
     * Load our settings in the priority order of:
     *
     *   - yaml file
     *   - environment variables
     *
     * Each layer overriding properties from the last
     */
    let mut conf = config::Config::default();
    let stem = file_path.file_stem().expect("Failed to get the filestem for config file");
    conf.merge(config::File::with_name(stem.to_str().expect("Failed to convert stem to &str")).required(false))
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
    Forward {
        topic: String,
    },
    Merge {
        json: Value,
    },
    Replace {
        template: String,
    },
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

fn empty_regex() -> regex::Regex {
    return regex::Regex::new("").unwrap();
}

fn empty_str() -> String {
    return String::new();
}

#[derive(Debug, Deserialize)]
pub struct Listen {
    pub address: String,
    pub port: u64,
    pub tls: bool,
}

#[derive(Debug, Deserialize)]
pub struct Kafka {
    pub brokers: String,
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

#[cfg(test)]
mod tests {
    use super::*;
}
