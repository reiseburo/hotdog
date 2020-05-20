/**
 * The settings module contains the necessary structs and code to process the
 * hotdog.yml file format
 */
use async_std::path::Path;
use handlebars::Handlebars;
use log::*;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

pub fn load(file: &str) -> Settings {
    let conf = load_configuration(file);
    conf.try_into().expect("Failed to parse the configuration file")
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
    conf
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
    Merge {
        json: Value,
        #[serde(default = "default_none")]
        json_str: Option<String>,
        #[serde(default = "default_none")]
        template_id: Option<String>,
    },
    Replace {
        template: String,
        #[serde(default = "default_none")]
        template_id: Option<String>,
    },
    Stop,
}

impl Action {
    /**
     * prerender_templates will ensure that the action's Handlebarts templates have already been
     * compiled by the time hotdog starts receiving traffic
     */
    fn prerender_templates(&mut self, hb: &mut Handlebars) {
        match self {
            Action::Merge { json, json_str, template_id } => {
                let s = serde_json::to_string(json).expect("Failed to serialize Merge action");
                let id = format!("template: {}", Uuid::new_v4());
                hb.register_template_string(&id, &s);
                *json_str = Some(s);
                *template_id = Some(id);
            },
            _ => {
            },
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Rule {
    pub field: Field,
    pub actions: Vec<Action>,
    #[serde(with = "serde_regex", default = "default_none")]
    pub regex: Option<regex::Regex>,
    #[serde(default = "empty_str")]
    pub jmespath: String,
}

impl Rule {
    fn prerender_templates(&mut self, hb: &mut Handlebars) {
        self.actions.iter_mut().for_each(|action| {
            action.prerender_templates(hb);
        });
    }
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
        ca: Option<std::path::PathBuf>,
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

impl Settings {
    pub fn prerender_templates(&mut self, hb: &mut Handlebars) {
        self.rules.iter_mut().for_each(|rule| {
            rule.prerender_templates(hb);
        });
    }
}

/*
 * Default functions
 */

/**
 * Allocate an return an empty string
 */
fn empty_str() -> String {
    String::new()
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

fn default_none<T>() -> Option<T> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_example_config() {
        load("hotdog.yml");
    }

    #[test]
    fn test_load_example_and_render() {
        let mut settings = load("test/configs/single-rule-with-merge.yml");
        assert_eq!(settings.rules.len(), 1);

        let mut hb = Handlebars::new();
        settings.prerender_templates(&mut hb);

        match &settings.rules[0].actions[0] {
            Action::Merge { json: _, json_str, template_id } => {
                assert!(json_str.is_some());
                assert!(template_id.is_some());
            },
            _ => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_action_prerender() {
        let mut action = Action::Merge {
            json: json!({}),
            json_str: None,
            template_id: None,
        };
        let mut hb = Handlebars::new();
        action.prerender_templates(&mut hb);
        match action {
            Action::Merge { json: _, json_str: _, template_id } => {
                assert!(template_id.is_some());
            },
            _ => {
                assert!(false);
            },
        }
    }

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
