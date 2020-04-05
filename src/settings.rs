/**
 * The settings module contains the necessary structs and code to process the
 * hotdog.yml file format
 */


use regex;

pub fn load() -> Settings {
    let conf = load_configuration();
    conf.try_into().expect("Failed to parse the hotdog.yml file")
}

fn load_configuration() -> config::Config {
    /*
     * Load our settings in the priority order of:
     *
     *   - yaml file
     *   - environment variables
     *
     * Each layer overriding properties from the last
     */
    let mut conf = config::Config::default();
    conf.merge(config::File::with_name("hotdog").required(false))
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
    Replace {
        template: String,
    },
    Forward {
        topic: String,
    },
    Stop,
}

#[derive(Debug, Deserialize)]
pub struct Rule {
    pub field: Field,
    pub actions: Vec<Action>,
    #[serde(with = "serde_regex")]
    pub regex: regex::Regex,
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
pub struct Global {
    pub listen: Listen,
    pub kafka: Kafka,
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
