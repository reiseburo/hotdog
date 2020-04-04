/**
 * The settings module contains the necessary structs and code to process the
 * hotdog.yml file format
 */


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
#[serde(rename_all = "camelCase")]
pub enum Action {
    Drop,
    Forward,
    Modify,
}

#[derive(Debug, Deserialize)]
pub struct Rule {
    pub field: Field,
    pub action: Action,
    pub regex: String,
}

#[derive(Debug, Deserialize)]
pub struct ListenSettings {
    pub address: String,
    pub port: u64,
    pub tls: bool,
}

#[derive(Debug, Deserialize)]
pub struct Global {
    pub listen: ListenSettings,
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
