/**
 * hotdog's main
 */
extern crate config;
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
use log::*;
use regex::Regex;
use syslog_rfc5424::parse_message;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn main() -> Result<()> {
    pretty_env_logger::init();

    let conf = load_configuration();
    let settings: Arc<Settings> = Arc::new(conf.try_into().unwrap());
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
async fn accept_loop(addr: impl ToSocketAddrs, settings: Arc<Settings>) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        let stream = stream?;
        debug!("Accepting from: {}", stream.peer_addr()?);
        let _handle = task::spawn(connection_loop(stream, settings.clone()));
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum Field {
    Severity,
    Facility,
    Timestamp,
    Hostname,
    Appname,
    Msg,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum Action {
    Drop,
    Forward,
    Modify,
}

#[derive(Debug, Deserialize)]
struct Rule {
    field: Field,
    action: Action,
    regex: String,
}

#[derive(Debug, Deserialize)]
struct ListenSettings {
    //#[serde(default = "localhost")]
    address: String,
    port: u64,
    tls: bool,
}

#[derive(Debug, Deserialize)]
struct Global {
    listen: ListenSettings,
}

#[derive(Debug, Deserialize)]
struct Settings {
    global: Global,
    rules: Vec<Rule>,
}

/**
 * connection_loop is responsible for handling incoming syslog streams connections
 *
 */
async fn connection_loop(stream: TcpStream, settings: Arc<Settings>) -> Result<()> {
    let reader = BufReader::new(&stream);
    let mut lines = reader.lines();

    while let Some(line) = lines.next().await {
        let line = line?;
        let msg = parse_message(line)?;
        for rule in settings.rules.iter() {
            let re = Regex::new(&rule.regex).unwrap();
            if let Some(captures) = re.captures(&msg.msg) {
                if let Some(name) = captures.name("name") {
                    info!("saying howdy to {}", name.as_str());
                }
            }
        }
    }

    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
}
