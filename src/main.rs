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

mod settings;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn main() -> Result<()> {
    pretty_env_logger::init();

    let settings = Arc::new(settings::load());

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
async fn accept_loop(addr: impl ToSocketAddrs, settings: Arc<settings::Settings>) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        let stream = stream?;
        debug!("Accepting from: {}", stream.peer_addr()?);
        let _handle = task::spawn(connection_loop(stream, settings.clone()));
    }
    Ok(())
}

/**
 * connection_loop is responsible for handling incoming syslog streams connections
 *
 */
async fn connection_loop(stream: TcpStream, settings: Arc<settings::Settings>) -> Result<()> {
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

