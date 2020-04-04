/**
 *
 */

extern crate syslog_rfc5424;

use async_std::{
    io::BufReader,
    prelude::*,
    task,
    net::{TcpListener, TcpStream, ToSocketAddrs},
};

use syslog_rfc5424::parse_message;


type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn accept_loop(addr: impl ToSocketAddrs) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    println!("Listening..");
    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        let stream = stream?;
        println!("Accepting from: {}", stream.peer_addr()?);
        let _handle = task::spawn(connection_loop(stream));
    }
    Ok(())
}

async fn connection_loop(stream: TcpStream) -> Result<()> {
    let reader = BufReader::new(&stream);
    let mut lines = reader.lines();

    while let Some(line) = lines.next().await {
        let line = line?;
        let msg = parse_message(line)?;
        println!("{} from host: {}", msg.msg, msg.hostname.unwrap_or("<no hostname>".to_string()));
    }

    Ok(())
}

fn main() -> Result<()> {
    let fut = accept_loop("127.0.0.1:1514");
    task::block_on(fut)
}
