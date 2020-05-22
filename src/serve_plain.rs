use async_std::{
    io::BufReader,
    net::{TcpListener, ToSocketAddrs},
    prelude::*,
    sync::Arc,
    task,
};
use crate::kafka::Kafka;
use crate::settings::*;
use crate::{read_logs, ConnectionState};
use dipstick::*;
use log::*;

/**
 * accept_loop will simply create the socket listener and dispatch newly accepted connections to
 * the connection_loop function
 */
pub async fn accept_loop(
    addr: impl ToSocketAddrs,
    settings: Arc<Settings>,
    metrics: Arc<StatsdScope>,
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
