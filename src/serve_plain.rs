/**
 * This module is responsible for receiving connections over plaintext TCP
 */
use async_std::{
    io::BufReader,
    net::{TcpListener, ToSocketAddrs},
    prelude::*,
    sync::Arc,
    task,
};
use crate::kafka::Kafka;
use crate::serve::*;
use crate::settings::*;
use crate::read_logs;
use dipstick::*;
use log::*;

pub struct PlaintextServer {
}

impl PlaintextServer {
    /**
    * accept_loop will simply create the socket listener and dispatch newly accepted connections to
    * the connection_loop function
    */
    pub async fn accept_loop(
        &self,
        addr: impl ToSocketAddrs,
        settings: Arc<Settings>,
        metrics: Arc<StatsdScope>,
    ) -> std::result::Result<(), ServerError> {

        let mut kafka = self.kafka_connect(settings.clone())
            .expect("Failed to connect to Kafka properly");
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
}

impl Server for PlaintextServer {
}

