use crate::connection::*;
use crate::errors;
use crate::kafka::Kafka;
use crate::settings::Settings;
/**
 * The serve module is responsible for general syslog over TCP serving functionality
 */
use async_std::{io::BufReader, net::*, prelude::*, sync::Arc, task};
use async_trait::async_trait;
use dipstick::StatsdScope;
use log::*;

pub struct ServerState {
    /**
     * A reference to the global Settings object for all configuration information
     */
    pub settings: Arc<Settings>,
    /**
     * A valid and constructed StatsdScope for sending metrics along
     */
    pub metrics: Arc<StatsdScope>,
}

/**
 * The Server trait describes the necessary functionality to implement a new hotdog backend server
 * which can receive syslog messages
 */
#[async_trait]
pub trait Server {
    /**
     * Bootstrap can/should be overridden by implementations which need to perform some work prior
     * to the creation of the TcpListener and the incoming connection loop
     */
    fn bootstrap(&mut self, _state: &ServerState) -> Result<(), errors::HotdogError> {
        Ok(())
    }

    /**
     * Shutdown scan/should be overridden by implementations which need to perform some work after
     * the termination of the connection accept loop
     */
    fn shutdown(&self, _state: &ServerState) -> Result<(), errors::HotdogError> {
        Ok(())
    }

    /*
     * Handle a single connection
     */
    fn handle_connection(
        &self,
        stream: TcpStream,
        connection: Connection,
    ) -> Result<(), std::io::Error> {
        debug!("Accepting from: {}", stream.peer_addr()?);
        let reader = BufReader::new(stream);

        task::spawn(async move { connection.read_logs(reader).await });

        Ok(())
    }

    /**
     * Accept connections on the addr
     */
    async fn accept_loop(
        &mut self,
        addr: &str,
        state: ServerState,
    ) -> Result<(), errors::HotdogError> {
        let mut addr = addr.to_socket_addrs().await?;
        let addr = addr
            .next()
            .unwrap_or_else(|| panic!("Could not turn {:?} into a listenable interface", addr));

        let mut kafka = Kafka::new(state.settings.global.kafka.buffer);

        if !kafka.connect(
            &state.settings.global.kafka.conf,
            Some(state.settings.global.kafka.timeout_ms),
        ) {
            error!("Cannot start hotdog without a workable broker connection");
            return Err(errors::HotdogError::KafkaConnectError);
        }

        kafka.with_metrics(state.metrics.clone());
        let sender = kafka.get_sender();

        task::spawn(async move {
            debug!("Starting Kafka sendloop");
            kafka.sendloop();
        });

        self.bootstrap(&state)?;

        let listener = TcpListener::bind(addr).await?;
        let mut incoming = listener.incoming();

        while let Some(stream) = incoming.next().await {
            let stream = stream?;
            debug!("Accepting from: {}", stream.peer_addr()?);
            let connection = Connection::new(
                state.settings.clone(),
                state.metrics.clone(),
                sender.clone(),
            );
            if let Err(e) = self.handle_connection(stream, connection) {
                error!("Failed to handle_connection properly: {:?}", e);
            }
        }

        self.shutdown(&state)?;

        Ok(())
    }
}
