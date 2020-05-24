use crate::connection::*;
use crate::errors;
use crate::kafka::Kafka;
use crate::settings::Settings;
/**
 * The serve module is responsible for general syslog over TCP serving functionality
 */
use async_std::{io::BufReader, net::*, prelude::*, sync::Arc, task};
use async_trait::async_trait;
use crossbeam::channel::{bounded, Sender};
use dipstick::{InputScope, StatsdScope};
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

    /**
     * Handle a single connection
     *
     * The close_channel parameter must be a clone of our connection-tracking channel Sender
     */
    fn handle_connection(
        &self,
        stream: TcpStream,
        connection: Connection,
        close_channel: Sender<i64>,
    ) -> Result<(), std::io::Error> {
        debug!("Accepting from: {}", stream.peer_addr()?);
        let reader = BufReader::new(stream);

        task::spawn(async move {
            if let Err(e) = connection.read_logs(reader).await {
                error!("Failure occurred while read_logs executed: {:?}", e);
            }

            if let Err(e) = close_channel.send(-1) {
                error!("Somehow failed to track the channel close: {:?}", e);
            }
        });

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

        /*
         * Create a crossbeam channel to keep up with inbound connections
         */
        let (conn_tx, conn_rx) = bounded::<i64>(1);
        let counter = state.metrics.gauge("connections");

        task::spawn(async move {
            let mut connections = 0;
            loop {
                if let Ok(count) = conn_rx.recv() {
                    connections += count;
                    debug!("Connection count now {}", connections);
                    counter.value(connections);
                }
            }
        });

        self.bootstrap(&state)?;

        let listener = TcpListener::bind(addr).await?;
        let mut incoming = listener.incoming();

        while let Some(stream) = incoming.next().await {
            let stream = stream?;
            debug!("Accepting from: {}", stream.peer_addr()?);
            conn_tx.send(1).expect("Failed to send a connection count increment, something is seriously wrong");

            let connection = Connection::new(
                state.settings.clone(),
                state.metrics.clone(),
                sender.clone(),
            );
            if let Err(e) = self.handle_connection(stream, connection, conn_tx.clone()) {
                error!("Failed to handle_connection properly: {:?}", e);
            }
        }

        self.shutdown(&state)?;

        Ok(())
    }
}
