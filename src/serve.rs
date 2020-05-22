/**
 * The serve module is responsible for general syslog over TCP serving functionality
 */

use async_std::{
    net::*,
    sync::Arc,
};
use async_trait::async_trait;
use crate::kafka::{Kafka, KafkaMessage};
use crate::settings::Settings;
use crossbeam::channel::Sender;
use dipstick::StatsdScope;
use log::*;

/**
 * ConnectionState carries the necessary types of state into new tasks for handling connections
 */
pub struct ConnectionState {
    /**
     * A reference to the global Settings object for all configuration information 
     */
    pub settings: Arc<Settings>,
    /**
     * A valid and constructed StatsdScope for sending metrics along
     */
    pub metrics: Arc<StatsdScope>,
    /**
     * The sender-side of the channel to our Kafka connection, allowing the logs read in to be
     * sent over to the Kafka handler
     */
    pub sender: Sender<KafkaMessage>,
}

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
 * A generic enum to wrap some of the potential errors a server can bubble up
 */
pub enum ServerError {
    GenericError,
    IOError {
        err: std::io::Error,
    },
}

/**
 * Allow standard io::Errors to be wrapped in a ServerError type
 */
impl std::convert::From<std::io::Error> for ServerError {
    fn from(err: std::io::Error) -> ServerError {
        ServerError::IOError { err }
    }
}


/**
 * The Server trait describes the necessary functionality to implement a new hotdog backend server
 * which can receive syslog messages
 */
#[async_trait]
pub trait Server {
    /**
     * Connect to the Kafka broker(s) specified in the settings and return Some() with a Kafka
     * struct if it exists
     */
    fn kafka_connect(&self, settings: Arc<Settings>) -> Option<Kafka> {
        let mut kafka = Kafka::new(settings.global.kafka.buffer);

        if !kafka.connect(
            &settings.global.kafka.conf,
            Some(settings.global.kafka.timeout_ms),
        ) {
            error!("Cannot start hotdog without a workable broker connection");
            return None;
        }
        Some(kafka)
    }

    fn bootstrap(&mut self) -> Result<(), ServerError> {
        Ok(())
    }

    async fn run_kafka(&self) -> Result<(), ServerError> {
        Ok(())
    }
}
