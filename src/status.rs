/**
 * This module contains the necessary code to launch the internal status HTTP
 * server when so configured by the administrator
 */

use log::*;
use tide;


/**
 * Launch the status server
 */
pub async fn status_server(listen_to: String) -> Result<(), std::io::Error> {
    debug!("Starting the status server on: {}", listen_to);
    let mut app = tide::new();
    app.at("/").get(|_| async {
        Ok("Welcome to Hotdog")
        });
    app.listen(listen_to).await?;
    Ok(())
}

pub struct StatsHandler {
}

#[derive(Debug, Display)]
pub enum Stats {
    #[strum(serialize="connections")]
    ConnectionCount,
    #[strum(serialize="lines")]
    LineReceived,
    #[strum(serialize="kafka.submitted")]
    KafkaMsgSubmitted {
        topic: String,
    },
    #[strum(serialize="kafka.producer.sent")]
    KafkaMsgSent,
    #[strum(serialize="kafka.producer.error")]
    KafkaMsgErrored {
        errcode: String,
    },
    LogParseError,
    FullInternalQueueError,
    InternalPushError,
    MergeInvalidJsonError,
    MergeTargetNotJsonError,
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanity_check_strum_serialize() {
        let s = Stats::ConnectionCount.to_string();
        assert_eq!("connections", s);
    }
}
