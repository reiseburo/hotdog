/**
 * This module contains the necessary code to launch the internal status HTTP
 * server when so configured by the administrator
 *
 * The status module is also responsible for dispatching _all_ statsd metrics.
 */
use async_std::sync::{channel, Arc, Mutex, Receiver, Sender};
use dipstick::{InputScope, StatsdScope};
use log::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryInto;
use tide::{Request, Response, StatusCode};

/**
 * HealthResponse is the simple struct used for serializing statistics for the /stats healthcheck
 * endpoint
 */
#[derive(Deserialize, Serialize)]
struct HealthResponse {
    message: String,
    stats: HashMap<String, i64>,
}

/**
 * Launch the status server
 */
pub async fn status_server(
    listen_to: String,
    stats: Arc<StatsHandler>,
) -> Result<(), std::io::Error> {
    let mut app = tide::with_state(stats);
    debug!("Starting the status server on: {}", listen_to);

    app.at("/")
        .get(|_| async move { Ok("hotdog status server") });

    app.at("/stats")
        .get(|req: Request<Arc<StatsHandler>>| async move {
            let health = req.state().healthcheck().await;

            Ok(Response::new(StatusCode::Ok).body_json(&health)?)
        });

    app.listen(listen_to).await?;
    Ok(())
}

/**
 * Simple type for tracking our statistics as time goes on
 */
type ThreadsafeStats = Arc<Mutex<HashMap<String, i64>>>;
pub type Statistic = (Stats, i64);

pub struct StatsHandler {
    values: ThreadsafeStats,
    metrics: Arc<StatsdScope>,
    rx: Receiver<Statistic>,
    pub tx: Sender<Statistic>,
}

impl StatsHandler {
    pub fn new(metrics: Arc<StatsdScope>) -> Self {
        let (tx, rx) = channel(1_000_000);
        let values = Arc::new(Mutex::new(HashMap::<String, i64>::new()));

        StatsHandler {
            values,
            metrics,
            rx,
            tx,
        }
    }

    /**
     * The runloop will simply read from the channel and record statistics as
     * they come in
     */
    pub async fn runloop(&self) {
        loop {
            if let Ok((stat, count)) = self.rx.recv().await {
                trace!("Received stat to record: {} - {}", stat, count);

                match stat {
                    Stats::ConnectionCount => {
                        self.handle_gauge(stat, count).await;
                    }
                    Stats::KafkaMsgSent => {
                        self.handle_timer(stat, count).await;
                    }
                    _ => {
                        self.handle_counter(stat, count).await;
                    }
                }
            }
        }
    }

    /**
     * Update the internal map with a new count like it is a gauge
     */
    async fn handle_gauge(&self, stat: Stats, count: i64) {
        let key = &stat.to_string();
        let mut values = self.values.lock().await;
        let new_count = values.get(key).unwrap_or(&0) + count;

        self.metrics.gauge(key).value(new_count);

        values.insert(key.to_string(), new_count);
    }

    /**
     * Update the internal map with a new count like it is a counter
     */
    async fn handle_counter(&self, stat: Stats, count: i64) {
        let key = &stat.to_string();
        let mut values = self.values.lock().await;
        let new_count = values.get(key).unwrap_or(&0) + count;

        let sized_count: usize = count.try_into().expect("Could not convert to usize!");

        self.metrics.counter(key).count(sized_count);

        /* Handle special case enums which have more data associated */
        match &stat {
            Stats::KafkaMsgSubmitted { topic } => {
                let subkey = &*format!("{}.{}", key, topic);
                self.metrics.counter(subkey).count(sized_count);
                values.insert(subkey.to_string(), new_count);
            }
            Stats::KafkaMsgErrored { errcode } => {
                let subkey = &*format!("{}.{}", key, errcode);
                self.metrics.counter(subkey).count(sized_count);
                values.insert(subkey.to_string(), new_count);
            }
            _ => {}
        };

        values.insert(key.to_string(), new_count);
    }

    /**
     * Update the internal map with the latest timero
     */
    async fn handle_timer(&self, stat: Stats, duration_us: i64) {
        let key = &stat.to_string();
        let mut values = self.values.lock().await;
        if let Ok(duration) = duration_us.try_into() {
            self.metrics.timer(key).interval_us(duration);
        } else {
            error!("Failed to report timer to statsd with an i64 that couldn't fit into u64");
        }
        values.insert(key.to_string(), duration_us);
    }

    /**
     * Take the internal values map and generated a HealthResponse struct for
     * the /stats url to respond with
     */
    async fn healthcheck(&self) -> HealthResponse {
        let mut stats = HashMap::new();

        for (key, value) in self.values.lock().await.iter() {
            stats.insert(key.to_string(), *value);
        }

        HealthResponse {
            message: "You should smile more".into(),
            stats,
        }
    }
}

#[derive(Debug, Display, Hash, PartialEq, Eq)]
pub enum Stats {
    /* Gauges */
    #[strum(serialize = "connections")]
    ConnectionCount,

    /* Counters */
    #[strum(serialize = "lines")]
    LineReceived,
    #[strum(serialize = "kafka.submitted")]
    KafkaMsgSubmitted { topic: String },
    #[strum(serialize = "kafka.producer.error")]
    KafkaMsgErrored { errcode: String },
    #[strum(serialize = "error.log_parse")]
    LogParseError,
    #[strum(serialize = "error.full_internal_queue")]
    FullInternalQueueError,
    #[strum(serialize = "error.topic_parse_failed")]
    TopicParseFailed,
    #[strum(serialize = "error.internal_push_failed")]
    InternalPushError,
    #[strum(serialize = "error.merge_of_invalid_json")]
    MergeInvalidJsonError,
    #[strum(serialize = "error.merge_target_not_json")]
    MergeTargetNotJsonError,

    /* Timers */
    #[strum(serialize = "kafka.producer.sent")]
    KafkaMsgSent,
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
