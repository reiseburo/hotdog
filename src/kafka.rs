/**
 * The Kafka module contains all the tooling/code necessary for connecting hotdog to Kafka for
 * sending log lines along as Kafka messages
 */

use async_std::sync::Arc;
use crossbeam::channel::{bounded, Receiver, Sender};
use dipstick::*;
use futures::*;
use futures::executor::ThreadPool;
use log::*;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::collections::HashMap;
use std::time::Duration;

/**
 * KafkaMessage just carries a message and its destination topic between tasks
 */
pub struct KafkaMessage {
    topic: String,
    msg: String,
}

impl KafkaMessage {
    pub fn new(topic: String, msg: String) -> KafkaMessage {
        KafkaMessage { topic, msg }
    }
}

/**
 * The Kafka struct acts as the primary interface between hotdog and Kafka
 */
pub struct Kafka {
    /*
     * I'm not super thrilled about wrapping the FutureProducer in an option, but it's the only way
     * that I can think to create an effective two-phase construction of this struct between
     * ::new() and the .connect() function
     */
    producer: Option<FutureProducer<DefaultClientContext>>,
    metrics: Option<Arc<LockingOutput>>,
    rx: Receiver<KafkaMessage>,
    tx: Sender<KafkaMessage>,
}

impl Kafka {
    pub fn new(message_max: usize) -> Kafka {
        let (tx, rx) = bounded(message_max);
        Kafka {
            metrics: None,
            producer: None,
            tx,
            rx,
        }
    }

    pub fn with_metrics(&mut self, metrics: Arc<LockingOutput>) {
        self.metrics = Some(metrics);
    }

    /**
     * connect() will inherently validate the configuration and perform a blocking call to the
     * configured bootstrap.servers in order to determine whether Kafka is reachable.
     *
     * Assuming Kafka can be reached, the connect() call will construct the producer and return
     * true
     *
     * If timeout_ms is not specified, a default 10s timeout will be used
     */
    pub fn connect(
        &mut self,
        rdkafka_conf: &HashMap<String, String>,
        timeout_ms: Option<Duration>,
    ) -> bool {
        let mut rd_conf = ClientConfig::new();
        for (key, value) in rdkafka_conf.iter() {
            rd_conf.set(key, value);
        }

        let consumer: BaseConsumer = rd_conf
            .create()
            .expect("Creation of Kafka consumer (for metadata) failed");

        let timeout = match timeout_ms {
            Some(ms) => ms,
            None => Duration::from_secs(10),
        };

        if let Ok(metadata) = consumer.fetch_metadata(None, timeout) {
            debug!("  Broker count: {}", metadata.brokers().len());
            debug!("  Topics count: {}", metadata.topics().len());
            debug!("  Metadata broker name: {}", metadata.orig_broker_name());
            debug!("  Metadata broker id: {}\n", metadata.orig_broker_id());

            self.producer = Some(
                rd_conf
                    .create()
                    .expect("Failed to create the Kafka producer!"),
            );

            return true;
        }

        warn!("Failed to connect to a Kafka broker");

        return false;
    }

    /**
     * get_sender() will return a cloned reference to the sender suitable for tasks or threads to
     * consume and take ownership of
     */
    pub fn get_sender(&self) -> Sender<KafkaMessage> {
        self.tx.clone()
    }

    /**
     * sendloop should be called in a thread/task and will never return
     */
    pub fn sendloop(&self) -> ! {
        if self.producer.is_none() {
            panic!("Cannot enter the sendloop() without a valid producer");
        }

        let pool = ThreadPool::new().unwrap();
        let producer = self.producer.as_ref().unwrap();

        // How long should we wait for an internal message to show up on our channel
        let timeout_ms = Duration::from_millis(100);

        // TODO: replace me with a select
        loop {
            if let Ok(kmsg) = self.rx.recv_timeout(timeout_ms) {
                let record = FutureRecord::to(&kmsg.topic)
                    .payload(&kmsg.msg)
                    .key(&kmsg.msg);

                /*
                 * Intentionally setting the timeout_ms to -1 here so this blocks forever if the
                 * outbound librdkafka queue is full. This will block up the crossbeam channel
                 * properly and cause messages to begin to be dropped, rather than buffering
                 * "forever" inside of hotdog
                 */
                if let Some(metrics) = &self.metrics {
                    metrics.timer("kafka.producer.sending").time(|| {
                        let m = metrics.clone();
                        let timer = metrics.timer("kafka.producer.sent");
                        let handle = timer.start();
                        let fut = producer.send(record, -1 as i64)
                            .then(move |res| {
                                info!("write completed: {:?}", res);
                                timer.stop(handle);
                                future::ok::<bool, bool>(true)
                            })
                            /* Need to obliterate the Output type defined on then's TryFuture with
                             * a map so this can be spawned off to the threadpool, which requires
                             * Future<Output = ()>
                             */
                            .map(|_| ());
                        /*
                         * Resolve this future off in the threadpool so we can report metrics once
                         * things are complete
                         */
                        pool.spawn_ok(fut);
                    });
                    metrics.counter("kafka.submitted").count(1)
                }
                else {
                    let _future = producer.send(record, -1 as i64);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /**
     * Test that trying to connect to a nonexistent cluster returns false
     */
    #[test]
    fn test_connect_bad_cluster() {
        let mut conf = HashMap::<String, String>::new();
        conf.insert(
            String::from("bootstrap.servers"),
            String::from("example.com:9092"),
        );

        let mut k = Kafka::new(0);
        assert_eq!(false, k.connect(&conf, Some(Duration::from_secs(1))));
    }
}
