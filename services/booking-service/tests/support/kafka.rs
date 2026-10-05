use std::time::{Duration, Instant};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use uuid::Uuid;

pub fn brokers() -> String {
    std::env::var("KAFKA_TEST_BROKERS").unwrap_or_else(|_| "localhost:9094".to_string())
}

pub fn base_consumer(group: &str) -> BaseConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .create()
        .expect("kafka client")
}

pub async fn eventually(within: Duration, what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("timed out after {within:?} waiting for {what}");
}

pub async fn create_topic(name: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create()
        .expect("admin client");
    let results = admin
        .create_topics(
            &[NewTopic::new(name, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .expect("create topics");
    for r in results {
        r.unwrap_or_else(|(t, e)| panic!("create topic {t}: {e}"));
    }
    let probe = base_consumer(&format!("probe-{}", Uuid::new_v4()));
    eventually(Duration::from_secs(15), "topic leader", || {
        probe
            .fetch_metadata(Some(name), Duration::from_secs(2))
            .ok()
            .and_then(|m| {
                m.topics()
                    .first()
                    .map(|t| t.error().is_none() && t.partitions().iter().all(|p| p.leader() >= 0))
            })
            .unwrap_or(false)
    })
    .await;
}

pub async fn produce(topic: &str, key: &str, payload: Option<&str>, event_type: &str) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create()
        .expect("producer");
    let mut record = FutureRecord::to(topic)
        .key(key)
        .headers(OwnedHeaders::new().insert(Header {
            key: "event_type",
            value: Some(event_type),
        }));
    if let Some(p) = payload {
        record = record.payload(p);
    }
    producer
        .send(record, Duration::from_secs(10))
        .await
        .unwrap_or_else(|(e, _)| panic!("produce to {topic}: {e}"));
}
