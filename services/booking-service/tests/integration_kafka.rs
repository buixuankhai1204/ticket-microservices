mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use booking_service::adapter::messaging::kafka::consumer::{
    HandlerError, SagaConsumer, SagaHandler,
};
use common::{
    committed_offset, create_topic_with_max_message_bytes, create_topics, eventually,
    kafka_brokers, produce, read_all, set_max_message_bytes, unique,
};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Deserialize)]
struct Probe {
    id: String,
}

struct RecordingHandler {
    group: String,
    seen: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl SagaHandler for RecordingHandler {
    type Event = Probe;

    fn group_id(&self) -> &str {
        &self.group
    }

    fn event_type(&self) -> &str {
        "Probe"
    }

    async fn handle(&self, ev: &Probe) -> Result<bool, HandlerError> {
        self.seen.lock().unwrap().push(ev.id.clone());
        Ok(false)
    }
}

struct Running {
    brokers: String,
    topic: String,
    group: String,
    seen: Arc<Mutex<Vec<String>>>,
    shutdown: CancellationToken,
    join: tokio::task::JoinHandle<()>,
}

impl Running {
    async fn start(topic: String) -> Running {
        let brokers = kafka_brokers().await;
        let group = unique("it-group");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handler = RecordingHandler {
            group: group.clone(),
            seen: seen.clone(),
        };
        let consumer = SagaConsumer::new(&brokers, &topic, 3, handler).expect("create consumer");
        let shutdown = CancellationToken::new();
        let join = tokio::spawn(consumer.run(shutdown.clone()));
        Running {
            brokers,
            topic,
            group,
            seen,
            shutdown,
            join,
        }
    }

    async fn produce(&self, key: &str, payload: Option<&[u8]>, event_type: &str) {
        produce(&self.brokers, &self.topic, key, payload, Some(event_type)).await;
    }

    async fn committed(&self) -> i64 {
        committed_offset(&self.brokers, &self.group, &self.topic).await
    }

    async fn wait_for_commit(&self, offset: i64) {
        eventually(
            Duration::from_secs(60),
            &format!("offset {offset} to be committed"),
            || async { (self.committed().await == offset).then_some(()) },
        )
        .await;
    }

    async fn dead_letters(&self) -> Vec<common::Record> {
        read_all(&self.brokers, &format!("{}.dlq", self.topic)).await
    }

    fn handled(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }

    async fn stop(self) {
        self.shutdown.cancel();
        let _ = self.join.await;
    }
}

async fn topics_with_dlq() -> String {
    let brokers = kafka_brokers().await;
    let topic = unique("it.events");
    create_topics(&brokers, &[&topic, &format!("{topic}.dlq")]).await;
    topic
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker (or TEST_KAFKA_BROKERS)"]
async fn an_event_is_handled_before_its_offset_is_committed_and_another_groups_type_is_skipped() {
    let topic = topics_with_dlq().await;
    let consumer = Running::start(topic).await;

    consumer
        .produce("k0", Some(br#"{"id":"other"}"#), "SomethingElse")
        .await;
    consumer
        .produce("k1", Some(br#"{"id":"e1"}"#), "Probe")
        .await;

    consumer.wait_for_commit(2).await;
    assert_eq!(consumer.handled(), ["e1"]);
    assert!(consumer.dead_letters().await.is_empty());
    consumer.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker (or TEST_KAFKA_BROKERS)"]
async fn poison_and_empty_messages_are_dead_lettered_with_their_origin_and_the_next_message_gets_through(
) {
    let topic = topics_with_dlq().await;
    let consumer = Running::start(topic).await;

    consumer.produce("poison", Some(b"not-json"), "Probe").await;
    consumer.produce("empty", None, "Probe").await;
    consumer
        .produce("good", Some(br#"{"id":"good"}"#), "Probe")
        .await;

    consumer.wait_for_commit(3).await;
    let dead = consumer.dead_letters().await;
    assert_eq!(dead.len(), 2);
    assert_eq!(dead[0].key, b"poison");
    assert_eq!(dead[0].payload, b"not-json");
    assert!(dead[0]
        .header("x-dlq-reason")
        .unwrap()
        .starts_with("parse:"));
    assert_eq!(
        dead[0].header("x-dlq-source-topic"),
        Some(consumer.topic.as_str())
    );
    assert_eq!(dead[0].header("x-dlq-source-partition"), Some("0"));
    assert_eq!(dead[0].header("x-dlq-source-offset"), Some("0"));
    assert_eq!(dead[1].key, b"empty");
    assert_eq!(dead[1].header("x-dlq-source-offset"), Some("1"));
    assert_eq!(consumer.handled(), ["good"]);
    consumer.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker (or TEST_KAFKA_BROKERS)"]
async fn no_message_is_lost_while_the_dead_letter_write_keeps_failing() {
    let brokers = kafka_brokers().await;
    let topic = unique("it.events");
    let dlq = format!("{topic}.dlq");
    create_topics(&brokers, &[&topic]).await;
    create_topic_with_max_message_bytes(&brokers, &dlq, 2_000).await;
    let oversized_poison = vec![b'x'; 5_000];
    let consumer = Running::start(topic.clone()).await;
    consumer
        .produce("poison", Some(&oversized_poison), "Probe")
        .await;
    consumer
        .produce("good", Some(br#"{"id":"good"}"#), "Probe")
        .await;

    let window = Instant::now() + Duration::from_secs(8);
    while Instant::now() < window {
        let committed = consumer.committed().await;
        assert!(
            committed <= 0,
            "offset {committed} was committed while the poison message at offset 0 could not be dead-lettered: it is lost"
        );
        assert!(
            consumer.handled().is_empty(),
            "the consumer moved past the poison message"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    set_max_message_bytes(&brokers, &dlq, 1_048_576).await;

    consumer.wait_for_commit(2).await;
    let dead = consumer.dead_letters().await;
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].key, b"poison");
    assert_eq!(consumer.handled(), ["good"]);
    consumer.stop().await;
}
