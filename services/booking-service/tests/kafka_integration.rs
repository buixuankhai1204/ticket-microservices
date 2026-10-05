use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rdkafka::consumer::Consumer;
use rdkafka::message::{Headers, Message};
use rdkafka::{Offset, TopicPartitionList};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod support;

use support::kafka::{base_consumer, brokers, create_topic, eventually, produce};

use booking_service::adapter::messaging::kafka::consumer::{
    HandlerError, SagaConsumer, SagaHandler,
};

#[derive(Debug, Clone, Deserialize)]
struct ProbeEvent {
    id: String,
}

#[derive(Clone, Copy)]
enum Fail {
    Transient,
    Permanent,
}

struct Shared {
    script: Vec<Option<Fail>>,
    seen: Mutex<Vec<ProbeEvent>>,
}

struct ScriptedHandler {
    group: String,
    shared: Arc<Shared>,
}

#[async_trait]
impl SagaHandler for ScriptedHandler {
    type Event = ProbeEvent;

    fn group_id(&self) -> &str {
        &self.group
    }

    fn event_type(&self) -> &str {
        "Probe"
    }

    async fn handle(&self, ev: &ProbeEvent) -> Result<bool, HandlerError> {
        let mut seen = self.shared.seen.lock().unwrap();
        let i = seen.len();
        seen.push(ev.clone());
        let step = if self.shared.script.is_empty() {
            None
        } else {
            self.shared.script[i.min(self.shared.script.len() - 1)]
        };
        match step {
            None => Ok(false),
            Some(Fail::Transient) => Err(HandlerError::Transient("pool acquire timeout".into())),
            Some(Fail::Permanent) => Err(HandlerError::Permanent("constraint violated".into())),
        }
    }
}

struct Case {
    name: &'static str,
    script: Vec<Option<Fail>>,
    want_calls: usize,
    want_dead: usize,
    want_reason: &'static str,
}

struct DeadRecord {
    key: Vec<u8>,
    payload: Vec<u8>,
    headers: HashMap<String, String>,
}

async fn new_topics(with_dlq: bool) -> (String, String) {
    let id = &Uuid::new_v4().to_string()[..8];
    let topic = format!("it-{id}");
    let group = format!("it-group-{id}");
    create_topic(&topic).await;
    if with_dlq {
        create_topic(&format!("{topic}.dlq")).await;
    }
    (topic, group)
}

fn committed_offset(group: &str, topic: &str) -> i64 {
    let c = base_consumer(group);
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(topic, 0);
    match c.committed_offsets(tpl, Duration::from_secs(5)) {
        Ok(list) => match list.find_partition(topic, 0).map(|p| p.offset()) {
            Some(Offset::Offset(n)) => n,
            _ => -1,
        },
        Err(_) => -1,
    }
}

fn read_all(topic: &str) -> Vec<DeadRecord> {
    let c = base_consumer(&format!("reader-{}", Uuid::new_v4()));
    let (_, high) = c
        .fetch_watermarks(topic, 0, Duration::from_secs(5))
        .expect("watermarks");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, Offset::Beginning)
        .expect("offset");
    c.assign(&tpl).expect("assign");
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut out = Vec::new();
    while (out.len() as i64) < high && Instant::now() < deadline {
        if let Some(Ok(m)) = c.poll(Duration::from_millis(500)) {
            let mut headers = HashMap::new();
            if let Some(hs) = m.headers() {
                for h in hs.iter() {
                    headers.insert(
                        h.key.to_string(),
                        h.value
                            .map(|v| String::from_utf8_lossy(v).into_owned())
                            .unwrap_or_default(),
                    );
                }
            }
            out.push(DeadRecord {
                key: m.key().unwrap_or_default().to_vec(),
                payload: m.payload().unwrap_or_default().to_vec(),
                headers,
            });
        }
    }
    out
}

struct Running {
    token: CancellationToken,
    join: tokio::task::JoinHandle<()>,
}

impl Running {
    async fn stop(self) {
        self.token.cancel();
        let _ = self.join.await;
    }
}

fn start_consumer(topic: &str, group: &str, max_attempts: u32, shared: Arc<Shared>) -> Running {
    let handler = ScriptedHandler {
        group: group.to_string(),
        shared,
    };
    let consumer = SagaConsumer::new(&brokers(), topic, max_attempts, handler).expect("consumer");
    let token = CancellationToken::new();
    let join = tokio::spawn(consumer.run(token.clone()));
    Running { token, join }
}

fn shared(script: Vec<Option<Fail>>) -> Arc<Shared> {
    Arc::new(Shared {
        script,
        seen: Mutex::new(Vec::new()),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs kafka: docker compose up -d --wait kafka"]
async fn handles_a_matching_event_and_commits_its_offset() {
    let (topic, group) = new_topics(true).await;
    let state = shared(vec![]);
    produce(&topic, "k1", Some(r#"{"id":"e1"}"#), "Probe").await;

    let running = start_consumer(&topic, &group, 3, state.clone());

    eventually(Duration::from_secs(45), "offset 1 to be committed", || {
        committed_offset(&group, &topic) == 1
    })
    .await;
    running.stop().await;
    let seen = state.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].id, "e1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs kafka: docker compose up -d --wait kafka"]
async fn skips_and_commits_an_event_type_owned_by_another_group() {
    let (topic, group) = new_topics(true).await;
    let state = shared(vec![]);
    produce(&topic, "k2", Some(r#"{"id":"e2"}"#), "SomethingElse").await;

    let running = start_consumer(&topic, &group, 3, state.clone());

    eventually(Duration::from_secs(45), "offset 1 to be committed", || {
        committed_offset(&group, &topic) == 1
    })
    .await;
    running.stop().await;
    assert!(state.seen.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs kafka: docker compose up -d --wait kafka"]
async fn dead_letters_poison_and_empty_messages_with_their_origin_and_commits() {
    let (topic, group) = new_topics(true).await;
    let state = shared(vec![]);
    produce(&topic, "k3", Some("not-json"), "Probe").await;
    produce(&topic, "k4", None, "Probe").await;

    let running = start_consumer(&topic, &group, 3, state.clone());

    eventually(Duration::from_secs(60), "offset 2 to be committed", || {
        committed_offset(&group, &topic) == 2
    })
    .await;
    running.stop().await;
    let dead = read_all(&format!("{topic}.dlq"));
    assert_eq!(dead.len(), 2);
    assert_eq!(dead[0].key, b"k3");
    assert_eq!(dead[0].payload, b"not-json");
    assert!(dead[0].headers["x-dlq-reason"].starts_with("parse:"));
    assert_eq!(dead[0].headers["x-dlq-source-topic"], topic);
    assert_eq!(dead[0].headers["x-dlq-source-partition"], "0");
    assert_eq!(dead[0].headers["x-dlq-source-offset"], "0");
    assert_eq!(dead[1].key, b"k4");
    assert!(dead[1].headers["x-dlq-reason"].starts_with("parse:"));
    assert_eq!(dead[1].headers["x-dlq-source-offset"], "1");
    assert!(state.seen.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs kafka: docker compose up -d --wait kafka"]
async fn classifies_handler_failures() {
    let cases = vec![
        Case {
            name: "permanent goes straight to the dlq",
            script: vec![Some(Fail::Permanent)],
            want_calls: 1,
            want_dead: 1,
            want_reason: "permanent:",
        },
        Case {
            name: "transient then success retries without a dlq",
            script: vec![Some(Fail::Transient), Some(Fail::Transient), None],
            want_calls: 3,
            want_dead: 0,
            want_reason: "",
        },
        Case {
            name: "transient forever dead-letters after max attempts",
            script: vec![Some(Fail::Transient)],
            want_calls: 3,
            want_dead: 1,
            want_reason: "max-retries after 3 attempts",
        },
    ];
    let mut tasks = Vec::new();
    for case in cases {
        tasks.push(tokio::spawn(async move {
            let Case {
                name,
                script,
                want_calls,
                want_dead,
                want_reason,
            } = case;
            let (topic, group) = new_topics(true).await;
            let state = shared(script);
            produce(&topic, "k5", Some(r#"{"id":"e5"}"#), "Probe").await;

            let running = start_consumer(&topic, &group, 3, state.clone());

            eventually(Duration::from_secs(60), "offset 1 to be committed", || {
                committed_offset(&group, &topic) == 1
            })
            .await;
            running.stop().await;
            assert_eq!(state.seen.lock().unwrap().len(), want_calls, "{name}");
            let dead = read_all(&format!("{topic}.dlq"));
            assert_eq!(dead.len(), want_dead, "{name}");
            if want_dead == 1 {
                assert!(
                    dead[0].headers["x-dlq-reason"].starts_with(want_reason),
                    "{name}"
                );
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs kafka: docker compose up -d --wait kafka"]
async fn never_loses_a_message_while_the_dlq_is_unavailable() {
    let (topic, group) = new_topics(false).await;
    let state = shared(vec![]);
    produce(&topic, "poison", Some("not-json"), "Probe").await;
    produce(&topic, "good", Some(r#"{"id":"good"}"#), "Probe").await;

    let running = start_consumer(&topic, &group, 3, state.clone());

    let window = Instant::now() + Duration::from_secs(45);
    while Instant::now() < window {
        let got = committed_offset(&group, &topic);
        assert!(
            got <= 0,
            "offset {got} was committed while the poison message at offset 0 could not be dead-lettered: it is lost"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    create_topic(&format!("{topic}.dlq")).await;

    eventually(
        Duration::from_secs(120),
        "both messages to be committed once the dlq exists",
        || committed_offset(&group, &topic) == 2,
    )
    .await;
    running.stop().await;
    let dead = read_all(&format!("{topic}.dlq"));
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].key, b"poison");
    let seen = state.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].id, "good");
}
