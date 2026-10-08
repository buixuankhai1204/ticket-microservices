#![allow(dead_code)]

use std::any::Any;
use std::future::Future;
use std::net::TcpListener;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, NewTopic, ResourceSpecifier, TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{Offset, TopicPartitionList};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgConnection, PgPool, Row};
use testcontainers_modules::kafka::apache::{Kafka, KAFKA_PORT};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tokio::sync::OnceCell;
use uuid::Uuid;

pub const JWT_SECRET: &str = "booking-test-secret";
pub const JWT_ISSUER: &str = "booking-test-issuer";

static KEEP_ALIVE: Mutex<Vec<Box<dyn Any + Send>>> = Mutex::new(Vec::new());
static POSTGRES_ADMIN_URL: OnceCell<String> = OnceCell::const_new();
static KAFKA_BROKERS: OnceCell<String> = OnceCell::const_new();

fn infra_runtime() -> &'static tokio::runtime::Handle {
    static HANDLE: OnceLock<tokio::runtime::Handle> = OnceLock::new();
    HANDLE.get_or_init(|| {
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("build the infrastructure runtime");
            sender.send(runtime.handle().clone()).unwrap();
            runtime.block_on(std::future::pending::<()>());
        });
        receiver.recv().unwrap()
    })
}

async fn start_postgres() -> String {
    let node = Postgres::default()
        .with_tag("16-alpine")
        .with_cmd(["-c", "max_connections=400"])
        .start()
        .await
        .expect("failed to start the Postgres container (is Docker running?)");
    let port = node.get_host_port_ipv4(5432).await.unwrap();
    KEEP_ALIVE.lock().unwrap().push(Box::new(node));
    format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres")
}

async fn start_kafka() -> String {
    let node = Kafka::default()
        .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .start()
        .await
        .expect("failed to start the Kafka container (is Docker running?)");
    let port = node.get_host_port_ipv4(KAFKA_PORT).await.unwrap();
    KEEP_ALIVE.lock().unwrap().push(Box::new(node));
    format!("127.0.0.1:{port}")
}

pub async fn postgres_admin_url() -> String {
    POSTGRES_ADMIN_URL
        .get_or_init(|| async {
            if let Ok(url) = std::env::var("TEST_DATABASE_URL") {
                return url;
            }
            infra_runtime().spawn(start_postgres()).await.unwrap()
        })
        .await
        .clone()
}

pub async fn kafka_brokers() -> String {
    KAFKA_BROKERS
        .get_or_init(|| async {
            if let Ok(brokers) = std::env::var("TEST_KAFKA_BROKERS") {
                return brokers;
            }
            infra_runtime().spawn(start_kafka()).await.unwrap()
        })
        .await
        .clone()
}

pub struct TestDb {
    pub pool: PgPool,
    pub url: String,
    name: String,
    admin_url: String,
}

impl TestDb {
    pub async fn empty() -> TestDb {
        let admin_url = postgres_admin_url().await;
        let name = format!("t_{}", Uuid::new_v4().simple());
        let mut admin = PgConnection::connect(&admin_url)
            .await
            .expect("connect to the admin database");
        sqlx::query(&format!("CREATE DATABASE {name}"))
            .execute(&mut admin)
            .await
            .expect("create the throwaway database");
        let base = admin_url.rsplit_once('/').map(|(base, _)| base).unwrap();
        let url = format!("{base}/{name}");
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await
            .expect("connect to the throwaway database");
        TestDb {
            pool,
            url,
            name,
            admin_url,
        }
    }

    pub async fn migrated() -> TestDb {
        let db = TestDb::empty().await;
        booking_service::app::migrate(&db.pool)
            .await
            .expect("apply migrations");
        db
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let admin_url = self.admin_url.clone();
        let statement = format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name);
        let _ = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                if let Ok(mut admin) = PgConnection::connect(&admin_url).await {
                    let _ = sqlx::query(&statement).execute(&mut admin).await;
                }
            });
        })
        .join();
    }
}

pub struct Published {
    pub id: Uuid,
    pub aggregate_id: Uuid,
    pub aggregate_type: String,
    pub event_type: String,
    pub payload: Value,
}

pub async fn install_outbox_tap(pool: &PgPool) {
    let statements = [
        "CREATE TABLE IF NOT EXISTS outbox_tap (
            seq BIGSERIAL PRIMARY KEY,
            id UUID,
            aggregate_id UUID,
            aggregate_type TEXT,
            event_type TEXT,
            payload JSONB
        )",
        "CREATE OR REPLACE FUNCTION outbox_tap_copy() RETURNS trigger AS $$
         BEGIN
             INSERT INTO outbox_tap (id, aggregate_id, aggregate_type, event_type, payload)
             VALUES (NEW.id, NEW.aggregate_id, NEW.aggregate_type, NEW.event_type, NEW.payload);
             RETURN NEW;
         END
         $$ LANGUAGE plpgsql",
        "DROP TRIGGER IF EXISTS outbox_tap_after_insert ON outbox_events",
        "CREATE TRIGGER outbox_tap_after_insert AFTER INSERT ON outbox_events
         FOR EACH ROW EXECUTE FUNCTION outbox_tap_copy()",
    ];
    for statement in statements {
        sqlx::query(statement)
            .execute(pool)
            .await
            .expect("install the outbox tap");
    }
}

pub async fn published(pool: &PgPool) -> Vec<Published> {
    sqlx::query(
        "SELECT id, aggregate_id, aggregate_type, event_type, payload FROM outbox_tap ORDER BY seq",
    )
    .fetch_all(pool)
    .await
    .expect("read the outbox tap")
    .into_iter()
    .map(|row| Published {
        id: row.get("id"),
        aggregate_id: row.get("aggregate_id"),
        aggregate_type: row.get("aggregate_type"),
        event_type: row.get("event_type"),
        payload: row.get("payload"),
    })
    .collect()
}

pub async fn published_types(pool: &PgPool) -> Vec<String> {
    published(pool)
        .await
        .into_iter()
        .map(|event| event.event_type)
        .collect()
}

pub fn unique(name: &str) -> String {
    format!("{name}.{}", &Uuid::new_v4().simple().to_string()[..12])
}

fn client_config(brokers: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", brokers);
    config
}

pub async fn set_max_message_bytes(brokers: &str, topic: &str, bytes: u32) {
    let admin: AdminClient<DefaultClientContext> = client_config(brokers).create().unwrap();
    let limit = bytes.to_string();
    let results = admin
        .alter_configs(
            &[AlterConfig::new(ResourceSpecifier::Topic(topic)).set("max.message.bytes", &limit)],
            &AdminOptions::new(),
        )
        .await
        .unwrap();
    for result in results {
        result.unwrap_or_else(|(resource, code)| panic!("alter {resource:?}: {code}"));
    }
}

pub async fn create_topics(brokers: &str, topics: &[&str]) {
    let configured: Vec<(&str, Option<u32>)> = topics.iter().map(|topic| (*topic, None)).collect();
    create_configured_topics(brokers, &configured).await;
}

pub async fn create_topic_with_max_message_bytes(brokers: &str, topic: &str, bytes: u32) {
    create_configured_topics(brokers, &[(topic, Some(bytes))]).await;
}

async fn create_configured_topics(brokers: &str, configured: &[(&str, Option<u32>)]) {
    let topics: Vec<&str> = configured.iter().map(|(topic, _)| *topic).collect();
    let admin: AdminClient<DefaultClientContext> = client_config(brokers).create().unwrap();
    let limits: Vec<Option<String>> = configured
        .iter()
        .map(|(_, limit)| limit.map(|bytes| bytes.to_string()))
        .collect();
    let new_topics: Vec<_> = topics
        .iter()
        .zip(&limits)
        .map(|(topic, limit)| {
            let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
            match limit {
                Some(limit) => new_topic.set("max.message.bytes", limit),
                None => new_topic,
            }
        })
        .collect();
    let results = admin
        .create_topics(&new_topics, &AdminOptions::new())
        .await
        .unwrap();
    for result in results {
        if let Err((topic, code)) = result {
            panic!("could not create topic {topic}: {code}");
        }
    }
    for topic in topics {
        eventually(
            Duration::from_secs(30),
            "the topic to have a leader",
            || {
                let brokers = brokers.to_string();
                let topic = topic.to_string();
                async move {
                    tokio::task::spawn_blocking(move || {
                        let probe: BaseConsumer = client_config(&brokers)
                            .set("group.id", "topic-probe")
                            .create()
                            .unwrap();
                        let metadata = probe
                            .fetch_metadata(Some(&topic), Duration::from_secs(2))
                            .ok()?;
                        let ready = metadata.topics().first().is_some_and(|t| {
                            t.error().is_none() && t.partitions().iter().all(|p| p.leader() >= 0)
                        });
                        ready.then_some(())
                    })
                    .await
                    .unwrap()
                }
            },
        )
        .await;
    }
}

pub async fn produce(
    brokers: &str,
    topic: &str,
    key: &str,
    payload: Option<&[u8]>,
    event_type: Option<&str>,
) {
    let producer: FutureProducer = client_config(brokers).create().unwrap();
    let mut headers = OwnedHeaders::new();
    if let Some(event_type) = event_type {
        headers = headers.insert(Header {
            key: "event_type",
            value: Some(event_type),
        });
    }
    let mut record = FutureRecord::to(topic).key(key).headers(headers);
    if let Some(payload) = payload {
        record = record.payload(payload);
    }
    producer
        .send(record, Timeout::After(Duration::from_secs(10)))
        .await
        .map_err(|(error, _)| error)
        .unwrap_or_else(|error| panic!("produce to {topic}: {error}"));
}

#[derive(Debug)]
pub struct Record {
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
    pub headers: Vec<(String, String)>,
}

impl Record {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

pub async fn read_all(brokers: &str, topic: &str) -> Vec<Record> {
    let brokers = brokers.to_string();
    let topic = topic.to_string();
    tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = client_config(&brokers)
            .set("group.id", Uuid::new_v4().to_string())
            .set("enable.auto.commit", "false")
            .create()
            .unwrap();
        let (_, high) = consumer
            .fetch_watermarks(&topic, 0, Duration::from_secs(10))
            .expect("watermarks");
        let mut assignment = TopicPartitionList::new();
        assignment
            .add_partition_offset(&topic, 0, Offset::Beginning)
            .unwrap();
        consumer.assign(&assignment).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut records = Vec::new();
        while (records.len() as i64) < high && Instant::now() < deadline {
            if let Some(Ok(message)) = consumer.poll(Duration::from_millis(500)) {
                records.push(Record {
                    key: message.key().unwrap_or_default().to_vec(),
                    payload: message.payload().unwrap_or_default().to_vec(),
                    headers: message
                        .headers()
                        .map(|headers| {
                            headers
                                .iter()
                                .map(|h| {
                                    (
                                        h.key.to_string(),
                                        String::from_utf8_lossy(h.value.unwrap_or_default())
                                            .into_owned(),
                                    )
                                })
                                .collect()
                        })
                        .unwrap_or_default(),
                });
            }
        }
        records
    })
    .await
    .unwrap()
}

pub async fn committed_offset(brokers: &str, group: &str, topic: &str) -> i64 {
    let brokers = brokers.to_string();
    let group = group.to_string();
    let topic = topic.to_string();
    tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = client_config(&brokers)
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .create()
            .unwrap();
        let mut partitions = TopicPartitionList::new();
        partitions.add_partition(&topic, 0);
        match consumer.committed_offsets(partitions, Duration::from_secs(5)) {
            Ok(list) => match list.find_partition(&topic, 0).map(|p| p.offset()) {
                Some(Offset::Offset(offset)) => offset,
                _ => -1,
            },
            Err(_) => -1,
        }
    })
    .await
    .unwrap()
}

pub async fn eventually<T, F, Fut>(timeout: Duration, what: &str, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = check().await {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "timed out after {timeout:?} waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[derive(Serialize)]
struct Claims {
    sub: String,
    iss: String,
    exp: i64,
}

pub fn token_with(sub: &str, secret: &str, issuer: &str, expires_in_secs: i64) -> String {
    jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &Claims {
            sub: sub.to_string(),
            iss: issuer.to_string(),
            exp: chrono::Utc::now().timestamp() + expires_in_secs,
        },
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap()
}

pub fn token_for(user: Uuid) -> String {
    token_with(&user.to_string(), JWT_SECRET, JWT_ISSUER, 3600)
}

pub struct Api {
    pub base: String,
    http: reqwest::Client,
}

impl Api {
    pub fn new(base: impl Into<String>) -> Api {
        Api {
            base: base.into(),
            http: reqwest::Client::new(),
        }
    }

    pub async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        token: Option<&str>,
        body: Option<Value>,
    ) -> (u16, Value) {
        let mut request = self.http.request(method, format!("{}{path}", self.base));
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.expect("http request");
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    pub async fn try_status(&self, path: &str) -> Option<u16> {
        let response = self
            .http
            .get(format!("{}{path}", self.base))
            .send()
            .await
            .ok()?;
        Some(response.status().as_u16())
    }

    pub async fn get(&self, path: &str, token: Option<&str>) -> (u16, Value) {
        self.send(reqwest::Method::GET, path, token, None).await
    }

    pub async fn create_booking(
        &self,
        token: &str,
        event_id: Uuid,
        seats: &[Uuid],
    ) -> (u16, Value) {
        self.send(
            reqwest::Method::POST,
            "/api/v1/bookings",
            Some(token),
            Some(json!({ "event_id": event_id, "seat_ids": seats })),
        )
        .await
    }

    pub async fn create_booking_with_seats(&self, token: &str, seat_count: usize) -> Value {
        let seats: Vec<Uuid> = (0..seat_count).map(|_| Uuid::new_v4()).collect();
        let (status, body) = self.create_booking(token, Uuid::new_v4(), &seats).await;
        assert_eq!(status, 202, "create booking: {body}");
        body
    }

    pub async fn booking(&self, token: &str, id: &str) -> Value {
        self.get(&format!("/api/v1/bookings/{id}"), Some(token))
            .await
            .1
    }

    pub async fn wait_for_status(&self, token: &str, id: &str, wanted: &str) -> Value {
        eventually(
            Duration::from_secs(60),
            &format!("booking {id} to become {wanted}"),
            || async {
                let booking = self.booking(token, id).await;
                (booking["status"] == wanted).then_some(booking)
            },
        )
        .await
    }
}

pub fn seat_reserved(booking_id: &str, ticketed_event_id: Uuid) -> Value {
    json!({
        "event_id": Uuid::new_v4(),
        "booking_id": booking_id,
        "ticketed_event_id": ticketed_event_id,
        "seat_ids": [Uuid::new_v4()],
        "reserved_at": chrono::Utc::now(),
    })
}

pub fn seat_reservation_failed(booking_id: &str, ticketed_event_id: Uuid, reason: &str) -> Value {
    json!({
        "event_id": Uuid::new_v4(),
        "booking_id": booking_id,
        "ticketed_event_id": ticketed_event_id,
        "seat_ids": [Uuid::new_v4()],
        "reason": reason,
        "failed_at": chrono::Utc::now(),
    })
}
