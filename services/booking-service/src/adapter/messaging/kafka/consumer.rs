use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;

use crate::domain::{BookingError, SeatReserved};
use crate::usecase::ConfirmBookingUseCase;

pub enum HandlerError {
    Transient(String),
    Permanent(String),
}

#[async_trait]
pub trait SagaHandler: Send + Sync + 'static {
    type Event: DeserializeOwned + Send + Sync;

    fn group_id(&self) -> &str;
    fn event_type(&self) -> &str;
    async fn handle(&self, ev: &Self::Event) -> Result<bool, HandlerError>;
}

pub struct SagaConsumer<H: SagaHandler> {
    consumer: StreamConsumer,
    dlq: FutureProducer,
    dlq_topic: String,
    handler: H,
    max_attempts: u32,
}

impl<H: SagaHandler> SagaConsumer<H> {
    pub fn new(
        brokers: &str,
        topic: &str,
        max_attempts: u32,
        handler: H,
    ) -> Result<Self, rdkafka::error::KafkaError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", handler.group_id())
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()?;
        consumer.subscribe(&[topic])?;

        let dlq: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()?;

        Ok(Self {
            consumer,
            dlq,
            dlq_topic: format!("{topic}.dlq"),
            handler,
            max_attempts: max_attempts.max(1),
        })
    }

    pub async fn run(self, shutdown: CancellationToken) {
        tracing::info!(
            group = self.handler.group_id(),
            max_attempts = self.max_attempts,
            "kafka consumer started"
        );

        loop {
            let msg = tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!(group = self.handler.group_id(), "kafka consumer stopping");
                    return;
                }
                res = self.consumer.recv() => match res {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::error!(err = %e, "kafka recv failed, retrying");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                },
            };

            match self.process(&msg).await {
                Ok(()) => {
                    if let Err(e) = self.consumer.commit_message(&msg, CommitMode::Sync) {
                        tracing::error!(err = %e, "offset commit failed; message may be redelivered");
                    }
                }
                Err(e) => {
                    tracing::error!(err = %e, "message not processed; will be redelivered");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn process(&self, msg: &BorrowedMessage<'_>) -> Result<(), String> {
        if let Some(event_type) = header_str(msg, "event_type") {
            if event_type != self.handler.event_type() {
                tracing::info!(
                    event_type,
                    "event_type not handled by this consumer, skipping"
                );
                return Ok(());
            }
        }

        let payload = msg.payload().ok_or_else(|| "empty payload".to_string())?;
        let ev: H::Event = match serde_json::from_slice(payload) {
            Ok(ev) => ev,
            Err(e) => {
                tracing::error!(err = %e, "undeserializable message -> dlq");
                return self.to_dlq(msg, &format!("parse: {e}")).await;
            }
        };

        let mut backoff = Duration::from_millis(250);
        for attempt in 1..=self.max_attempts {
            match self.handler.handle(&ev).await {
                Ok(already) => {
                    if already {
                        tracing::info!("duplicate event skipped");
                    } else {
                        tracing::info!("saga event processed");
                    }
                    return Ok(());
                }
                Err(HandlerError::Transient(m)) => {
                    if attempt >= self.max_attempts {
                        tracing::error!(attempts = attempt, err = %m, "max retries exhausted -> dlq");
                        return self
                            .to_dlq(msg, &format!("max-retries after {attempt} attempts: {m}"))
                            .await;
                    }
                    tracing::warn!(attempt, backoff_ms = backoff.as_millis() as u64, err = %m, "transient error, backing off");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
                Err(HandlerError::Permanent(m)) => {
                    tracing::error!(err = %m, "permanent error -> dlq");
                    return self.to_dlq(msg, &format!("permanent: {m}")).await;
                }
            }
        }

        Ok(())
    }

    async fn to_dlq(&self, msg: &BorrowedMessage<'_>, reason: &str) -> Result<(), String> {
        let partition = msg.partition().to_string();
        let offset = msg.offset().to_string();
        let headers = OwnedHeaders::new()
            .insert(Header {
                key: "x-dlq-reason",
                value: Some(reason),
            })
            .insert(Header {
                key: "x-dlq-source-topic",
                value: Some(msg.topic()),
            })
            .insert(Header {
                key: "x-dlq-source-partition",
                value: Some(partition.as_str()),
            })
            .insert(Header {
                key: "x-dlq-source-offset",
                value: Some(offset.as_str()),
            });

        let key: &[u8] = msg.key().unwrap_or(&[]);
        let payload: &[u8] = msg.payload().unwrap_or(&[]);
        let record = FutureRecord::to(&self.dlq_topic)
            .key(key)
            .payload(payload)
            .headers(headers);

        self.dlq
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(e, _)| format!("dlq send: {e}"))?;

        tracing::error!(reason, topic = %self.dlq_topic, "message dead-lettered");
        Ok(())
    }
}

fn header_str<'a>(msg: &'a BorrowedMessage<'a>, key: &str) -> Option<&'a str> {
    msg.headers()?
        .iter()
        .find(|h| h.key == key)?
        .value
        .and_then(|v| std::str::from_utf8(v).ok())
}

pub struct ConfirmBookingHandler {
    pub use_case: Arc<ConfirmBookingUseCase>,
}

#[async_trait]
impl SagaHandler for ConfirmBookingHandler {
    type Event = SeatReserved;

    fn group_id(&self) -> &str {
        "booking-service-SeatReserved"
    }

    fn event_type(&self) -> &str {
        "SeatReserved"
    }

    async fn handle(&self, ev: &SeatReserved) -> Result<bool, HandlerError> {
        self.use_case.execute(ev).await.map_err(|e| match e {
            BookingError::Repository(m) => HandlerError::Transient(m),
            other => HandlerError::Permanent(other.to_string()),
        })
    }
}
