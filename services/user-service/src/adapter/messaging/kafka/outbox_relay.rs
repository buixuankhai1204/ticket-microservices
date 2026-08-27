use std::error::Error;
use std::time::Duration;

use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

type BoxError = Box<dyn Error + Send + Sync>;

/// Background task that drains `outbox_events` to Kafka. Started from `main` and
/// cancelled by the same shutdown signal as the HTTP server.
///
/// At-least-once: if the process dies between the Kafka `send` and stamping
/// `published_at`, the row is re-published on the next run. Consumers must be
/// idempotent — this side deliberately does not try to be exactly-once.
pub struct OutboxRelay {
    pool: PgPool,
    producer: FutureProducer,
    topic: String,
    poll_interval: Duration,
    batch_size: i64,
}

#[derive(sqlx::FromRow)]
struct OutboxRow {
    id: Uuid,
    aggregate_id: Uuid,
    event_type: String,
    payload: serde_json::Value,
}

impl OutboxRelay {
    pub fn new(pool: PgPool, producer: FutureProducer, topic: String) -> Self {
        Self {
            pool,
            producer,
            topic,
            poll_interval: Duration::from_millis(500),
            batch_size: 100,
        }
    }

    /// Polls until `shutdown` is cancelled. A failed poll is logged and retried
    /// on the next tick — the unpublished rows are still there.
    pub async fn run(self, shutdown: CancellationToken) {
        tracing::info!(topic = %self.topic, "outbox relay started");
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("outbox relay stopping");
                    return;
                }
                _ = tokio::time::sleep(self.poll_interval) => {}
            }

            if let Err(err) = self.drain_once().await {
                tracing::error!(error = %err, "outbox relay poll failed; will retry next tick");
            }
        }
    }

    /// Publishes currently-unpublished rows, oldest first, in `batch_size`
    /// chunks. Rows are locked with `FOR UPDATE SKIP LOCKED` so a second relay
    /// instance never double-sends the same row. Each row that publishes
    /// successfully is marked `published_at` in the batch transaction.
    ///
    /// A publish failure stops the current chunk (we never skip past an
    /// unpublished row and reorder a later one), commits whatever already
    /// succeeded, and returns `Ok` — the next tick retries from the failed row.
    /// So a transient broker outage just delays delivery; it never kills the
    /// relay task or loses a row.
    async fn drain_once(&self) -> Result<(), BoxError> {
        loop {
            let mut tx = self.pool.begin().await?;

            let rows = sqlx::query_as::<_, OutboxRow>(
                "SELECT id, aggregate_id, event_type, payload \
                 FROM outbox_events \
                 WHERE published_at IS NULL \
                 ORDER BY created_at \
                 LIMIT $1 \
                 FOR UPDATE SKIP LOCKED",
            )
            .bind(self.batch_size)
            .fetch_all(&mut *tx)
            .await?;

            if rows.is_empty() {
                tx.commit().await?;
                return Ok(());
            }

            let mut published = 0usize;
            let mut stalled = false;
            for row in &rows {
                // Key by aggregate_id so all events about one user land on the
                // same partition and keep their order.
                let key = row.aggregate_id.to_string();
                let payload = serde_json::to_vec(&row.payload)?;
                let headers = OwnedHeaders::new().insert(Header {
                    key: "event_type",
                    value: Some(row.event_type.as_bytes()),
                });
                let record = FutureRecord::to(&self.topic)
                    .key(&key)
                    .payload(&payload)
                    .headers(headers);

                match self
                    .producer
                    .send(record, Timeout::After(Duration::from_secs(10)))
                    .await
                {
                    Ok(_) => {
                        sqlx::query("UPDATE outbox_events SET published_at = now() WHERE id = $1")
                            .bind(row.id)
                            .execute(&mut *tx)
                            .await?;
                        published += 1;
                    }
                    Err((err, _)) => {
                        tracing::error!(
                            error = %err,
                            event_id = %row.id,
                            topic = %self.topic,
                            "outbox publish failed; will retry from this row next tick"
                        );
                        stalled = true;
                        break;
                    }
                }
            }

            tx.commit().await?;
            if published > 0 {
                tracing::info!(count = published, topic = %self.topic, "relayed outbox events");
            }

            if stalled || (rows.len() as i64) < self.batch_size {
                return Ok(());
            }
        }
    }
}
