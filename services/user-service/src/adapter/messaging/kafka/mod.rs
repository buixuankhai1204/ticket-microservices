//! Kafka adapter for user-service.
//!
//! The outbox *publish* side no longer lives here: a Debezium PostgreSQL
//! connector (Kafka Connect) tails `outbox_events` from the WAL and publishes
//! via the Outbox Event Router SMT, so there is no in-process relay to run.
//!
//! This module is kept as the home for the planned *inbound* consumer (e.g.
//! reacting to a downstream saga event); `rdkafka` stays in `Cargo.toml` for it.
