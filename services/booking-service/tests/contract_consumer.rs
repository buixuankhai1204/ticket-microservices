use std::sync::Arc;

use booking_service::adapter::messaging::kafka::consumer::{
    CancelBookingHandler, ConfirmBookingHandler, SagaHandler,
};
use booking_service::adapter::repository::postgres::PostgresBookingRepository;
use booking_service::platform::port::{BookingRepository, PgTransactor, Transactor};
use booking_service::usecase::{CancelBookingUseCase, ConfirmBookingUseCase};
use pact_consumer::prelude::*;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

const PACT_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/pacts");

const UUID_PATTERN: &str = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$";
const TIMESTAMP_PATTERN: &str = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$";

const EVENT_ID: &str = "7c9e6679-7425-40de-944b-e07fc1f90ae7";
const BOOKING_ID: &str = "3f2b8c1e-5a47-4d0e-9a52-1c6f0e7d2b90";
const TICKETED_EVENT_ID: &str = "0d6f4b1a-93c2-4e55-8f3a-2b7c5d9e1a44";
const SEAT_ID: &str = "a1b2c3d4-e5f6-4789-8abc-def012345678";

struct Example {
    event_type: String,
    body: Vec<u8>,
}

fn examples(pact: &PactBuilder) -> Vec<Example> {
    pact.messages()
        .map(|message| Example {
            event_type: message.contents.metadata["event_type"]
                .as_str()
                .unwrap()
                .to_string(),
            body: message.contents.contents.value().unwrap().to_vec(),
        })
        .collect()
}

fn example_of<'a>(examples: &'a [Example], event_type: &str) -> &'a Example {
    examples
        .iter()
        .find(|example| example.event_type == event_type)
        .unwrap_or_else(|| panic!("the pact has no {event_type} interaction"))
}

fn handlers() -> (ConfirmBookingHandler, CancelBookingHandler) {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://nobody@127.0.0.1:1/none")
        .unwrap();
    let transactor: Arc<dyn Transactor> = Arc::new(PgTransactor::new(pool));
    let repository: Arc<dyn BookingRepository> = Arc::new(PostgresBookingRepository::new());
    (
        ConfirmBookingHandler::new(
            Arc::new(ConfirmBookingUseCase::new(
                transactor.clone(),
                repository.clone(),
            )),
            "",
        ),
        CancelBookingHandler::new(
            Arc::new(CancelBookingUseCase::new(transactor, repository)),
            "",
        ),
    )
}

fn parse<H: SagaHandler>(handler: &H, example: &Example) -> H::Event {
    assert_eq!(
        example.event_type,
        handler.event_type(),
        "the consumer filters on the event_type header the provider promises"
    );
    serde_json::from_slice(&example.body).expect("the consumer must understand the pact example")
}

fn uuid(raw: &str) -> Uuid {
    raw.parse().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seat_outcomes_from_the_event_service_are_understood_by_the_booking_consumers() {
    std::env::set_var("PACT_DO_NOT_TRACK", "true");
    let mut pact = PactBuilder::new_v4("booking-service", "event-service");
    pact.with_output_dir(PACT_DIR)
        .message_interaction("seats were reserved for a booking", |mut i| {
            i.given("a booking is waiting for its seats");
            i.metadata("event_type", "SeatReserved");
            i.json_body(json_pattern!({
                "event_id": term!(UUID_PATTERN, EVENT_ID),
                "booking_id": term!(UUID_PATTERN, BOOKING_ID),
                "ticketed_event_id": term!(UUID_PATTERN, TICKETED_EVENT_ID),
                "seat_ids": each_like!(term!(UUID_PATTERN, SEAT_ID)),
                "reserved_at": term!(TIMESTAMP_PATTERN, "2030-05-01T12:00:00Z")
            }));
            i
        })
        .message_interaction("seats could not be reserved for a booking", |mut i| {
            i.given("a booking is waiting for its seats");
            i.metadata("event_type", "SeatReservationFailed");
            i.json_body(json_pattern!({
                "event_id": term!(UUID_PATTERN, EVENT_ID),
                "booking_id": term!(UUID_PATTERN, BOOKING_ID),
                "ticketed_event_id": term!(UUID_PATTERN, TICKETED_EVENT_ID),
                "seat_ids": each_like!(term!(UUID_PATTERN, SEAT_ID)),
                "reason": term!("^(seat_unavailable|seat_not_found|event_not_found)$", "seat_unavailable"),
                "failed_at": term!(TIMESTAMP_PATTERN, "2030-05-01T12:00:00Z")
            }));
            i
        });
    let examples = examples(&pact);
    let (confirm, cancel) = handlers();

    let reserved = parse(&confirm, example_of(&examples, "SeatReserved"));
    let failed = parse(&cancel, example_of(&examples, "SeatReservationFailed"));

    assert_eq!(reserved.event_id, uuid(EVENT_ID));
    assert_eq!(reserved.booking_id, uuid(BOOKING_ID));
    assert_eq!(reserved.ticketed_event_id, uuid(TICKETED_EVENT_ID));
    assert_eq!(reserved.seat_ids, [uuid(SEAT_ID)]);
    assert_eq!(
        reserved.reserved_at.to_rfc3339(),
        "2030-05-01T12:00:00+00:00"
    );
    assert_eq!(failed.event_id, uuid(EVENT_ID));
    assert_eq!(failed.booking_id, uuid(BOOKING_ID));
    assert_eq!(failed.ticketed_event_id, uuid(TICKETED_EVENT_ID));
    assert_eq!(failed.seat_ids, [uuid(SEAT_ID)]);
    assert_eq!(failed.reason, "seat_unavailable");
    assert_eq!(failed.failed_at.to_rfc3339(), "2030-05-01T12:00:00+00:00");
}
