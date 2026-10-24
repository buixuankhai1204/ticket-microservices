mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use common::{
    new_database, published_events, unique_email, Api, ScriptedEmail, ScriptedPayment, PASSWORD,
};
use pact_verifier::callback_executors::HttpRequestProviderStateExecutor;
use pact_verifier::{
    verify_provider_async, FilterInfo, NullRequestFilterExecutor, PactSource, ProviderInfo,
    ProviderTransport, VerificationOptions,
};
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;
use user_service::adapter::http::build_router;
use user_service::app::{App, AppConfig, Gateways};
use user_service::domain::RenewalPolicy;

const ANALYTICS_PACT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../analytics-service/pacts/analytics-service-user-service.json"
);
const MESSAGES_PATH: &str = "/pact-messages";
const STATES_PATH: &str = "/pact-states";
const USER_CREATED_DESCRIPTION: &str = "^a UserCreated event$";

#[derive(Clone)]
struct Bridge {
    pool: PgPool,
    service: String,
}

#[derive(Deserialize)]
struct StateChange {
    state: String,
    action: String,
}

async fn serve(router: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    address
}

async fn change_state(State(bridge): State<Bridge>, Json(change): Json<StateChange>) -> Response {
    if change.action != "setup" {
        return StatusCode::OK.into_response();
    }
    match change.state.as_str() {
        "a user has just registered" => {
            let (status, body) = Api::new(&bridge.service)
                .register(&unique_email(), PASSWORD)
                .await;
            if status == 201 {
                StatusCode::OK.into_response()
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("register answered {status}: {body}"),
                )
                    .into_response()
            }
        }
        other => (
            StatusCode::BAD_REQUEST,
            format!("no handler for the provider state {other:?}"),
        )
            .into_response(),
    }
}

async fn newest_published_event(State(bridge): State<Bridge>) -> Response {
    let Some(event) = published_events(&bridge.pool).await.pop() else {
        return (
            StatusCode::NOT_FOUND,
            "the provider state published no event",
        )
            .into_response();
    };
    let metadata = json!({
        "contentType": "application/json",
        "event_type": event.event_type,
        "topic": format!("{}.events", event.aggregate_type),
    });
    (
        [(
            "pact-message-metadata",
            STANDARD.encode(metadata.to_string()),
        )],
        Json(event.payload),
    )
        .into_response()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn the_user_created_event_is_what_the_analytics_service_expects() {
    let db = new_database().await;
    let app = App::new(
        db.pool.clone(),
        AppConfig {
            jwt_secret: "contract-test-secret".to_string(),
            jwt_issuer: "contract-test".to_string(),
            renewal_policy: RenewalPolicy::default(),
        },
        Gateways {
            payment: ScriptedPayment::approving(),
            email: ScriptedEmail::new(),
        },
    );
    let service = serve(build_router(app.state.clone())).await;
    let bridge = serve(
        Router::new()
            .route(STATES_PATH, post(change_state))
            .route(MESSAGES_PATH, post(newest_published_event))
            .with_state(Bridge {
                pool: db.pool.clone(),
                service: format!("http://{service}"),
            }),
    )
    .await;

    let provider = ProviderInfo {
        name: "user-service".to_string(),
        host: "127.0.0.1".to_string(),
        transports: vec![ProviderTransport {
            transport: "message".to_string(),
            port: Some(bridge.port()),
            path: Some(MESSAGES_PATH.to_string()),
            scheme: Some("http".to_string()),
        }],
        ..ProviderInfo::default()
    };
    let options = VerificationOptions::<NullRequestFilterExecutor> {
        coloured_output: false,
        ..VerificationOptions::default()
    };
    let states = HttpRequestProviderStateExecutor {
        state_change_url: Some(format!("http://{bridge}{STATES_PATH}")),
        retries: 0,
        ..HttpRequestProviderStateExecutor::default()
    };

    let verification = verify_provider_async(
        provider,
        vec![PactSource::File(ANALYTICS_PACT.to_string())],
        FilterInfo::Description(USER_CREATED_DESCRIPTION.to_string()),
        vec![],
        &options,
        None,
        &Arc::new(states),
        None,
    )
    .await
    .expect("the verifier could not run");

    assert!(verification.result, "{}", verification.output.join("\n"));
}
