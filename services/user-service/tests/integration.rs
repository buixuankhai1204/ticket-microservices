#![allow(dead_code, unused_imports)]

use std::sync::Arc;
use std::time::Duration;

use chrono::Duration as ChronoDuration;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

use testcontainers_modules::postgres::Postgres as PostgresImage;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ContainerAsync;
use tokio::sync::OnceCell;

#[path = "../src/domain/mod.rs"]
mod domain;

#[path = "../src/platform/mod.rs"]
mod platform;

#[path = "../src/usecase/mod.rs"]
mod usecase;

#[path = "../src/adapter/repository/postgres.rs"]
mod postgres_repo;

#[path = "../src/adapter/http/dto.rs"]
mod dto;

#[path = "../src/adapter/security/argon2_hasher.rs"]
mod argon2_hasher;

#[path = "../src/adapter/security/jwt_issuer.rs"]
mod jwt_issuer;

use argon2_hasher::Argon2PasswordHasher;
use domain::{Pagination, PasswordHasher, UserError};
use dto::PaginatedUsersResponse;
use jwt_issuer::JwtTokenIssuer;
use postgres_repo::PostgresUserRepository;
use usecase::{GetUserProfileUseCase, ListUsersUseCase, LoginUserUseCase, RegisterUserUseCase};

const JWT_SECRET: &str = "integration-test-secret";

struct SharedPg {
    _container: ContainerAsync<PostgresImage>,
    base_url: String,
}

static SHARED_PG: OnceCell<SharedPg> = OnceCell::const_new();

async fn shared_pg() -> &'static SharedPg {
    SHARED_PG
        .get_or_init(|| async {
            let container = PostgresImage::default().start().await.expect(
                "failed to start the Postgres testcontainer - is Docker running? \
                 These are integration tests and will not run without it.",
            );
            let port = container
                .get_host_port_ipv4(5432)
                .await
                .expect("failed to map the Postgres container port");
            let base_url = format!("postgres://postgres:postgres@127.0.0.1:{port}");
            SharedPg {
                _container: container,
                base_url,
            }
        })
        .await
}

async fn fresh_db() -> PgPool {
    use sqlx::Connection;

    let shared = shared_pg().await;

    let mut admin = sqlx::PgConnection::connect(&format!("{}/postgres", shared.base_url))
        .await
        .expect("failed to open an admin connection");

    let name = format!("test_{}", Uuid::new_v4().simple());
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#))
        .execute(&mut admin)
        .await
        .expect("failed to create the isolated test database");
    let _ = admin.close().await;

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&format!("{}/{}", shared.base_url, name))
        .await
        .expect("failed to connect the test database pool");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("failed to run migrations on the test database");

    pool
}

async fn count(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(sql)
        .fetch_one(pool)
        .await
        .expect("count query")
}

fn repo() -> Arc<PostgresUserRepository> {
    Arc::new(PostgresUserRepository::new())
}

fn hasher() -> Arc<Argon2PasswordHasher> {
    Arc::new(Argon2PasswordHasher::new())
}

fn issuer() -> Arc<JwtTokenIssuer> {
    Arc::new(JwtTokenIssuer::new(
        JWT_SECRET,
        ChronoDuration::hours(1),
        "user-service".to_string(),
    ))
}

fn unique_email(prefix: &str) -> String {
    format!("{prefix}-{}@example.com", Uuid::new_v4())
}

#[tokio::test]
async fn register_persists_the_user_and_writes_the_user_created_outbox_row() {
    let pool = fresh_db().await;
    let h = hasher();
    let register = RegisterUserUseCase::new(pool.clone(), repo(), h.clone());

    let email = unique_email("register");
    let password = "correct horse battery staple";

    let user = register
        .execute(email.clone(), password.to_string())
        .await
        .expect("register should succeed");

    let (db_id, db_email, db_hash): (Uuid, String, String) =
        sqlx::query_as("SELECT id, email, password_hash FROM users WHERE email = $1")
            .bind(&email)
            .fetch_one(&pool)
            .await
            .expect("a users row must exist");

    assert_eq!(db_id, user.id);
    assert_eq!(db_email, email);
    assert_ne!(db_hash, password);
    assert!(h
        .verify(password, &db_hash)
        .expect("stored hash must verify"));

    assert_eq!(count(&pool, "SELECT count(*) FROM users").await, 1);

    let (evt_type, evt_aggregate, evt_payload): (String, Uuid, serde_json::Value) = sqlx::query_as(
        "SELECT event_type, aggregate_id, payload FROM outbox_events WHERE aggregate_id = $1",
    )
    .bind(user.id)
    .fetch_one(&pool)
    .await
    .expect("a UserCreated outbox row must exist");

    assert_eq!(evt_type, "UserCreated");
    assert_eq!(evt_aggregate, user.id);
    assert_eq!(evt_payload["user_id"], serde_json::json!(user.id));
    assert_eq!(evt_payload["email"], serde_json::json!(email));

    assert_eq!(
        count(&pool, "SELECT count(*) FROM outbox_events").await,
        1,
        "user-service write_outbox never issues the paired DELETE; the row lingers"
    );
}

#[tokio::test]
async fn register_rejects_a_duplicate_email_and_persists_nothing_new() {
    let pool = fresh_db().await;
    let register = RegisterUserUseCase::new(pool.clone(), repo(), hasher());

    let email = unique_email("dup");
    register
        .execute(email.clone(), "first-password".to_string())
        .await
        .expect("first register");

    let users_before = count(&pool, "SELECT count(*) FROM users").await;
    let outbox_before = count(&pool, "SELECT count(*) FROM outbox_events").await;

    let err = register
        .execute(email.clone(), "second-password".to_string())
        .await
        .expect_err("a duplicate email must be rejected");
    assert!(matches!(err, UserError::EmailAlreadyExists), "got {err:?}");

    assert_eq!(
        count(&pool, "SELECT count(*) FROM users").await,
        users_before
    );
    assert_eq!(
        count(&pool, "SELECT count(*) FROM outbox_events").await,
        outbox_before
    );

    let h = hasher();
    let db_hash: String = sqlx::query_scalar("SELECT password_hash FROM users WHERE email = $1")
        .bind(&email)
        .fetch_one(&pool)
        .await
        .expect("the first registration row");
    assert!(h.verify("first-password", &db_hash).expect("verify first"));
    assert!(!h
        .verify("second-password", &db_hash)
        .expect("verify second"));
}

#[tokio::test]
async fn login_with_valid_credentials_returns_a_token_and_writes_the_user_logged_in_outbox() {
    let pool = fresh_db().await;
    let register = RegisterUserUseCase::new(pool.clone(), repo(), hasher());
    let login = LoginUserUseCase::new(pool.clone(), repo(), hasher(), issuer());

    let email = unique_email("login");
    let password = "a-very-good-password";
    let user = register
        .execute(email.clone(), password.to_string())
        .await
        .expect("register should succeed");

    let token = login
        .execute(email.clone(), password.to_string())
        .await
        .expect("login should succeed");
    assert_eq!(token.split('.').count(), 3);

    let (evt_type, evt_aggregate): (String, Uuid) = sqlx::query_as(
        "SELECT event_type, aggregate_id FROM outbox_events WHERE event_type = 'UserLoggedIn'",
    )
    .fetch_one(&pool)
    .await
    .expect("a UserLoggedIn outbox row must exist");
    assert_eq!(evt_type, "UserLoggedIn");
    assert_eq!(evt_aggregate, user.id);

    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM outbox_events WHERE event_type = 'UserLoggedIn'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn login_with_a_wrong_password_is_rejected_and_writes_no_outbox() {
    let pool = fresh_db().await;
    let register = RegisterUserUseCase::new(pool.clone(), repo(), hasher());
    let login = LoginUserUseCase::new(pool.clone(), repo(), hasher(), issuer());

    let email = unique_email("badlogin");
    register
        .execute(email.clone(), "the-real-password".to_string())
        .await
        .expect("register should succeed");

    let err = login
        .execute(email.clone(), "not-the-password".to_string())
        .await
        .expect_err("a wrong password must be rejected");
    assert!(matches!(err, UserError::InvalidCredentials), "got {err:?}");

    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM outbox_events WHERE event_type = 'UserLoggedIn'"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn get_user_profile_returns_the_persisted_user() {
    let pool = fresh_db().await;
    let register = RegisterUserUseCase::new(pool.clone(), repo(), hasher());
    let get_profile = GetUserProfileUseCase::new(pool.clone(), repo());

    let email = unique_email("profile");
    let created = register
        .execute(email.clone(), "profile-password".to_string())
        .await
        .expect("register should succeed");

    let fetched = get_profile
        .execute(created.id)
        .await
        .expect("profile lookup should succeed");

    assert_eq!(fetched.id, created.id);
    assert_eq!(fetched.email, email);

    let drift = (fetched.created_at - created.created_at)
        .num_microseconds()
        .unwrap_or(i64::MAX)
        .abs();
    assert!(drift < 1_000, "created_at drifted {drift}us");
}

#[tokio::test]
async fn list_users_returns_a_paginated_envelope_newest_first() {
    let pool = fresh_db().await;
    let register = RegisterUserUseCase::new(pool.clone(), repo(), hasher());
    let list = ListUsersUseCase::new(pool.clone(), repo());

    let mut ids_in_order: Vec<Uuid> = Vec::new();
    for i in 0..3 {
        let created = register
            .execute(
                unique_email(&format!("list{i}")),
                "list-password".to_string(),
            )
            .await
            .expect("register should succeed");
        ids_in_order.push(created.id);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let newest_first: Vec<Uuid> = ids_in_order.iter().rev().copied().collect();

    let page_one = Pagination::new(2, 0).expect("valid pagination");
    let (rows_one, total_one) = list.execute(page_one).await.expect("first page");

    assert_eq!(total_one, 3);
    assert_eq!(rows_one.len(), 2);
    assert_eq!(
        rows_one.iter().map(|u| u.id).collect::<Vec<_>>(),
        newest_first[..2].to_vec()
    );

    let envelope_one = PaginatedUsersResponse::new(&rows_one, &page_one, total_one);
    assert_eq!(envelope_one.pagination.limit, 2);
    assert_eq!(envelope_one.pagination.offset, 0);
    assert_eq!(envelope_one.pagination.total, 3);
    assert!(envelope_one.pagination.has_more);
    assert_eq!(envelope_one.data.len(), 2);
    assert_eq!(envelope_one.data[0].id, newest_first[0]);

    let page_two = Pagination::new(2, 2).expect("valid pagination");
    let (rows_two, total_two) = list.execute(page_two).await.expect("second page");

    assert_eq!(total_two, 3);
    assert_eq!(rows_two.len(), 1);
    assert_eq!(rows_two[0].id, newest_first[2]);

    let envelope_two = PaginatedUsersResponse::new(&rows_two, &page_two, total_two);
    assert_eq!(envelope_two.pagination.offset, 2);
    assert!(!envelope_two.pagination.has_more);
}
