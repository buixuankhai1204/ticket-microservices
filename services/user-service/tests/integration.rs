#![allow(dead_code, unused_imports)]

use std::sync::Arc;

use chrono::{Duration as ChronoDuration, Utc};
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

#[path = "../src/adapter/security/argon2_hasher.rs"]
mod argon2_hasher;

#[path = "../src/adapter/security/jwt_issuer.rs"]
mod jwt_issuer;

use argon2_hasher::Argon2PasswordHasher;
use domain::{DomainEvent, PasswordHasher, User, UserCreated, UserError};
use jwt_issuer::JwtTokenIssuer;
use platform::port::UserRepository;
use postgres_repo::PostgresUserRepository;
use usecase::{GetUserProfileUseCase, LoginUserUseCase, RegisterUserUseCase};

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

fn repo(_pool: &PgPool) -> Arc<PostgresUserRepository> {
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

const JWT_SECRET: &str = "integration-test-secret";

fn unique_email(prefix: &str) -> String {
    format!("{prefix}-{}@example.com", Uuid::new_v4())
}

#[tokio::test]
async fn register_persists_user() {
    let pool = fresh_db().await;
    let h = hasher();
    let register = RegisterUserUseCase::new(pool.clone(), repo(&pool), h.clone());

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
    assert_ne!(
        db_hash, password,
        "password must never be stored as plaintext"
    );
    assert!(
        h.verify(password, &db_hash).expect("verify"),
        "the stored hash must verify against the original password"
    );

    assert_eq!(
        count(&pool, "SELECT count(*) FROM outbox_events").await,
        0,
        "the outbox row must not linger after the transaction commits"
    );

    assert_eq!(count(&pool, "SELECT count(*) FROM users").await, 1);
}

#[tokio::test]
async fn register_rejects_duplicate_email_and_leaves_first_registration_intact() {
    let pool = fresh_db().await;
    let h = hasher();
    let register = RegisterUserUseCase::new(pool.clone(), repo(&pool), h.clone());

    let email = unique_email("dup");
    register
        .execute(email.clone(), "c".to_string())
        .await
        .expect("first register");

    let users_before = count(&pool, "SELECT count(*) FROM users").await;

    let err = register
        .execute(email.clone(), "second-password".to_string())
        .await
        .expect_err("duplicate email must be rejected");
    assert!(matches!(err, UserError::EmailAlreadyExists), "got {err:?}");

    assert_eq!(
        count(&pool, "SELECT count(*) FROM users").await,
        users_before,
        "no second users row from the rejected register"
    );

    let db_hash: String = sqlx::query_scalar("SELECT password_hash FROM users WHERE email = $1")
        .bind(&email)
        .fetch_one(&pool)
        .await
        .expect("row");
    assert!(
        h.verify("first-password", &db_hash).unwrap(),
        "the first registration's credential survived"
    );
    assert!(
        !h.verify("second-password", &db_hash).unwrap(),
        "the rejected registration did not overwrite it"
    );
}
