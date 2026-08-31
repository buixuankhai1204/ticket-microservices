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
async fn register_persists_user_and_leaves_no_outbox_row() {
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
        db_hash.starts_with("$argon2"),
        "stored credential should be an argon2 PHC string, got {db_hash}"
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
async fn entity_ids_are_random_uuidv4_not_sequential() {
    let pool = fresh_db().await;
    let register = RegisterUserUseCase::new(pool.clone(), repo(&pool), hasher());

    let mut ids = Vec::new();
    for i in 0..4 {
        let user = register
            .execute(unique_email(&format!("seq-{i}")), "pw".to_string())
            .await
            .expect("register");
        assert_eq!(
            user.id.get_version_num(),
            4,
            "entity id must be a v4 (random) UUID, not an auto-increment integer"
        );
        ids.push(user.id.as_u128());
    }

    for pair in ids.windows(2) {
        assert!(
            pair[0].abs_diff(pair[1]) > 1,
            "consecutive entity ids must not be adjacent integers (guessable / enumerable for IDOR)"
        );
    }
}

#[tokio::test]
async fn register_rejects_duplicate_email_and_leaves_first_registration_intact() {
    let pool = fresh_db().await;
    let h = hasher();
    let register = RegisterUserUseCase::new(pool.clone(), repo(&pool), h.clone());

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
        .expect_err("duplicate email must be rejected");
    assert!(matches!(err, UserError::EmailAlreadyExists), "got {err:?}");

    assert_eq!(
        count(&pool, "SELECT count(*) FROM users").await,
        users_before,
        "no second users row from the rejected register"
    );
    assert_eq!(
        count(&pool, "SELECT count(*) FROM outbox_events").await,
        outbox_before,
        "no orphan outbox row from the rejected register"
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

#[tokio::test]
async fn users_insert_rolls_back_when_the_outbox_insert_in_the_same_tx_fails() {
    let pool = fresh_db().await;
    let repository = PostgresUserRepository::new();

    let poison_event_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO outbox_events (id, aggregate_id, aggregate_type, event_type, payload) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(poison_event_id)
    .bind(Uuid::new_v4())
    .bind("user")
    .bind("Preexisting")
    .bind(serde_json::json!({ "seeded": true }))
    .execute(&pool)
    .await
    .expect("seed conflicting outbox row");

    let email = unique_email("atomic");
    let mut user = User::new(email.clone(), "argon2-placeholder-hash".to_string()).expect("valid");
    user.record_event(DomainEvent::UserCreated(UserCreated {
        event_id: poison_event_id,
        user_id: user.id,
        email: user.email.clone(),
        created_at: user.created_at,
    }));

    let mut tx = pool.begin().await.expect("begin");
    let result = repository.create(&mut tx, &user).await;
    let _ = tx.rollback().await;
    assert!(
        matches!(result, Err(UserError::Repository(_))),
        "create must surface the failed outbox insert, got {result:?}"
    );

    let user_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM users WHERE id = $1")
        .bind(user.id)
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(
        user_rows, 0,
        "the users row must not commit when its outbox row cannot"
    );

    let outbox_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM outbox_events WHERE aggregate_id = $1")
            .bind(user.id)
            .fetch_one(&pool)
            .await
            .expect("count");
    assert_eq!(outbox_rows, 0, "no partial outbox row either");
}

#[tokio::test]
async fn concurrent_registrations_for_the_same_email_commit_exactly_one_user() {
    let pool = fresh_db().await;
    let register = Arc::new(RegisterUserUseCase::new(
        pool.clone(),
        repo(&pool),
        hasher(),
    ));

    let email = unique_email("race");
    let n = 8;

    let mut handles = Vec::new();
    for _ in 0..n {
        let register = register.clone();
        let email = email.clone();
        handles.push(tokio::spawn(async move {
            register.execute(email, "shared-password".to_string()).await
        }));
    }

    let mut succeeded = 0;
    for h in handles {
        if h.await.expect("task join").is_ok() {
            succeeded += 1;
        }
    }
    assert_eq!(
        succeeded, 1,
        "exactly one concurrent registration for an email may win - never {n}, never 2"
    );

    let users: i64 = sqlx::query_scalar("SELECT count(*) FROM users WHERE email = $1")
        .bind(&email)
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(users, 1, "unique email held under concurrency");

    let outbox: i64 = count(&pool, "SELECT count(*) FROM outbox_events").await;
    assert_eq!(
        outbox, 0,
        "the winner deleted its own outbox row in-transaction; the losers rolled back - \
         either way nothing lingers"
    );
}

#[tokio::test]
async fn login_with_correct_credentials_issues_a_jwt_for_that_user() {
    let pool = fresh_db().await;
    let h = hasher();
    let register = RegisterUserUseCase::new(pool.clone(), repo(&pool), h.clone());
    let login = LoginUserUseCase::new(pool.clone(), repo(&pool), h, issuer());

    let email = unique_email("login-ok");
    let password = "right-password";
    let user = register
        .execute(email.clone(), password.to_string())
        .await
        .expect("register");

    let token = login
        .execute(email.clone(), password.to_string())
        .await
        .expect("login should succeed");

    #[derive(serde::Deserialize)]
    struct Claims {
        sub: String,
        email: String,
        exp: usize,
    }
    let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
    validation.set_issuer(&["user-service"]);
    let decoded = jsonwebtoken::decode::<Claims>(
        &token,
        &jsonwebtoken::DecodingKey::from_secret(JWT_SECRET.as_bytes()),
        &validation,
    )
    .expect("token must verify with the issuing secret");

    assert_eq!(
        decoded.claims.sub,
        user.id.to_string(),
        "token subject is the user id"
    );
    assert_eq!(decoded.claims.email, email);
    assert!(
        decoded.claims.exp > Utc::now().timestamp() as usize,
        "exp claim is in the future"
    );
}

#[tokio::test]
async fn login_with_wrong_password_is_rejected_as_invalid_credentials() {
    let pool = fresh_db().await;
    let h = hasher();
    let register = RegisterUserUseCase::new(pool.clone(), repo(&pool), h.clone());
    let login = LoginUserUseCase::new(pool.clone(), repo(&pool), h, issuer());

    let email = unique_email("login-wrong");
    register
        .execute(email.clone(), "the-real-password".to_string())
        .await
        .expect("register");

    let err = login
        .execute(email, "not-the-password".to_string())
        .await
        .expect_err("wrong password must not authenticate");
    assert!(matches!(err, UserError::InvalidCredentials), "got {err:?}");
}

#[tokio::test]
async fn login_failures_are_indistinguishable_between_wrong_password_and_unknown_email() {
    let pool = fresh_db().await;
    let h = hasher();
    let register = RegisterUserUseCase::new(pool.clone(), repo(&pool), h.clone());
    let login = LoginUserUseCase::new(pool.clone(), repo(&pool), h, issuer());

    let email = unique_email("login-enum");
    register
        .execute(email.clone(), "the-real-password".to_string())
        .await
        .expect("register");

    let wrong_password = login
        .execute(email, "not-the-password".to_string())
        .await
        .unwrap_err();
    let unknown_email = login
        .execute(unique_email("ghost"), "whatever".to_string())
        .await
        .unwrap_err();

    assert!(
        matches!(wrong_password, UserError::InvalidCredentials),
        "got {wrong_password:?}"
    );
    assert!(
        matches!(unknown_email, UserError::InvalidCredentials),
        "got {unknown_email:?}"
    );
    assert_eq!(
        wrong_password.to_string(),
        unknown_email.to_string(),
        "the two failure modes must not be distinguishable to a caller (no user enumeration)"
    );
    assert_eq!(wrong_password.to_string(), "invalid email or password");
}

#[tokio::test]
async fn get_user_profile_returns_the_stored_profile_for_a_known_id() {
    let pool = fresh_db().await;
    let register = RegisterUserUseCase::new(pool.clone(), repo(&pool), hasher());
    let profile = GetUserProfileUseCase::new(pool.clone(), repo(&pool));

    let email = unique_email("profile");
    let created = register
        .execute(email.clone(), "pw".to_string())
        .await
        .expect("register");

    let got = profile
        .execute(created.id)
        .await
        .expect("profile for a known id");

    assert_eq!(got.id, created.id);
    assert_eq!(got.email, email);
    assert!(got.created_at <= Utc::now());
}

#[tokio::test]
async fn get_user_profile_maps_an_unknown_id_to_not_found() {
    let pool = fresh_db().await;
    let profile = GetUserProfileUseCase::new(pool.clone(), repo(&pool));

    let err = profile
        .execute(Uuid::new_v4())
        .await
        .expect_err("an unknown id must not resolve");
    assert!(matches!(err, UserError::NotFound), "got {err:?}");
}

#[tokio::test]
async fn get_user_profile_performs_no_ownership_check_idor_gap() {
    let pool = fresh_db().await;
    let register = RegisterUserUseCase::new(pool.clone(), repo(&pool), hasher());
    let profile = GetUserProfileUseCase::new(pool.clone(), repo(&pool));

    let victim = register
        .execute(unique_email("victim"), "pw".to_string())
        .await
        .expect("register victim");
    let _attacker = register
        .execute(unique_email("attacker"), "pw".to_string())
        .await
        .expect("register attacker");

    let leaked = profile
        .execute(victim.id)
        .await
        .expect("no ownership check exists to stop this");
    assert_eq!(leaked.email, victim.email);
}
