use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgConnection, PgPool};
use uuid::Uuid;

fn admin_url() -> String {
    std::env::var("COMPONENT_PG_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5440/postgres".to_string())
}

pub struct TestDb {
    pub pool: PgPool,
    name: String,
}

pub async fn new_database() -> TestDb {
    let admin = admin_url();
    let mut conn = PgConnection::connect(&admin).await.expect(
        "connect to the component-test postgres (docker compose --profile component-test up -d postgres-test)",
    );
    let name = format!("comp_{}", &Uuid::new_v4().simple().to_string()[..8]);
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&mut conn)
        .await
        .expect("create database");

    let base = admin.rsplit_once('/').map(|(b, _)| b).unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&format!("{base}/{name}"))
        .await
        .expect("connect to the test database");
    user_service::app::migrate(&pool)
        .await
        .expect("run migrations");
    TestDb { pool, name }
}

impl TestDb {
    pub async fn drop_database(self) {
        self.pool.close().await;
        if let Ok(mut conn) = PgConnection::connect(&admin_url()).await {
            let _ = sqlx::query(&format!(
                "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                self.name
            ))
            .execute(&mut conn)
            .await;
        }
    }
}
