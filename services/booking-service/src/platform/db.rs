use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

pub type DbPool = PgPool;

pub async fn build_pool(database_url: &str, max_connections: u32) -> Result<DbPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(database_url)
        .await
}
