use async_trait::async_trait;
use sqlx::{PgConnection, PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::domain::{Booking, BookingError, DomainEvent, Pagination};

#[async_trait]
pub trait TxBackend: Send {
    fn conn(&mut self) -> &mut PgConnection;
    async fn commit(self: Box<Self>) -> Result<(), BookingError>;
}

pub struct Tx(Box<dyn TxBackend>);

impl Tx {
    pub fn new(backend: Box<dyn TxBackend>) -> Self {
        Self(backend)
    }

    pub fn conn(&mut self) -> &mut PgConnection {
        self.0.conn()
    }

    pub async fn commit(self) -> Result<(), BookingError> {
        self.0.commit().await
    }
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait Transactor: Send + Sync {
    async fn begin(&self) -> Result<Tx, BookingError>;
    async fn begin_read_only(&self) -> Result<Tx, BookingError>;
}

pub fn database_error(e: sqlx::Error) -> BookingError {
    let sqlstate = e
        .as_database_error()
        .and_then(|d| d.code())
        .map(|c| c.into_owned());
    BookingError::Repository {
        message: e.to_string(),
        sqlstate,
    }
}

pub struct PgTransactor(PgPool);

impl PgTransactor {
    pub fn new(pool: PgPool) -> Self {
        Self(pool)
    }
}

struct PgTx(Transaction<'static, Postgres>);

#[async_trait]
impl TxBackend for PgTx {
    fn conn(&mut self) -> &mut PgConnection {
        &mut self.0
    }

    async fn commit(self: Box<Self>) -> Result<(), BookingError> {
        self.0.commit().await.map_err(database_error)
    }
}

#[async_trait]
impl Transactor for PgTransactor {
    async fn begin(&self) -> Result<Tx, BookingError> {
        let tx = self.0.begin().await.map_err(database_error)?;
        Ok(Tx::new(Box::new(PgTx(tx))))
    }

    async fn begin_read_only(&self) -> Result<Tx, BookingError> {
        let mut tx = self.0.begin().await.map_err(database_error)?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(database_error)?;
        Ok(Tx::new(Box::new(PgTx(tx))))
    }
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait BookingRepository: Send + Sync {
    async fn find_by_id_for_user(
        &self,
        tx: &mut Tx,
        id: Uuid,
        user_id: Uuid,
    ) -> Result<Booking, BookingError>;
    async fn find_for_update(&self, tx: &mut Tx, id: Uuid) -> Result<Booking, BookingError>;
    async fn claim_oldest_stale_pending(
        &self,
        tx: &mut Tx,
        older_than_secs: i64,
    ) -> Result<Option<Booking>, BookingError>;
    async fn count_oversold_seats(&self, tx: &mut Tx) -> Result<i64, BookingError>;
    async fn count_stuck_pending(
        &self,
        tx: &mut Tx,
        older_than_secs: i64,
    ) -> Result<i64, BookingError>;
    async fn list_for_user(
        &self,
        tx: &mut Tx,
        user_id: Uuid,
        pagination: Pagination,
    ) -> Result<(Vec<Booking>, i64), BookingError>;
    async fn create(&self, tx: &mut Tx, booking: &Booking) -> Result<(), BookingError>;
    async fn update_status(&self, tx: &mut Tx, booking: &Booking) -> Result<(), BookingError>;
    async fn mark_processed(&self, tx: &mut Tx, event_id: Uuid) -> Result<bool, BookingError>;
    async fn write_outbox(&self, tx: &mut Tx, event: &DomainEvent) -> Result<(), BookingError>;
}

#[cfg(test)]
pub mod testing {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Clone, Default)]
    pub struct Journal(Arc<Mutex<Vec<String>>>);

    impl Journal {
        pub fn note(&self, step: &str) {
            self.0.lock().unwrap().push(step.to_string());
        }

        pub fn steps(&self) -> Vec<String> {
            self.0.lock().unwrap().clone()
        }

        pub fn committed(&self) -> bool {
            self.steps().iter().any(|s| s == "commit")
        }
    }

    struct FakeBackend {
        journal: Journal,
    }

    #[async_trait]
    impl TxBackend for FakeBackend {
        fn conn(&mut self) -> &mut PgConnection {
            unreachable!("mocked repositories never touch the connection")
        }

        async fn commit(self: Box<Self>) -> Result<(), BookingError> {
            self.journal.note("commit");
            Ok(())
        }
    }

    fn fake_tx(journal: &Journal) -> Tx {
        Tx::new(Box::new(FakeBackend {
            journal: journal.clone(),
        }))
    }

    pub fn transactor(journal: &Journal) -> MockTransactor {
        let mut transactor = MockTransactor::new();
        let begin_journal = journal.clone();
        transactor.expect_begin().returning(move || {
            begin_journal.note("begin");
            Ok(fake_tx(&begin_journal))
        });
        let read_journal = journal.clone();
        transactor.expect_begin_read_only().returning(move || {
            read_journal.note("begin_read_only");
            Ok(fake_tx(&read_journal))
        });
        transactor
    }
}
