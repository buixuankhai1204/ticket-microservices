use std::fmt;

use async_trait::async_trait;
use chrono::{Duration, NaiveDate};
use sqlx::pool::PoolConnection;
use sqlx::{PgConnection, PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::domain::{
    DomainEvent, Pagination, RenewalAttempt, Subscription, SubscriptionStatus, User, UserError,
};

#[async_trait]
pub trait TxBackend: Send {
    fn conn(&mut self) -> &mut PgConnection;
    async fn commit(self: Box<Self>) -> Result<(), UserError>;
    async fn rollback(self: Box<Self>) -> Result<(), UserError>;
}

pub struct Tx(Box<dyn TxBackend>);

impl Tx {
    pub fn new(backend: Box<dyn TxBackend>) -> Self {
        Self(backend)
    }

    pub fn conn(&mut self) -> &mut PgConnection {
        self.0.conn()
    }

    pub async fn commit(self) -> Result<(), UserError> {
        self.0.commit().await
    }

    pub async fn rollback(self) -> Result<(), UserError> {
        self.0.rollback().await
    }
}

impl fmt::Debug for Tx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tx").finish_non_exhaustive()
    }
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait Transactor: Send + Sync {
    async fn begin(&self) -> Result<Tx, UserError>;
    async fn begin_read_only(&self) -> Result<Tx, UserError>;
    async fn acquire(&self) -> Result<Tx, UserError>;
}

fn repo_err(e: sqlx::Error) -> UserError {
    UserError::Repository(e.to_string())
}

pub struct PgTransactor(PgPool);

impl PgTransactor {
    pub fn new(pool: PgPool) -> Self {
        Self(pool)
    }
}

struct PgTxBackend(Transaction<'static, Postgres>);

#[async_trait]
impl TxBackend for PgTxBackend {
    fn conn(&mut self) -> &mut PgConnection {
        &mut self.0
    }

    async fn commit(self: Box<Self>) -> Result<(), UserError> {
        self.0.commit().await.map_err(repo_err)
    }

    async fn rollback(self: Box<Self>) -> Result<(), UserError> {
        self.0.rollback().await.map_err(repo_err)
    }
}

struct PgConnectionBackend(PoolConnection<Postgres>);

#[async_trait]
impl TxBackend for PgConnectionBackend {
    fn conn(&mut self) -> &mut PgConnection {
        &mut self.0
    }

    async fn commit(self: Box<Self>) -> Result<(), UserError> {
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<(), UserError> {
        Ok(())
    }
}

#[async_trait]
impl Transactor for PgTransactor {
    async fn begin(&self) -> Result<Tx, UserError> {
        let tx = self.0.begin().await.map_err(repo_err)?;
        Ok(Tx::new(Box::new(PgTxBackend(tx))))
    }

    async fn begin_read_only(&self) -> Result<Tx, UserError> {
        let mut tx = self.0.begin().await.map_err(repo_err)?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(repo_err)?;
        Ok(Tx::new(Box::new(PgTxBackend(tx))))
    }

    async fn acquire(&self) -> Result<Tx, UserError> {
        let conn = self.0.acquire().await.map_err(repo_err)?;
        Ok(Tx::new(Box::new(PgConnectionBackend(conn))))
    }
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait UserRepository: Send + Sync {
    async fn find_by_id(&self, tx: &mut Tx, id: Uuid) -> Result<User, UserError>;
    async fn find_by_email(&self, tx: &mut Tx, email: &str) -> Result<Option<User>, UserError>;
    async fn create(&self, tx: &mut Tx, user: &User) -> Result<(), UserError>;
    async fn write_outbox(&self, tx: &mut Tx, event: &DomainEvent) -> Result<(), UserError>;
    async fn list(
        &self,
        tx: &mut Tx,
        pagination: Pagination,
    ) -> Result<(Vec<User>, i64), UserError>;
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait SubscriptionRepository: Send + Sync {
    async fn create(&self, tx: &mut Tx, subscription: &Subscription) -> Result<(), UserError>;

    /// Owner-scoped fetch: a row whose `user_id` is not `user_id` is reported as
    /// `UserError::NotFound`, indistinguishable from a missing row (no IDOR
    /// existence leak).
    async fn find_by_id_for_user(
        &self,
        tx: &mut Tx,
        id: Uuid,
        user_id: Uuid,
    ) -> Result<Subscription, UserError>;

    /// Owner-scoped page plus the full match count for that owner, both read on
    /// the one connection the use case passes in.
    async fn list_for_user(
        &self,
        tx: &mut Tx,
        user_id: Uuid,
        pagination: Pagination,
    ) -> Result<(Vec<Subscription>, i64), UserError>;

    /// Unscoped fetch by id — for the system-context renewal Job B, which acts
    /// on behalf of no user.
    async fn find_by_id(&self, tx: &mut Tx, id: Uuid) -> Result<Subscription, UserError>;

    /// Renewal succeeded: advance `current_period_end` and set status `active`.
    async fn renew_period(
        &self,
        tx: &mut Tx,
        id: Uuid,
        new_period_end: NaiveDate,
    ) -> Result<(), UserError>;

    async fn set_status(
        &self,
        tx: &mut Tx,
        id: Uuid,
        status: SubscriptionStatus,
    ) -> Result<(), UserError>;

    /// Write a saga event into `outbox_events` (INSERT only, matching the
    /// existing user-service behaviour). Debezium tails the WAL insert.
    async fn write_outbox(&self, tx: &mut Tx, event: &DomainEvent) -> Result<(), UserError>;
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait RenewalAttemptRepository: Send + Sync {
    /// The attempt row for a `(subscription, period)`, locked `FOR UPDATE` so a
    /// concurrent Job B tick (`FOR UPDATE SKIP LOCKED`) cannot flip its status
    /// between this read and the caller's write.
    async fn find_for_period_for_update(
        &self,
        tx: &mut Tx,
        subscription_id: Uuid,
        period_end: NaiveDate,
    ) -> Result<Option<RenewalAttempt>, UserError>;

    async fn create(&self, tx: &mut Tx, attempt: &RenewalAttempt) -> Result<(), UserError>;

    /// Persist `status` / `next_attempt_at` / `updated_at` for an existing row.
    async fn update_schedule(&self, tx: &mut Tx, attempt: &RenewalAttempt)
        -> Result<(), UserError>;

    // ---- Job B (docs/sagas/renewal-subscriptions.md §2) --------------------

    /// TX1: claim the single oldest due attempt whose subscription is still
    /// `active`, `FOR UPDATE SKIP LOCKED` so parallel Job B workers never grab
    /// the same row. `None` = nothing due.
    async fn claim_one_due(&self, tx: &mut Tx) -> Result<Option<RenewalAttempt>, UserError>;

    /// TX1: persist the `charging` transition (`status`, `attempt_count`,
    /// `updated_at`).
    async fn mark_charging(&self, tx: &mut Tx, attempt: &RenewalAttempt) -> Result<(), UserError>;

    /// TX2: re-read the row `FOR UPDATE` to confirm it is still `charging`
    /// before recording the provider outcome.
    async fn find_by_id_for_update(
        &self,
        tx: &mut Tx,
        id: Uuid,
    ) -> Result<Option<RenewalAttempt>, UserError>;

    /// TX2: persist every mutable field of the settled attempt.
    async fn settle(&self, tx: &mut Tx, attempt: &RenewalAttempt) -> Result<(), UserError>;

    /// Reaper (§7): reset rows wedged in `charging` past `stale_after` back to
    /// `failed_retryable, next_attempt_at=now()` — safe because the provider
    /// idempotency key makes any re-charge a no-op. Returns the count reaped.
    async fn reap_stale_charging(
        &self,
        tx: &mut Tx,
        stale_after: Duration,
    ) -> Result<u64, UserError>;

    // ---- dunning-email ledger (step 4) ------------------------------------

    async fn dunning_email_recorded(&self, tx: &mut Tx, event_id: Uuid) -> Result<bool, UserError>;

    async fn record_dunning_email(
        &self,
        tx: &mut Tx,
        event_id: Uuid,
        user_id: Uuid,
        template: &str,
    ) -> Result<(), UserError>;

    // ---- Job A (docs/sagas/renewal-subscriptions.md §1/§11) ----------------

    async fn try_advisory_lock(&self, tx: &mut Tx, key: &str) -> Result<bool, UserError>;

    async fn release_advisory_lock(&self, tx: &mut Tx, key: &str) -> Result<(), UserError>;

    async fn enqueue_due(&self, tx: &mut Tx) -> Result<u64, UserError>;
}

#[cfg(test)]
pub mod testing {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use sqlx::PgConnection;

    use super::{MockTransactor, Transactor, Tx, TxBackend};
    use crate::domain::UserError;

    #[derive(Clone, Default)]
    pub struct CallLog(Arc<Mutex<Vec<String>>>);

    impl CallLog {
        pub fn note(&self, call: &str) {
            self.0.lock().unwrap().push(call.to_string());
        }

        pub fn calls(&self) -> Vec<String> {
            self.0.lock().unwrap().clone()
        }

        pub fn committed(&self) -> bool {
            self.calls().iter().any(|c| c == "commit")
        }
    }

    #[derive(Clone)]
    pub struct Captured<T>(Arc<Mutex<Vec<T>>>);

    impl<T> Default for Captured<T> {
        fn default() -> Self {
            Self(Arc::new(Mutex::new(Vec::new())))
        }
    }

    impl<T: Clone> Captured<T> {
        pub fn push(&self, value: T) {
            self.0.lock().unwrap().push(value);
        }

        pub fn all(&self) -> Vec<T> {
            self.0.lock().unwrap().clone()
        }

        pub fn only(&self) -> T {
            let all = self.all();
            assert_eq!(all.len(), 1, "expected exactly one captured value");
            all[0].clone()
        }
    }

    struct FakeBackend {
        log: CallLog,
        fail_commit: bool,
    }

    #[async_trait]
    impl TxBackend for FakeBackend {
        fn conn(&mut self) -> &mut PgConnection {
            unreachable!("mocked repositories never touch the connection")
        }

        async fn commit(self: Box<Self>) -> Result<(), UserError> {
            if self.fail_commit {
                self.log.note("commit_failed");
                return Err(UserError::Repository("commit failed".to_string()));
            }
            self.log.note("commit");
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<(), UserError> {
            self.log.note("rollback");
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    pub struct FakeDb {
        log: CallLog,
        fail_begin: bool,
        fail_commit: bool,
    }

    impl FakeDb {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn failing_begin(mut self) -> Self {
            self.fail_begin = true;
            self
        }

        pub fn failing_commit(mut self) -> Self {
            self.fail_commit = true;
            self
        }

        pub fn log(&self) -> CallLog {
            self.log.clone()
        }

        pub fn transactor(&self) -> Arc<dyn Transactor> {
            let mut db = MockTransactor::new();
            let fake = self.clone();
            db.expect_begin().returning(move || fake.open("begin"));
            let fake = self.clone();
            db.expect_begin_read_only()
                .returning(move || fake.open("begin_read_only"));
            let fake = self.clone();
            db.expect_acquire().returning(move || fake.open("acquire"));
            Arc::new(db)
        }

        fn open(&self, name: &str) -> Result<Tx, UserError> {
            self.log.note(name);
            if self.fail_begin {
                return Err(UserError::Repository("pool exhausted".to_string()));
            }
            Ok(Tx::new(Box::new(FakeBackend {
                log: self.log.clone(),
                fail_commit: self.fail_commit,
            })))
        }
    }
}
