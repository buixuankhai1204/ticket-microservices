mod common;

use chrono::{Duration, NaiveDate, Utc};
use common::{new_database, published_events, TestDb};
use user_service::adapter::repository::postgres::PostgresUserRepository;
use user_service::adapter::repository::renewal_attempt_postgres::PostgresRenewalAttemptRepository;
use user_service::adapter::repository::subscription_postgres::PostgresSubscriptionRepository;
use user_service::domain::{
    BillingInterval, DomainEvent, Pagination, RenewalAttempt, RenewalAttemptStatus, Subscription,
    SubscriptionStatus, User, UserCreated, UserError, UserLoggedIn,
};
use user_service::platform::port::{
    PgTransactor, RenewalAttemptRepository, SubscriptionRepository, Transactor, UserRepository,
};
use uuid::Uuid;

struct Fixture {
    db: TestDb,
    transactor: PgTransactor,
    users: PostgresUserRepository,
    subscriptions: PostgresSubscriptionRepository,
    attempts: PostgresRenewalAttemptRepository,
}

async fn fixture() -> Fixture {
    let db = new_database().await;
    Fixture {
        transactor: PgTransactor::new(db.pool.clone()),
        users: PostgresUserRepository::new(),
        subscriptions: PostgresSubscriptionRepository::new(),
        attempts: PostgresRenewalAttemptRepository::new(),
        db,
    }
}

fn date(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day).unwrap()
}

impl Fixture {
    async fn user(&self, email: &str) -> User {
        let user = User::new(email.to_string(), "phc-hash".to_string()).unwrap();
        let mut tx = self.transactor.begin().await.unwrap();
        self.users.create(&mut tx, &user).await.unwrap();
        tx.commit().await.unwrap();
        user
    }

    async fn subscription(
        &self,
        user_id: Uuid,
        status: SubscriptionStatus,
        current_period_end: NaiveDate,
    ) -> Subscription {
        let now = Utc::now();
        let subscription = Subscription {
            id: Uuid::new_v4(),
            user_id,
            plan_id: "pro".to_string(),
            status,
            current_period_end,
            billing_interval: BillingInterval::Month,
            price_minor: 1999,
            currency: "USD".to_string(),
            payment_method_id: "pm_1".to_string(),
            created_at: now,
            updated_at: now,
        };
        let mut tx = self.transactor.begin().await.unwrap();
        self.subscriptions
            .create(&mut tx, &subscription)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        subscription
    }

    async fn attempts_of(&self, subscription_id: Uuid) -> Vec<RenewalAttempt> {
        let periods = sqlx::query_scalar::<_, NaiveDate>(
            "SELECT period_end FROM renewal_attempts WHERE subscription_id = $1 ORDER BY period_end",
        )
        .bind(subscription_id)
        .fetch_all(&self.db.pool)
        .await
        .unwrap();
        let mut tx = self.transactor.begin().await.unwrap();
        let mut found = Vec::new();
        for period_end in periods {
            found.push(
                self.attempts
                    .find_for_period_for_update(&mut tx, subscription_id, period_end)
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        tx.rollback().await.unwrap();
        found
    }

    async fn attempt(
        &self,
        subscription: &Subscription,
        status: RenewalAttemptStatus,
        next_attempt_at: chrono::DateTime<Utc>,
    ) -> RenewalAttempt {
        let mut attempt =
            RenewalAttempt::queued_now(subscription.id, subscription.current_period_end);
        attempt.status = status;
        attempt.next_attempt_at = next_attempt_at;
        let mut tx = self.transactor.begin().await.unwrap();
        self.attempts.create(&mut tx, &attempt).await.unwrap();
        tx.commit().await.unwrap();
        attempt
    }
}

#[tokio::test]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn a_created_user_is_found_by_id_and_email_and_its_events_reach_the_outbox() {
    let fx = fixture().await;
    let mut ada = User::new("ada@example.com".to_string(), "phc-hash".to_string()).unwrap();
    ada.record_event(DomainEvent::UserCreated(UserCreated::new(
        ada.id,
        ada.email.clone(),
        ada.created_at,
    )));

    let mut tx = fx.transactor.begin().await.unwrap();
    fx.users.create(&mut tx, &ada).await.unwrap();
    let logged_in =
        DomainEvent::UserLoggedIn(UserLoggedIn::new(ada.id, ada.email.clone(), Utc::now()));
    fx.users.write_outbox(&mut tx, &logged_in).await.unwrap();
    tx.commit().await.unwrap();

    let mut tx = fx.transactor.begin_read_only().await.unwrap();
    let by_id = fx.users.find_by_id(&mut tx, ada.id).await.unwrap();
    let by_email = fx
        .users
        .find_by_email(&mut tx, "ada@example.com")
        .await
        .unwrap();
    assert_eq!(by_id.email, "ada@example.com");
    assert_eq!(by_id.password_hash, "phc-hash");
    assert_eq!(by_email.map(|u| u.id), Some(ada.id));
    assert!(fx
        .users
        .find_by_email(&mut tx, "nobody@example.com")
        .await
        .unwrap()
        .is_none());
    assert!(matches!(
        fx.users.find_by_id(&mut tx, Uuid::new_v4()).await,
        Err(UserError::NotFound)
    ));
    tx.commit().await.unwrap();

    let events = published_events(&fx.db.pool).await;
    let types: Vec<_> = events.iter().map(|e| e.event_type.as_str()).collect();
    assert_eq!(types, ["UserCreated", "UserLoggedIn"]);
    assert!(events
        .iter()
        .all(|e| e.aggregate_type == "user" && e.aggregate_id == ada.id));
    assert_eq!(events[0].payload["email"], "ada@example.com");
    assert_eq!(events[0].payload["user_id"], ada.id.to_string());
    assert_eq!(events[1].payload["event_id"], events[1].id.to_string());
}

#[tokio::test]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn subscriptions_round_trip_and_are_only_visible_to_their_owner() {
    let fx = fixture().await;
    let alice = fx.user("alice@example.com").await;
    let bob = fx.user("bob@example.com").await;
    let first = fx
        .subscription(alice.id, SubscriptionStatus::Active, date(2031, 3, 31))
        .await;
    let second = fx
        .subscription(alice.id, SubscriptionStatus::PastDue, date(2031, 4, 30))
        .await;

    let mut tx = fx.transactor.begin_read_only().await.unwrap();
    let loaded = fx
        .subscriptions
        .find_by_id_for_user(&mut tx, first.id, alice.id)
        .await
        .unwrap();
    assert_eq!(loaded.plan_id, "pro");
    assert_eq!(loaded.status, SubscriptionStatus::Active);
    assert_eq!(loaded.current_period_end, date(2031, 3, 31));
    assert_eq!(loaded.billing_interval, BillingInterval::Month);
    assert_eq!(
        (loaded.price_minor, loaded.currency.as_str()),
        (1999, "USD")
    );
    assert_eq!(loaded.payment_method_id, "pm_1");
    assert!(matches!(
        fx.subscriptions
            .find_by_id_for_user(&mut tx, first.id, bob.id)
            .await,
        Err(UserError::NotFound)
    ));
    assert_eq!(
        fx.subscriptions
            .find_by_id(&mut tx, first.id)
            .await
            .unwrap()
            .user_id,
        alice.id
    );

    let (page, total) = fx
        .subscriptions
        .list_for_user(&mut tx, alice.id, Pagination::new(1, 0).unwrap())
        .await
        .unwrap();
    assert_eq!((page.len(), total), (1, 2));
    assert_eq!(page[0].id, second.id);
    let (none, total) = fx
        .subscriptions
        .list_for_user(&mut tx, bob.id, Pagination::new(10, 0).unwrap())
        .await
        .unwrap();
    assert_eq!((none.len(), total), (0, 0));
}

#[tokio::test]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn enqueueing_creates_one_attempt_per_due_active_subscription_and_never_twice() {
    let fx = fixture().await;
    let owner = fx.user("owner@example.com").await;
    let yesterday = Utc::now().date_naive() - Duration::days(1);
    let today = Utc::now().date_naive();
    let tomorrow = today + Duration::days(1);
    let overdue = fx
        .subscription(owner.id, SubscriptionStatus::Active, yesterday)
        .await;
    let due_today = fx
        .subscription(owner.id, SubscriptionStatus::Active, today)
        .await;
    let future = fx
        .subscription(owner.id, SubscriptionStatus::Active, tomorrow)
        .await;
    let past_due = fx
        .subscription(owner.id, SubscriptionStatus::PastDue, yesterday)
        .await;
    let canceled = fx
        .subscription(owner.id, SubscriptionStatus::Canceled, yesterday)
        .await;

    let mut tx = fx.transactor.begin().await.unwrap();
    let first = fx.attempts.enqueue_due(&mut tx).await.unwrap();
    let second = fx.attempts.enqueue_due(&mut tx).await.unwrap();
    tx.commit().await.unwrap();

    assert_eq!((first, second), (2, 0));
    let enqueued = fx.attempts_of(overdue.id).await;
    assert_eq!(enqueued.len(), 1);
    assert_eq!(enqueued[0].status, RenewalAttemptStatus::Pending);
    assert_eq!(enqueued[0].period_end, yesterday);
    assert_eq!(
        enqueued[0].idempotency_key,
        RenewalAttempt::idempotency_key_for(overdue.id, yesterday)
    );
    assert_eq!(fx.attempts_of(due_today.id).await.len(), 1);
    for skipped in [future.id, past_due.id, canceled.id] {
        assert!(fx.attempts_of(skipped).await.is_empty());
    }
}

#[tokio::test]
#[ignore = "requires Docker (or TEST_DATABASE_URL)"]
async fn claiming_takes_the_oldest_due_claimable_row_and_concurrent_claims_never_collide() {
    let fx = fixture().await;
    let owner = fx.user("owner@example.com").await;
    let now = Utc::now();
    let mut claimable = Vec::new();
    for (offset_minutes, status) in [
        (30, RenewalAttemptStatus::FailedRetryable),
        (50, RenewalAttemptStatus::Pending),
        (10, RenewalAttemptStatus::FailedPermanent),
    ] {
        let subscription = fx
            .subscription(owner.id, SubscriptionStatus::Active, date(2031, 3, 31))
            .await;
        let attempt = fx
            .attempt(
                &subscription,
                status,
                now - Duration::minutes(offset_minutes),
            )
            .await;
        claimable.push(attempt);
    }
    for (status, offset) in [
        (RenewalAttemptStatus::Succeeded, -90),
        (RenewalAttemptStatus::GivenUp, -90),
        (RenewalAttemptStatus::Charging, -90),
        (RenewalAttemptStatus::Pending, 90),
    ] {
        let subscription = fx
            .subscription(owner.id, SubscriptionStatus::Active, date(2031, 3, 31))
            .await;
        fx.attempt(&subscription, status, now + Duration::minutes(offset))
            .await;
    }
    let canceled = fx
        .subscription(owner.id, SubscriptionStatus::Canceled, date(2031, 3, 31))
        .await;
    fx.attempt(
        &canceled,
        RenewalAttemptStatus::Pending,
        now - Duration::minutes(120),
    )
    .await;

    let mut first_worker = fx.transactor.begin().await.unwrap();
    let mut second_worker = fx.transactor.begin().await.unwrap();
    let mut third_worker = fx.transactor.begin().await.unwrap();
    let a = fx
        .attempts
        .claim_one_due(&mut first_worker)
        .await
        .unwrap()
        .unwrap();
    let b = fx
        .attempts
        .claim_one_due(&mut second_worker)
        .await
        .unwrap()
        .unwrap();
    let c = fx
        .attempts
        .claim_one_due(&mut third_worker)
        .await
        .unwrap()
        .unwrap();
    let mut fourth_worker = fx.transactor.begin().await.unwrap();
    let none = fx.attempts.claim_one_due(&mut fourth_worker).await.unwrap();

    assert_eq!(
        [a.id, b.id, c.id],
        [claimable[1].id, claimable[0].id, claimable[2].id]
    );
    assert!(none.is_none());
}
