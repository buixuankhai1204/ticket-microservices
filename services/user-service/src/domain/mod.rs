pub mod entities;
pub mod errors;
pub mod events;
pub mod pagination;
pub mod ports;
pub mod renewal_attempt;
pub mod subscription;

pub use entities::User;
pub use errors::{EmailError, PaymentError, UserError};
pub use events::{
    DomainEvent, SubscriptionCanceled, SubscriptionPaymentFailed, SubscriptionRenewed, UserCreated,
    UserLoggedIn,
};
pub use pagination::{Pagination, DEFAULT_LIMIT};
pub use ports::{
    ChargeOutcome, ChargeRequest, DunningEmail, EmailGateway, PasswordHasher, PaymentGateway,
    TokenIssuer,
};
pub use renewal_attempt::{
    DunningOutcome, RenewalAttempt, RenewalAttemptStatus, RenewalPolicy, TransientOutcome,
};
pub use subscription::{BillingInterval, Subscription, SubscriptionStatus};
