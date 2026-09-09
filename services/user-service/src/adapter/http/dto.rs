use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::domain::{Pagination, Subscription, User};

#[derive(Debug, Deserialize, ToSchema)]
pub struct RegisterRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct LoginResponse {
    pub token: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct UserResponse {
    pub id: Uuid,
    pub email: String,
}

impl From<&User> for UserResponse {
    fn from(user: &User) -> Self {
        Self {
            id: user.id,
            email: user.email.clone(),
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateSubscriptionRequest {
    pub plan_id: String,
    /// `"month"` or `"year"`.
    pub billing_interval: String,
    /// Price per period in integer minor units (e.g. cents). Must be `>= 0`.
    pub price_minor: i64,
    /// ISO 4217 currency code, e.g. `"USD"`.
    pub currency: String,
    /// Opaque payment-provider token for the saved payment method.
    pub payment_method_id: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SubscriptionResponse {
    pub id: Uuid,
    pub user_id: Uuid,
    pub plan_id: String,
    pub status: String,
    /// ISO-8601 date (`YYYY-MM-DD`) the next renewal falls due.
    pub current_period_end: String,
    pub billing_interval: String,
    pub price_minor: i64,
    pub currency: String,
}

impl From<&Subscription> for SubscriptionResponse {
    fn from(subscription: &Subscription) -> Self {
        Self {
            id: subscription.id,
            user_id: subscription.user_id,
            plan_id: subscription.plan_id.clone(),
            status: subscription.status.as_str().to_string(),
            current_period_end: subscription.current_period_end.to_string(),
            billing_interval: subscription.billing_interval.as_str().to_string(),
            price_minor: subscription.price_minor,
            currency: subscription.currency.clone(),
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PaginationMeta {
    pub limit: i64,
    pub offset: i64,
    pub total: i64,
    pub has_more: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PaginatedUsersResponse {
    pub data: Vec<UserResponse>,
    pub pagination: PaginationMeta,
}

impl PaginatedUsersResponse {
    pub fn new(users: &[User], pagination: &Pagination, total: i64) -> Self {
        let data: Vec<UserResponse> = users.iter().map(UserResponse::from).collect();
        let has_more = pagination.has_more(data.len(), total);
        Self {
            data,
            pagination: PaginationMeta {
                limit: pagination.limit,
                offset: pagination.offset,
                total,
                has_more,
            },
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PaginatedSubscriptionsResponse {
    pub data: Vec<SubscriptionResponse>,
    pub pagination: PaginationMeta,
}

impl PaginatedSubscriptionsResponse {
    pub fn new(subscriptions: &[Subscription], pagination: &Pagination, total: i64) -> Self {
        let data: Vec<SubscriptionResponse> = subscriptions
            .iter()
            .map(SubscriptionResponse::from)
            .collect();
        let has_more = pagination.has_more(data.len(), total);
        Self {
            data,
            pagination: PaginationMeta {
                limit: pagination.limit,
                offset: pagination.offset,
                total,
                has_more,
            },
        }
    }
}
