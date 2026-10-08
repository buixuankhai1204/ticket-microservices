mod common;

use common::{unique_email, Api, PASSWORD};
use reqwest::Method;
use serde_json::{json, Value};

fn gateway() -> Api {
    let base = std::env::var("E2E_BASE_URL").expect(
        "set E2E_BASE_URL to the Kong gateway of a running stack, e.g. http://localhost:8000",
    );
    Api::new(base)
}

#[tokio::test]
#[ignore = "needs a running stack: set E2E_BASE_URL to the Kong gateway"]
async fn a_customer_signs_up_logs_in_reads_their_profile_and_lists_users_through_the_gateway() {
    let api = gateway();
    let email = unique_email();

    let (status, registered) = api.register(&email, PASSWORD).await;
    assert_eq!(status, 201, "register: {registered}");
    assert_eq!(registered["email"], email.as_str());
    let id = registered["id"].as_str().expect("register returns an id");

    let (duplicate, _) = api.register(&email, PASSWORD).await;
    let (bad_email, _) = api.register("not-an-email", PASSWORD).await;
    assert_eq!((duplicate, bad_email), (400, 400));

    let (wrong_password, _) = api.login(&email, "not-the-password").await;
    assert_eq!(wrong_password, 401);
    let (status, session) = api.login(&email, PASSWORD).await;
    assert_eq!(status, 200, "login: {session}");
    let token = session["token"].as_str().expect("login returns a token");

    let profile_path = format!("/api/v1/users/{id}");
    let (anonymous, _) = api.get(&profile_path, None).await;
    let (garbage, _) = api.get(&profile_path, Some("not.a.jwt")).await;
    assert_eq!((anonymous, garbage), (401, 401));
    let (status, profile) = api.get(&profile_path, Some(token)).await;
    assert_eq!(status, 200, "profile: {profile}");
    assert_eq!(profile["id"], id);
    assert_eq!(profile["email"], email.as_str());

    let (anonymous_list, _) = api.get("/api/v1/users", None).await;
    assert_eq!(anonymous_list, 401);
    let (status, page) = api.get("/api/v1/users?limit=1&offset=0", Some(token)).await;
    assert_eq!(status, 200, "list: {page}");
    assert_eq!(page["data"].as_array().unwrap().len(), 1);
    assert_eq!(page["pagination"]["limit"], 1);
    assert_eq!(page["pagination"]["offset"], 0);
    assert!(page["pagination"]["total"].as_i64().unwrap() >= 1);
    let (bad_limit, _) = api.get("/api/v1/users?limit=0", Some(token)).await;
    assert_eq!(bad_limit, 400);
}

#[tokio::test]
#[ignore = "needs a running stack: set E2E_BASE_URL to the Kong gateway"]
async fn a_customer_subscribes_and_only_they_can_see_and_retry_the_subscription() {
    let api = gateway();
    let owner = api.signup().await;
    let stranger = api.signup().await;

    let (anonymous, _) = api
        .send(
            Method::POST,
            "/api/v1/subscriptions",
            None,
            Some(Api::subscription_request()),
        )
        .await;
    assert_eq!(anonymous, 401);

    let created = api.subscribe(&owner.token).await;
    assert_eq!(created["status"], "active");
    assert_eq!(created["user_id"], owner.id.to_string());
    assert_eq!(created["plan_id"], "pro");
    assert_eq!(created["price_minor"], 1999);
    let id = created["id"].as_str().unwrap();
    let path = format!("/api/v1/subscriptions/{id}");

    let (status, fetched) = api.get(&path, Some(&owner.token)).await;
    assert_eq!((status, fetched["id"].clone()), (200, json!(id)));
    let (hidden, _) = api.get(&path, Some(&stranger.token)).await;
    assert_eq!(hidden, 404);

    let (status, owned) = api.get("/api/v1/subscriptions", Some(&owner.token)).await;
    assert_eq!(status, 200);
    assert_eq!(owned["pagination"]["total"], 1);
    assert_eq!(owned["data"][0]["id"], id);
    let (_, others) = api
        .get("/api/v1/subscriptions", Some(&stranger.token))
        .await;
    assert_eq!(others["pagination"]["total"], 0);

    let mut invalid = Api::subscription_request();
    invalid["currency"] = Value::String("US".to_string());
    let (rejected, _) = api
        .post("/api/v1/subscriptions", Some(&owner.token), invalid)
        .await;
    assert_eq!(rejected, 400);

    let retry = format!("{path}/retry-renewal");
    let (queued, body) = api
        .send(Method::POST, &retry, Some(&owner.token), None)
        .await;
    assert_eq!(queued, 202, "retry: {body}");
    assert_eq!(body["subscription_id"], id);
    assert_eq!(body["status"], "failed_retryable");
    let (again, _) = api
        .send(Method::POST, &retry, Some(&owner.token), None)
        .await;
    let (foreign, _) = api
        .send(Method::POST, &retry, Some(&stranger.token), None)
        .await;
    assert_eq!((again, foreign), (202, 404));
}
