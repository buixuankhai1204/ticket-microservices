# user-service

Rust (axum) service on port 8081: register, login, profile, subscriptions and the renewal jobs.

## Testing

| Tier | Where | Real | Faked | Docker |
|---|---|---|---|---|
| Unit | `src/**` (`#[cfg(test)]`) | domain and use cases | repositories, hasher, token issuer, gateways (`mockall`) and the transaction | no |
| Integration, repositories | `tests/integration_repository.rs` | the Postgres repositories (Testcontainers) | nothing | yes |
| Integration, gateways | `tests/integration_gateways.rs` | `HttpPaymentGateway`, `HttpEmailGateway` | the providers (in-process `wiremock`) | no |
| Component, in-process | `tests/component_in_process.rs` | `app::App` over real HTTP and Postgres | payment and email providers | yes |
| Component, out-of-process | `tests/component_out_of_process.rs` | the compiled binary, real Postgres | payment and email providers (`wiremock`) | yes |
| Contract | `tests/contract_consumer.rs` (writes `pacts/`) | the HTTP gateways | the providers, as Pact | no |
| End-to-end | `tests/e2e.rs` | a running stack through Kong | nothing | stack |

```bash
cargo test                                      # unit, gateways, contract: no Docker
cargo test --test integration_repository -- --ignored
cargo test --test component_in_process -- --ignored
cargo test --test component_out_of_process -- --ignored
E2E_BASE_URL=http://localhost:8000 cargo test --test e2e -- --ignored
```

Set `TEST_DATABASE_URL` to reuse a Postgres instead of starting a container. Each test gets its
own throwaway database. Outbox rows are read back through an `outbox_tap` trigger table that the
test helpers install.
