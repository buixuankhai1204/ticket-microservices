# curl examples

Examples target the Kong API Gateway, not services directly. Set `$GATEWAY_URL` (default
`http://localhost:8000` for local docker-compose) before running these, and `$JWT_TOKEN` for
routes that require a bearer token (obtained from the `login` endpoint below).

## user-service

### Register a new user

Rate limit: 60 req/min (`auth-routes` Kong route, local policy). No JWT required.
Expected status: `201` on success, `400` if the email is already registered or invalid, `429`
if the rate limit is exceeded.

```bash
curl -i -X POST "http://localhost:8000/api/v1/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "jane1.doe@example.com",
    "password": "correct-horse-battery-staple"
  }'
```

### Log in

Rate limit: 60 req/min (`auth-routes` Kong route, local policy). No JWT required to call;
returns one on success.
Expected status: `200` with a JWT in the response body on success, `401` on invalid
credentials, `429` if the rate limit is exceeded.

```bash
curl -i -X POST "$GATEWAY_URL/api/v1/auth/login" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "jane.doe@example.com",
    "password": "correct-horse-battery-staple"
  }'
```

### Get a user's profile by ID

Rate limit: 100 req/min (`user-profile-routes` Kong route, local policy). Requires
`Authorization: Bearer $JWT_TOKEN` (Kong's `jwt` plugin verifies the `exp` claim).
Expected status: `200` on success, `401` if the token is missing/invalid, `404` if the user
does not exist, `429` if the rate limit is exceeded.

```bash
curl -i -X GET "$GATEWAY_URL/api/v1/users/00000000-0000-0000-0000-000000000000" \
  -H "Authorization: Bearer $JWT_TOKEN"
```

## product-service

Catalog reads are public (`product-catalog-routes`, 600 req/min). Writes go through
`product-admin-routes` (60 req/min): Kong verifies the JWT `exp`, and the service additionally
requires the token's `sub` to be listed in `PRODUCT_ADMIN_USER_IDS`. Errors use
`{ "error": "<message>" }`; lists use `{ "data": [...], "pagination": { "limit", "offset", "total", "has_more" } }`
(default `limit=20`, clamped to 100).

### List products

Optional query: `category` (`food|drink|snack|merchandise`), `status` (`active|inactive`),
`limit`, `offset`. Expected status: `200`, `400` on an invalid query value, `429` over the limit.

```bash
curl -i "$GATEWAY_URL/api/v1/products?category=snack&limit=20&offset=0"
```

### Get one product

Expected status: `200`, `400` if the id is not a UUID, `404` if it does not exist.

```bash
curl -i "$GATEWAY_URL/api/v1/products/00000000-0000-0000-0000-000000000000"
```

### Event menu (products with the event's price and availability)

Expected status: `200` (an empty `data` array when the event has no inventory).

```bash
curl -i "$GATEWAY_URL/api/v1/products/events/00000000-0000-0000-0000-000000000000/menu"
```

### Create a product (admin)

`price_minor` is an integer in minor units; `currency` defaults to the service's
`DEFAULT_CURRENCY`. Expected status: `201`, `400` on invalid input, `401` without a valid token,
`403` if the caller is not an admin, `409` if the `sku` already exists.

```bash
curl -i -X POST "$GATEWAY_URL/api/v1/products" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sku": "POPCORN-L", "name": "Large Popcorn", "category": "snack", "price_minor": 650}'
```

### Update a product (admin)

Any subset of `name`, `description`, `category`, `price_minor`, `status`. Expected status: `200`,
`400`, `401`, `403`, `404`.

```bash
curl -i -X PUT "$GATEWAY_URL/api/v1/products/00000000-0000-0000-0000-000000000000" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"price_minor": 700}'
```

### Set an event's stock and price override (admin)

Upsert; omit `price_minor` to use the product's base price. `stock_total` cannot drop below the
units already sold (`409`). Expected status: `200`, `400`, `401`, `403`, `404` if the product does
not exist, `409`.

```bash
curl -i -X PUT "$GATEWAY_URL/api/v1/products/events/00000000-0000-0000-0000-000000000000/inventory/00000000-0000-0000-0000-000000000001" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"stock_total": 100, "price_minor": 600}'
```

### Adjust stock (admin)

`delta` is a non-zero integer (negative removes stock); the adjustment is atomic and never takes
available stock below zero. Expected status: `200`, `400`, `401`, `403`, `404` if there is no
inventory for that event and product, `409` if the removal exceeds available stock.

```bash
curl -i -X POST "$GATEWAY_URL/api/v1/products/events/00000000-0000-0000-0000-000000000000/inventory/00000000-0000-0000-0000-000000000001/stock-adjustments" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"delta": -4, "reason": "damaged"}'
```
