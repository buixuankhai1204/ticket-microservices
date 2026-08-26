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
curl -i -X POST "$GATEWAY_URL/api/v1/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "jane.doe@example.com",
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
