# ride-hailing

## Public API

Clients talk only to the gateway (`services/gateway`, `localhost:8000` in compose). It serves HTTP/JSON and calls the
gRPC services behind it. Routes come from the `google.api.http` options in `proto/`; an RPC without one, like every
service-to-service call, has no public route. `TestOnlyPublicRPCsHaveRoutes` lists the public RPCs, so exposing one
means changing that test too.

- **Auth:** send `Authorization: Bearer <access token>` from `/v1/auth/login`. The gateway forwards it and the services
  check it; the gateway holds no secrets and forwards no other header.
- **Errors:** an HTTP status with `{"code", "message", "details"}`, where `code` is the gRPC code.
- **Streams** (`:watch` routes) are newline-delimited JSON, one `{"result": {...}}` per line, sent as things change.
  They don't time out; reconnect when one closes, and the first line is the current state again.
- **Rate limits** (per minute): 10 per client IP on register, login and refresh; 300 per access token; 60 per client
  IP without a token. Over the limit is `429` with `Retry-After`. Set `AUTH_LIMIT_PER_MINUTE`, `USER_LIMIT_PER_MINUTE`
  and `ANON_LIMIT_PER_MINUTE` to change them.

| Route | Who | What |
|---|---|---|
| `POST /v1/auth/register`, `/login`, `/refresh`, `/logout` | anyone | Account and tokens |
| `GET /v1/me` | user | Profile, and driver profile if any |
| `POST /v1/me/driver` | user | Apply to drive |
| `PUT /v1/me/payment-method` | rider | Set the card |
| `POST /v1/fares:estimate` | rider | Price a trip |
| `POST /v1/rides` | rider | Book; the fare is fixed here |
| `GET /v1/rides/{ride_id}:watch` | rider, driver | Stream the ride until it finishes |
| `POST /v1/rides/{ride_id}:cancel` | rider | Cancel; a fee applies once a driver accepted |
| `POST /v1/rides/{ride_id}:start`, `:complete` | driver | Pick up, drop off |
| `GET /v1/rides/{ride_id}/charge` | rider, driver | The ride's charge and refunds |
| `POST /v1/rides/{ride_id}/charge:retry` | rider | Retry a declined charge |
| `PUT /v1/driver/location`, `DELETE /v1/driver/location` | driver | Heartbeat position; go offline |
| `GET /v1/driver/offer:watch` | driver | Stream offers as they are made |
| `GET /v1/driver/offer` | driver | The pending offer, for clients that can't stream |
| `POST /v1/offers/{offer_id}:accept`, `:decline` | driver | Answer an offer |
| `GET /v1/driver/earnings` | driver | Ledger balance |
