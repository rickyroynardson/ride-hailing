# ride-hailing

## Rider / driver app

`apps/web` is a React/TypeScript client using only the public gateway. Run `npm ci && npm run dev` in that directory,
then open http://localhost:5173. See [the web app guide](apps/web/README.md) for driver approval, a manual trip walkthrough,
session/recovery limitations, and the optional Docker profile. No database access or signing secrets are shipped to
the browser.

## Development console

`http://localhost:9000` exercises public actions through the gateway (`GATEWAY_URL`, default
`http://localhost:8000` when running on the host). Calls show their HTTP method, path and status.
Drivers listen on `WatchOffers` automatically; the offer card is driven by that stream, not the database inspector.
Riders and drivers follow trips through `WatchRide`. Streams reconnect with refreshed tokens after disconnects.

The console is still **development-only**: database tables, driver approval and reset are privileged inspection/admin
features. Manual StartMatching and Refund controls use internal gRPC and are labelled `internal gRPC` in the call log;
they are deliberately not exposed by the gateway. Register/login limits apply to the console too; a 429 is shown as
such instead of silently bypassing the gateway. Rebuilding the console clears its in-memory sessions.

## Service discovery

Backend clients use `pkg/grpcserver.NewClient`. It resolves service DNS names at connection startup and every
five seconds while the connection is active, even when the current endpoint is healthy. This matters after
container recreation: another gRPC service can reuse the old IP, accept connections, and return `unknown service`
without triggering normal connection-failure discovery. A changed DNS answer updates the existing client connection.

- Addresses must be `host:port` or `dns:///host:port`; only A/AAAA lookup is supported, not SRV/TXT, custom DNS
  authorities or other resolver schemes. Literal IPs work but cannot discover a replacement address.
- Each lookup has a two-second timeout. Failed/empty answers keep the last usable address set; refreshes continue.
  Reordered or duplicate answers do not churn connections. Closing the client cancels discovery.
- Connection backoff starts at 200ms and caps at three seconds. This is not a recovery-time guarantee: DNS caching,
  lookup time, connection establishment and caller deadlines still matter. Connections remain lazy at startup.
- Credentials and tracing options are unchanged. The helper adds no RPC retry policy or readiness wait; callers
  can provide earlier deadlines, and retries of writes still need idempotency. gRPC's normal transparent retries still apply.
- Discovery affects subsequent connections/calls; it cannot resume an existing stream. Stream consumers must reconnect.
  It also does not verify service identity: scoped authentication/mTLS is a separate concern.

Run the small loopback-server and controlled-DNS tests without Docker or changes to machine DNS:

```sh
(cd pkg && go test -race -count=1 ./grpcserver)
```

`TestClientRecoversFromHealthyWrongService` first reaches the original service, reconnects its old address to a
healthy server that returns `Unimplemented`, then changes the DNS answer and verifies recovery using the same client.
Other checks cover initial/transient lookup failures, normalized answers, cancellation, literal IPs and no replay
of an RPC that a handler received and answered with `Unavailable`.

## Backpressure

`pkg/grpcserver.NewClient` also puts a bulkhead around outgoing unary RPCs, shared across methods on that connection:

- **32 active calls**, with at most **32 additional calls waiting**. Excess work immediately returns gRPC
  `ResourceExhausted` (HTTP **429** through the gateway), without invoking the backend.
- **Three seconds total**, including queue time, discovery/connection establishment and the RPC itself.
  Earlier caller deadlines win. A queued request that cancels or expires is removed without dispatching it.
- Slots are released when calls return, including errors/cancellation. Waiting doesn't start extra worker goroutines;
  an unrelated backend connection has its own capacity. This is a bounded queue, not a strict FIFO scheduler.
- Streams bypass unary limits and deadlines; they need separate admission/lifetime policies. Existing gateway
  rate limiting remains separate: rate limits control arrivals over time, while concurrency limits bound work held now.

All existing shared backend clients use these defaults. They are initial guardrails, not measured production capacity.
Use `grpcserver.NewClientWithLimits(address, grpcserver.ClientLimits{MaxConcurrent: 16, MaxQueued: 8, Timeout: 2*time.Second}, opts...)`
for a dependency-specific policy; `MaxQueued: 0` means no waiting. Reuse connections: creating one per request defeats
the limit. Limits are per process/connection, not cluster-wide, and do not cap incoming HTTP connections, server work
from other clients, or streams. Client cancellation is not a rollback: a backend that ignores cancellation can continue
working after the caller returns. Keep write retries idempotent; no retry policy was added here.

Small deterministic tests use held slots and a slow loopback payment server, not the running stack:

```sh
(cd pkg && go test -race -run 'TestUnaryBulkhead|TestClientLimitsValidation' ./grpcserver)
(cd services/gateway && go test -race -run TestSlowPaymentBackpressureAndRecovery ./internal/gateway)
```

They check active/queue bounds, queued cancellation/expiry, earlier deadlines, capacity recovery, independent
connections, HTTP 429 translation, and that streams do not inherit unary limits. No Docker or load generation is needed.

## Failure recovery checks

Focused integration tests exercise crash boundaries without generating traffic against the running stack:

```sh
(cd services/ride && go test -race -p 1 -run TestFailureRecovery -v ./internal/outbox)
(cd services/payment && go test -race -p 1 -run TestFailureRecovery -v ./internal/payment)
```

They require Docker and create disposable Postgres containers, one suite at a time. For OrbStack, set
`DOCKER_HOST=unix://$HOME/.orbstack/run/docker.sock` if testcontainers doesn't find it. `-short` skips these tests.
They do not migrate or reset the development databases, start the Compose services, or run load tests.

Execution is stopped after a claim or provider/receiver commit, checkpoint writes are rejected with test-only SQL
constraints/triggers, and replacement workers resume from the same durable state. SQL explicitly expires leases;
there are no sleeps waiting out lease durations. Assertions cover replay/order, one provider operation, atomic ledger
postings, balanced postings and unchanged replacement leases after stale successes, declines or errors.

A lease is not a guarantee that the old process stopped: it may resume after another worker has claimed the job.
Outbox `claim_version`, and payment/refund attempt counters, fence checkpoint writes to their current owner. This
cannot prevent an old worker from making a remote call, so provider/receiver idempotency remains essential.
This harness tests those durable boundaries, not actual OS process kills, network partitions or Postgres crashes.

The outbox fencing migration must be applied before starting the updated ride service. Restart all worker replicas
when deploying the change; an old binary still has its unfenced write path.

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
- **Booking idempotency:** `POST /v1/rides` requires `requestId` (UUID). Generate it once per booking intent and keep
  the exact pickup/dropoff on retries. The key is scoped to the authenticated rider; a replay returns the same ride
  ID and stored fare even after completion or a later payment failure. Changed coordinates on the same key are
  rejected. Failed transactions consume no key. Keys are retained with the ride, without an expiry window.
- **Trip recovery:** `GET /v1/rides/current` returns the authenticated caller's active rider/driver trip, or an unset
  `ride` if none. It accepts no user ID. If an account is in both roles, its driver trip takes priority. This is not a
  receipt/history/unpaid-charge lookup.
- **Payment idempotency:** one charge per ride, with conflicting charge details rejected. `charge:retry` requires a
  fresh `requestId` for each intentional retry; re-delivering it never starts another attempt. A card is frozen for
  an attempt, so changing cards while an outcome is unknown cannot silently change an in-flight request. The local
  fake provider now stores receipts separately from the ledger in Postgres, surviving application restarts. It
  cannot reconstruct provider receipts from before that migration; use new test trips or a dev reset for those.
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
| `GET /v1/rides/current` | rider, driver | Recover the authenticated user's active trip |
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
