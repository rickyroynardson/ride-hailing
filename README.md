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

## Service identity and permissions

Services no longer share `JWT_SECRET` or sign their own service tokens. The local `identity` service implements a
small OAuth-style **client-credentials exchange** at internal-only `POST /token`. It is a learning implementation,
not a full OAuth/OIDC provider. For a production deployment prefer a maintained identity provider/workload identity.

1. Each calling workload has its own random client secret. Identity stores only its SHA-256 digest and authenticates
   the client with HTTP Basic auth over mTLS; the secret's client ID must match the certificate workload identity.
   The request cannot choose another identity or elevate scopes.
2. Identity signs an **Ed25519 JWT valid for 60 seconds**, for one target audience, with exact RPC-method scopes.
3. `jwtauth.TokenCredentials` caches tokens separately per target, refreshing within ten seconds of expiry.
   Concurrent refreshes are coalesced; exchanges have a two-second HTTP timeout and follow no redirects.
4. Backends verify the signature, key ID, issuer, identity kind, issued-at/expiry and audience. Both the token's scope
   and the backend's local service allowlist must permit the exact RPC. Missing/invalid tokens return
   `Unauthenticated`; authenticated callers without permission return `PermissionDenied`. For service JWTs, the
   caller must also match the mTLS client certificate's workload URI.

The policy in `pkg/jwtauth.ServicePermissions` starts deny-by-default:

| Caller | Allowed internal work |
| --- | --- |
| ride | Start/cancel matching, request a payment charge, read driver location |
| matching | Find nearby drivers; filter/assign drivers or mark a ride unmatched |
| payment | Record a payment outcome on ride |
| devconsole (development only) | Start/cancel matching and issue refunds |
| location, user, gateway | No service-token grants |

User login remains separate: **user** alone holds a different signing key and issues 15-minute user tokens. A user key
cannot issue service tokens, and the service issuer cannot mint user identities. Backends receive public keys only;
the gateway still forwards user tokens without holding JWT signing keys or client secrets (it has its own TLS key).
The devconsole no longer
holds a signing secret; it keeps only its scoped client credential, alongside its existing privileged database access.

### Docker-managed provisioning and TLS

All auth secrets, TLS keys and certificates live in **named Docker volumes**, not host `.local` files. No Go, goose,
OpenSSL, cloud account or public domain is required on the host. Start the core stack from the repository root:

```sh
sh scripts/security.sh up
```

This builds one image at a time, runs `security-init` without networking, applies existing SQL migrations through the
one-shot `migrate` service, then starts the core services. Ordinary `docker compose up -d --build` also provisions via
`service_completed_successfully` dependencies, but can build many images at once and starts observability too.
Automatic migrations here are a local-development convenience, not a production mixed-version deployment strategy.

`security-init` alone mounts `security_ca`, which holds a one-year development CA and its private key. Each workload
gets its own separate `security_<name>` volume: a seven-day certificate/key, public CA, public JWT trust bundle, and only
that workload's own JWT signing key or client credential when needed. Runtime mounts are read-only; files are mode 0600
and owned by the distroless nonroot UID 65532. The root provisioning container has no Docker socket and no network.
Negative-test credentials live in `security_checks`, mounted only by the disposable test container; it never receives
the CA private key. No private material is checked in or baked into an image.

Provisioning is idempotent: startup validates existing state instead of silently replacing credentials. Incomplete,
expired or mismatched state fails closed with an error. Inspect interrupted initialization, or use scoped security
cleanup below to regenerate it. Existing databases are not reset by startup or provisioning.

Transport protections:

- Identity serves **HTTPS with required client certificates**; there is no HTTP listener or public gateway route.
- All five backend gRPC listeners require **mTLS/TLS 1.3**. Gateway, devconsole and calling services present their own
  workload certificate. Clients check the expected service's DNS SAN (e.g. `payment`), not just whether any CA signed it.
- Certificates carry `spiffe://ride-hailing.local/workload/<name>` URIs. This is a local identity convention, not a
  SPIFFE/SPIRE implementation. The JWT service identity must match that URI at both issuer and backend.
- mTLS authenticates the transport; JWT audience, exact RPC scopes, caller-kind checks and business authorization still
  apply. A valid ride certificate and token do not allow refunds or acting as an end user.
- Browser-facing gateway/devconsole HTTP stays loopback-only for local development. Postgres, Redis and telemetry are
  not TLS-enabled by this change. The Docker CA is for development, not a production PKI or service mesh.

When switching from the old host-file/plaintext setup, rebuild/recreate all core services together. They are not
wire-compatible with old binaries. JWT signing material is newly provisioned, so refresh/login is needed for old access
tokens; stored refresh sessions and business data need no reset. Legacy HS256 helpers remain only for old isolated
test fixtures; deployed constructors have no HS256 or plaintext fallback.

Shared auth configuration is read from environment variables (not the service YAML):

| Variable | Who needs it |
| --- | --- |
| `AUTH_TRUST_BUNDLE_FILE` | Every backend and outgoing service-token client; public keys indexed by key ID |
| `USER_SIGNING_KEY_FILE` | User only |
| `SERVICE_SIGNING_KEY_FILE`, `SERVICE_CLIENTS_FILE` | Identity only |
| `SERVICE_CLIENT_FILE`, `SERVICE_TOKEN_URL` | Ride, matching, payment and devconsole |
| `TLS_DIR` | All core workloads, including identity and gateway; own `tls.crt`, `tls.key` and `ca.crt` |

Missing/malformed auth files fail startup rather than falling back to a shared secret. Display/telemetry `SERVICE_NAME`
does not select identity: the client credential and TLS certificate do. The old `AUTH_ALLOW_INSECURE` flag is ignored by
deployed constructors. Keep clocks synchronized. A stolen service token or client secret alone cannot be presented
under another workload's certificate. Theft of that workload's TLS key as well allows impersonation until rotation or
expiry. There is no per-token/certificate revocation list; established connections can outlive leaf-certificate expiry.

### Certificate rotation, cleanup and tests

```sh
sh scripts/security.sh check   # Docker loopback unit tests + live mTLS/JWT smoke checks; no financial writes
sh scripts/security.sh rotate  # New workload keys/certs under the same CA, recreate core services, rerun checks
sh scripts/security.sh clean   # Stop/remove core containers and this project's security volumes; retain DB data/images
sh scripts/security.sh clean --images # Also remove unused images labelled for this Compose project (never force)
```

Certificate rotation preserves the CA, JWT signing keys and client secrets, so user tokens and grants do not change.
Certificates are loaded at startup; this is a coordinated restart, not hot reload or a zero-downtime rollout. Do not run
initialization, rotation and cleanup concurrently. Rotate before the seven-day leaves expire; initialization will reject
expired leaves and direct you to rotation. A new CA requires scoped security cleanup and reprovisioning; CA rollover
with overlapping roots is not implemented.

Cleanup selects volumes by **both Compose project label and security label**. It never globally prunes Docker or removes
another project's volumes. After `clean`, `up` regenerates security; existing access tokens need refresh/login. For a
deliberate full local reset, including Postgres/observability data and Compose-built images:

```sh
sh scripts/security.sh reset --yes-delete-data
```

`docker compose down` alone keeps named volumes; `docker compose down -v` destroys **all** project volumes, including
databases and the CA. Images and shared BuildKit caches are separate: full reset removes Compose-local images and
unused project-labelled images, including old/dangling builds. Images still in use or referenced by other tags are kept;
shared build caches are intentionally not globally pruned. No host PEM/credential files need cleanup in the new setup.
Observability is not started by the core helper; exporter warnings are expected until you start that stack.

`check` builds/runs unit tests inside Docker, then uses a `--rm` container with only the ride identity and test fixture
volumes. It verifies HTTPS, client certificates, wrong-host rejection, unknown/expired certificates, missing JWTs,
audience/scopes and certificate/JWT identity binding. Both client and server expiry, provisioning idempotency, isolation
and certificate rotation also have deterministic loopback unit tests. The CA private key is never mounted into checks.

### JWT key rotation and issuer outages

- Add the next public key under a new ID alongside the old key in `trust.json`, then restart every verifier/client
  to load the overlapping ring. Switch the relevant signer's private-key file and restart that signer. After the last
  old token expires (60 seconds for services, 15 minutes for users, plus an operational margin), remove the old public
  key and restart verifiers again. Unknown key IDs are rejected; no network key-fetch or hot reload is implemented.
  The current helper automates TLS leaf rotation only; JWT key changes require an explicit provisioning change.
- For client-secret rotation, replace that client's digest in identity's registry and its own credential file; restart
  identity and that client. There is no dual-secret grace window, so coordinate the rollout. Removing a client prevents
  new issuance; already issued tokens stay valid until expiry.
- An issuer outage does not affect backend verification or still-fresh cached tokens. Once refresh is needed, callers
  fail within their budgets; they never self-sign or reuse expired tokens. Existing idempotent application retries
  handle recovery. Refresh failures aren't cached/backed off beyond normal caller backpressure and retry policies.
  Exchange failures return `Unavailable` (HTTP 503), not a misleading end-user authentication failure/401.

Check the auth mechanism without Docker, database changes or requests to the running stack:

```sh
(cd pkg && go test -race ./jwtauth ./grpcserver)
(cd services/gateway && go test -race -run TestCentralIdentityOverGRPC ./internal/gateway)
```

Tests cover client impersonation, forbidden grants/scopes, user/service separation, wrong audiences, expiry, key
rotation, cached tokens/issuer outages, redirect protection, unary/stream authorization and real loopback gRPC calls.

## Readiness, shutdown and safe-deployment exercise

Every core process now exposes **`GET /livez`** and **`GET /readyz`**. Backends and gateway expose them on their existing
admin HTTP listener (container port 8080); identity uses internal port 9090, and devconsole uses port 9000. The old `/`
pong is not a readiness check. Probes need no token/key and never expose storage error details.

- Liveness reports that the HTTP process is responding, even when storage is unavailable or the instance is draining.
- Readiness starts false. TLS, auth and schema setup must succeed and all listeners must bind before readiness is
  enabled. A readiness probe also checks the service's direct Postgres/Redis dependencies with a 500ms budget.
  It does not recursively probe other services, so one downstream outage doesn't make every service unready.
- On SIGTERM/SIGINT, readiness immediately becomes 503 and new business RPCs/HTTP requests get `Unavailable`/503.
  Already admitted calls may finish. Background loops are canceled and joined before shared resources close; interrupted
  worker deliveries still rely on the existing lease fencing and receiver/provider idempotency.
- A 500ms withdrawal window precedes transport shutdown. HTTP and gRPC then drain concurrently under **one five-second
  budget**, including that window. Remaining streams/connections are force-closed at the deadline. Telemetry flush has
  a separate one-second limit. Compose grants 12 seconds before its final process kill; arbitrary Go code that ignores
  cancellation cannot safely be killed individually. Expected stream timeouts log a forced drain, not a fatal crash.

Distroless images include a small `/probe` binary, so Compose needs no curl/shell or private keys for health checks.
`sh scripts/security.sh up` waits for readiness; startup-only dependencies can wait for identity to become healthy.
**Compose health checks are not a load balancer:** unhealthy instances are not automatically removed from DNS, traffic
is not automatically rerouted by readiness, and Docker does not restart a container merely because it becomes unhealthy.
An orchestrator/load balancer must implement that policy. During a restart callers may see transient errors; retry writes
only with the same idempotency key. This change adds no automatic replay policy or transparent stream resumption.

Run the controlled Docker drill:

```sh
sh scripts/security.sh deployment-check
```

This builds lifecycle unit tests inside Docker, then starts **two disposable payment-like fixture replicas** and a
separate tmpfs Postgres store. They use the existing public trust/leaf identities read-only, never the CA private key or
Docker socket. There are no fixture host ports. The fixture-only stop endpoint sends SIGTERM to its own process;
production services do not expose an HTTP shutdown/kill endpoint.

The drill holds an admitted unary call and a watch stream, signals replica A, checks `readyz=503` while `livez=200`, and
verifies new work is rejected. Replica B accepts a replay using the same key while A finishes with a deliberately lost
acknowledgement. The store must contain **one durable fixture effect**, and no effect for the rejected request. The
long stream must close within the deadline and A must exit normally, not via SIGKILL. After Docker restarts A, both
replicas accept replays without another effect and a client explicitly reopens a watch stream.

Failover/retry selection is explicit in the drill; it does not claim to demonstrate a production rolling orchestrator
or exactly-once network delivery. The fake effect/receipt models durable idempotency, while the existing ride/payment
failure-recovery suites cover real business postings separately. No development accounts, rides, charges or refunds
are created. The script removes fixture containers, anonymous volumes and tmpfs data on success/failure; retained test
images can be removed with the existing scoped `clean --images`. The full reset also includes this test profile.

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
