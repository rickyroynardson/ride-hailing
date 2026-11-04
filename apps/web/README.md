# Jalan web client

A small rider/driver app using **only the public gateway**. No service token, JWT signing secret, database credentials,
driver approval shortcut or administrative refund controls. This is a local preview, not a production payments app.

## Run

Start the backend and apply its migrations as usual, then:

```sh
cd apps/web
npm ci
npm run dev
```

Open **http://localhost:5173**. Vite forwards `/v1` to `http://localhost:8000`. If the gateway is elsewhere, set
`GATEWAY_URL` when starting Vite; that address is used by the dev server, not embedded in the browser bundle.

Alternatively, from the repository root:

```sh
docker compose --profile web up -d --build web
```

The optional `web` profile serves the built app on the same port. Its nginx proxy streams `/v1` responses without
buffering. Do not run it and Vite on port 5173 at the same time. Nothing starts automatically just by installing npm
dependencies.

## Try one trip (manually, not a load test)

1. Create a rider account in one tab, and a different driver account in another tab/browser.
2. On the driver account choose **I’m driving** and apply with a license, vehicle plate and model.
3. An administrator must approve the driver outside this app. In local development, for example:

   ```sh
   docker compose exec postgres psql -U postgres -d postgres -c \
     "UPDATE driver_profiles SET status = 'approved' WHERE user_id = '<driver-user-id>';"
   ```

   The ID is available from the driver's authenticated `GET /v1/me` response. Then click **Refresh approval status**;
   it also refreshes the access token to pick up the driver claim. The browser has no privileged approval endpoint.
4. Choose the driver's manual position on the map and **Go online**. Choose a nearby pickup/dropoff for the rider,
   **Estimate**, then **Request ride**.
5. The driver gets a pushed offer. Accept, start at pickup, and complete at dropoff. Both clients follow the trip
   stream. The rider then sees the charge; the driver can refresh earnings after capture.
6. To test a decline, pick **Declined card** under the rider's Payment method before completing. After the decline,
   save **Successful payment** and retry the payment. No real card information or money is involved.

## Deliberate boundaries

- Access and refresh tokens stay **in memory**; reloading requires signing in again. Automatic refresh is serialized
  because refresh tokens are single-use. Sessions are independent across tabs. Do not use the same single-use refresh
  token in multiple client instances.
- Only the last ride ID and selected mode are saved in tab-scoped `sessionStorage`, keyed by user ID. Signing in again
  can resume that ride. The API has no ride history/current-ride lookup; another device needs the ride ID manually.
  Copy the ID from the trip panel before leaving. If a booking response is lost, this API cannot recover its ID by
  user: don't keep rebooking. That needs a future current-ride API or idempotent booking contract.
- The map starts in Jakarta. It uses public OpenStreetMap tiles (internet access required). The dashed connection is
  **not** a road route or ETA. Coordinates also work when tiles can't load.
- Driver location is **manual**, with heartbeats every 4 seconds. There is no GPS tracking, navigation or background
  mobile service. Keep the tab open; browsers may throttle background tabs. Going offline explicitly removes the
  driver; closing a tab leaves the last position to expire server-side (about 30 seconds).
- Driver offers and ride updates use authenticated `fetch` NDJSON streams, not `EventSource`. They reconnect on
  disconnect, and refuse to act on stale offers. A ride's terminal snapshot stops its stream; payment is asynchronous
  and is polled for up to a minute before offering a manual refresh.
- Gateway rate limits apply normally, including register/login by IP. A 429 is shown with Retry-After when available.
  Requests are not silently bypassed or retried after ambiguous booking failures.
- Earnings is a ledger balance, **not a payout**. Refunds remain an internal support/admin operation.
- Production would need HTTPS, a deployment/security review and real payment-provider tokenization. Keeping tokens
  in memory avoids persistent token storage but doesn't protect them from JavaScript/XSS during a session.

## Lightweight checks

```sh
npm test
npm run build
```

Tests mock HTTP and streaming responses. They do not start Docker, register real users, or run load tests.
