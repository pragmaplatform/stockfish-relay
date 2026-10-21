# Changelog

User-facing changes to Stockfish Relay, newest first. Historical entries are
backfilled from Git history and grouped by package version in `Cargo.toml`;
dates come from that history.

## 0.3.6 - 2026-10-21

### Changed

- Include nested error details, timeout and connection-error flags, HTTP status,
  worker ID, process uptime, session duration, and retry delay in worker reconnect
  warnings. Log negotiated heartbeat, lease, and long-poll intervals on connection.

## 0.3.5 - 2026-10-13

### Changed

- Classify exhausted evaluations as engine failures (500), lost workers (503), or
  search timeouts (504), with job IDs and attempt counts in error responses.
  Older worker failure reports remain supported as generic engine failures.
- Correlate master and worker failure logs with assignment IDs and search settings;
  log recovered evaluations and bounded UCI diagnostics for missing exact scores.

## 0.3.4 - 2026-10-13

### Changed

- Assign jobs round-robin through shuffled worker slots, distributing turns in
  proportion to worker capacity. Both claim endpoints use the same rotation,
  skipping full, incompatible, or non-polling workers. Joining or departing
  workers reshuffle the slots.
- Skip incompatible queued jobs before searching the slot order and reuse engine
  selections within each dispatch pass to keep mixed-engine backlogs responsive.

## 0.3.3 - 2026-10-12

### Changed

- The admin dashboard uses the available screen width so wide tables can fit on
  desktop displays. Tables remain horizontally scrollable on smaller screens.

## 0.3.2 - 2026-10-12

### Added

- Average/p95 queue wait, engine execution, dispatch/result overhead, and total
  job time in the dashboard and `/v1/status`, using a rolling window of the latest
  1,000 successful completions. First-attempt and retried jobs are summarized
  separately, with sample counts and per-worker execution/overhead statistics.
- Per-worker heartbeat round-trip time, measured without extra requests and
  summarized over the latest 1,000 reported successful heartbeats per session.
  Older workers remain compatible and show no RTT data until upgraded.

## 0.3.1 - 2026-10-11

### Added

- Master and worker relay versions in the admin dashboard and `/v1/status`.
  Worker versions come from their registration User-Agent; unavailable versions
  appear as "Unknown" in the dashboard and `null` in the status API.
- Relay package version in master and worker startup logs.

## 0.3.0 - 2026-10-11

### Changed

- Workers submit completed results and request their next job in one HTTP call
  using `/v1/workers/{worker_id}/complete-and-claim`, reducing round trips between
  searches. Results reach waiting clients before the next job's long poll ends.
- Worker registration advertises support for the combined operation. New workers
  fall back to separate completion and claim requests with older masters, and
  the existing endpoints remain available for older workers.

## 0.2.1 - 2026-10-11

### Added

- Master and worker uptime in the admin dashboard and `/v1/status`, exposed as
  `uptime_seconds`. Master uptime resets on restart; worker uptime measures time
  since registration and resets on reconnection.

### Changed

- Reduced the minimum admin password length from 16 to 4 printable non-space
  ASCII characters. Admin passwords must still differ from client and worker keys.
- Documented existing worker recovery during master deployments, including
  automatic reconnection and the need for clients to retry pending requests
  after a master restart.

## 0.2.0 - 2026-10-11

### Added

- Optional read-only admin dashboard at `/admin`, protected by separate HTTP
  Basic Auth credentials configured with `STOCKFISH_RELAY_ADMIN_USERNAME` and
  `STOCKFISH_RELAY_ADMIN_PASSWORD`. The page refreshes every five seconds and
  shows connected workers, engine configuration, slot usage, queue sizes, and
  completed/retried/failed totals. It is disabled when credentials are unset.
- Package-version tags for Docker images published from `main`, alongside
  `latest` and commit tags. Package-version tags update on each successful main
  build until the version changes.

## 0.1.0 - 2026-10-11

### Added

- HTTP/JSON Stockfish evaluation service with `master` and `worker` modes in one
  binary, plus `slave` as an alias for `worker`. Remote workers connect outbound
  over HTTP(S) and run configurable pools of persistent Stockfish processes.
- `/v1/evaluate` with FEN and move history, node/depth/time search limits,
  restricted root moves, MultiPV, and routing by engine name. Responses include
  best moves, scores, principal variations, engine settings, and attempt counts.
- Position and move validation, terminal-position handling, and consistent
  MultiPV results selected from complete exact-score batches at one depth.
- In-memory scheduling with interactive and batch priorities, bounded capacity,
  renewable assignment leases, automatic retries, stale-result rejection,
  worker reconnection, and cancellation of expired work.
- Separate bearer keys for API clients and workers, `/v1/status`, health and
  readiness endpoints, and generated OpenAPI documentation available over HTTP
  and through the `openapi` command.
- Environment-variable and CLI configuration, an Ubuntu systemd worker service,
  and a Render master deployment blueprint.
- Docker image bundling Stockfish and running as a non-root user, with
  `STOCKFISH_RELAY_MODE` selecting master or worker mode.
- Automated publication of `linux/amd64` Docker images to
  `ghcr.io/pragmaplatform/stockfish-relay` after CI passes, with `latest` and
  commit tags for `main` builds and version tags for version-tag builds.
