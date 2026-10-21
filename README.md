# Stockfish Relay

An HTTP/JSON Stockfish service with remote workers, written in Rust. Run the
master beside your application and run workers on machines with spare CPU
capacity. Each worker opens outbound HTTP(S) connections to the master and runs
a configurable pool of persistent Stockfish processes through UCI.

One binary provides `master` and `worker` modes; `slave` is an alias for `worker`.
The master uses an in-memory queue and requires no database or Redis. This is a
standalone project with its own dependencies, lockfile, and build configuration.

See the [changelog](CHANGELOG.md) for changes by version.

## Quick start

Install the published binary from crates.io:

```bash
cargo install stockfish-relay --locked
```

Or build and install it from the repository root:

```bash
cargo install --path . --locked
```

Cargo installs it in `~/.cargo/bin`; ensure that directory is on `PATH`. To build
without installing, use `cargo build --release --locked`. Both machines run the
same binary. Only workers need a separately installed Stockfish executable;
Stockfish 19 is tested. Set an absolute path with `STOCKFISH_PATH`.

For a local smoke test from the repository, copy `.env.example` to `.env`, replace
both keys with separately generated values (`openssl rand -hex 32`), and adjust
the engine path. Load that environment in each of two terminals:

```bash
set -a
. ./.env
set +a
```

Start the master:

```bash
stockfish-relay master --listen 127.0.0.1:8080
```

Start a worker in the other terminal:

```bash
stockfish-relay worker --master http://127.0.0.1:8080
```

For deployment, use a master URL such as `https://engine.example.com:443`.
The master accepts HTTP behind your infrastructure's HTTPS reverse proxy; workers
verify HTTPS certificates. No inbound listener or port forwarding is needed on
the worker. A worker needs the worker key; API clients need the separate client key.
Use one master replica: its queue and worker sessions are process-local.

## Evaluate a position

```bash
curl --fail-with-body http://127.0.0.1:8080/v1/evaluate \
  -H "Authorization: Bearer $STOCKFISH_RELAY_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"moves":["e2e4","e7e5"],"search":{"nodes":1000000},"search_moves":["g1f3"]}'
```

The request waits for completion, including automatic retries. Its response has
`job_id`, `attempts`, and `result`. The result includes:

- `engine`: the actual UCI engine name, threads, and hash memory.
- `search`: the resolved search limit.
- `best_move`: standard UCI notation, or null in a terminal position.
- `variations`: ordered MultiPV entries with `multipv`, `score`, `depth`, `nodes`,
  and `pv` (a list of UCI moves).
- `elapsed_ms`: execution time for the successful attempt, excluding queue time
  and earlier attempts.

Scores use `{"kind":"centipawns","value":32}` or `{"kind":"mate","value":3}`.
All variations use the evaluated position's side-to-move perspective. Mate zero
means that side is already checkmated. Drawn terminal positions have a zero
centipawn score and no best move. Scores and principal variations are returned;
the relay does not assign mistake labels or practice grades.

MultiPV results come from a complete batch of exact scores at one depth, with
distinct candidate moves. If the final update is incomplete or contains bound
scores, the relay uses the deepest complete batch matching Stockfish's best move.
If no complete batch matches, the attempt fails and follows the normal retry policy.

| Request field | Behavior |
| --- | --- |
| `fen` | Optional starting FEN; omitted means the normal initial board |
| `moves` | UCI history from that starting board, including castling as `e1g1` and promotion as `e7e8q` |
| `search` | Exactly one of `{"nodes":1000000}`, `{"depth":20}`, or `{"movetime_ms":1000}`; omitted uses the configured node budget |
| `search_moves` | Optional restriction on the first move; omitted/empty allows every legal move |
| `multipv` | Number of variations; defaults to 1, bounded by the master's limit and legal root moves |
| `priority` | `batch` (default) or `interactive` |
| `engine` | Optional exact UCI engine name required for routing |

Send move history when available: a bare FEN cannot describe earlier repetitions.
Positions, moves, limits, and returned continuations are validated. Raw UCI
commands and filesystem paths are not accepted through the public API. Threads
and hash memory belong to the worker's configuration, not to individual requests.

`GET /v1/status` (client key) returns master and worker uptime in seconds (`uptime_seconds`),
workers, capacity, running/queued counts, and completed/retried/failed counters.
`/healthz` checks the master process; `/readyz` requires at least one connected
worker. `/openapi.json` serves the generated
contract, also available with `stockfish-relay openapi > openapi.json`.

## Admin dashboard

Set `STOCKFISH_RELAY_ADMIN_USERNAME` and `STOCKFISH_RELAY_ADMIN_PASSWORD` on the
master, then open `/admin` on its existing HTTP address. The browser prompts for
these separate HTTP Basic Auth credentials. Serve it through the same HTTPS
reverse proxy used for the API when accessing it remotely.

The read-only page reloads every five seconds and shows the relay package version
and uptime for the master and each connected worker, engine names, running/available
slots, configured threads and hash memory per engine, interactive/batch queue sizes, and
completed/retried/failed totals. Master uptime and totals reset when the master
restarts. Worker uptime measures time since registration and resets on each new
connection; disconnected workers disappear. There is no persistent historical storage or
collection of actual CPU/RAM usage. Worker versions come from the
`stockfish-relay/<version>` User-Agent sent at registration; workers without this
header show "Unknown". Both master and worker log their relay package version
at startup.

The dashboard also shows average and p95 timings in milliseconds over the latest
1,000 successfully completed jobs. First-attempt completions and completions after
retries have separate sample counts and summaries. Failed and unfinished jobs are
excluded. The p95 uses the nearest-rank method; an empty group shows `—`, not zero.

- **Queue wait:** all time spent queued across the job's attempts.
- **Engine time:** worker-reported execution time for the successful attempt.
- **Dispatch/result overhead:** successful assignment-to-completion time minus
  engine time, clamped to zero. This includes network travel, serialization,
  worker scheduling, and engine startup when needed; it is not pure network latency.
- **Total job time:** acceptance by the master to successful completion, including
  earlier attempts and queue waits. Travel between the API client and master is excluded.

The worker timing table shows execution and overhead for each connected session's
completions in that same shared window. Disconnected workers' samples remain in
the overall summary until evicted; a new worker session starts with no samples.
Timing history is bounded, held in memory, and resets on master restart. These
summaries are also available in `/v1/status` under `timings` and each worker's
`timings`; empty metric values are `null`. Durations use local monotonic clocks,
so master and worker wall clocks need not be synchronized. For retried jobs,
queue, engine, and overhead averages do not sum to total time because unsuccessful
attempt execution is included only in the total. Percentiles are not additive.

Each worker also shows **Heartbeat RTT** (average/p95 and sample count), measured
from sending a heartbeat request to reading its response on that worker. This is
an application-level round trip: it includes HTTP handling but no job execution
or long-poll wait. Workers report the previous successful measurement with their
next heartbeat, requiring no extra requests. The separate per-session window
holds the latest 1,000 reported successful round trips and resets on reconnect.
Older workers remain compatible and show no RTT data until upgraded. In
`/v1/status`, these are `heartbeat_rtt` and `heartbeat_rtt_samples` on each worker.

The dashboard is disabled (404) when both settings are absent. Setting only one
is a configuration error. The username must be printable non-space ASCII without
a colon. The password must contain at least 4 printable non-space ASCII
characters and differ from the client and worker keys; generate one with
`openssl rand -hex 32`. Admin credentials only authorize the dashboard.
On Render, add both variables to the master service's environment to enable it.

## Retries and scheduling

Workers register their engine identity and capacity, then long-poll for jobs.
The master shuffles one entry per worker slot when workers join or leave, then
assigns jobs round-robin through that order. Workers with more slots receive
proportionally more turns. Full workers, incompatible engines, and workers without
a pending claim are skipped so they cannot block ready workers. Both claim and
complete-and-claim follow the same rotation.
Each assignment has a fresh attempt ID and a renewable lease. Heartbeats renew
only assignments the worker actually acknowledges. A lost claim response therefore
cannot strand a job indefinitely.

- A reported Stockfish failure immediately requeues the job. The worker discards
  that process and starts a fresh one for subsequent work.
- A disconnected worker or expired lease requeues unfinished work. Another
  available worker can claim it; the original HTTP request keeps waiting.
- Late results and duplicate completions from old attempts receive `409` and
  cannot overwrite the current attempt's result.
- Retries allow at-least-once execution: after a network partition, an old search
  can briefly overlap its replacement. Only the current assignment is accepted.
- The default limit is three total attempts. Exhaustion returns `500` for engine
  failures (`engine_evaluation_failed` or `incomplete_evaluation`), `503` for lost
  workers (`worker_unavailable`), or `504` for search timeouts (`search_timeout`).
  The final attempt determines the classification. Exhausted responses include
  `code`, `message`, `job_id`, and `attempts`; failures before admission have no job context.
  A 120-second overall deadline includes queuing and retries, returning `504`
  (`request_timeout`).
- Worker failure reports carry a typed `kind`. Older workers remain compatible;
  reports without `kind` are classified as `engine_evaluation_failed`. Upgrade
  workers as well as the master for precise classification and engine diagnostics.
- Master failure logs include job/attempt/worker IDs, classification, reason,
  search settings, elapsed time, and whether retries were exhausted. Recovery logs
  identify evaluations that succeeded after retrying. Worker failures include
  engine identity, threads, hash memory, and search timeout. Missing exact
  evaluations additionally log the starting FEN, move history, best move, and
  the last 16 UCI output lines (at most 512 characters per line), correlated with
  the assignment IDs. Successful searches do not log UCI transcripts.
- Worker reconnect warnings include the nested error details, timeout and
  connection-error flags, HTTP status when available, worker ID, process uptime,
  session duration, and retry delay. Connection logs include the negotiated
  heartbeat, lease, and long-poll intervals to help diagnose timing failures.
- New requests receive `503` when no matching worker is connected and `429` when
  capacity is full. Existing requests can wait for workers to reconnect until
  their deadline. A master restart loses pending jobs; callers must retry.
- Workers reconnect automatically. On shutdown they stop their processes and
  release their assignments; leases recover work if that release cannot reach
  the master. Cancelled or timed-out assignments are stopped on the next heartbeat.

When deploying a replacement master, keep its URL and worker key unchanged.
Running workers abandon their old session and stop its searches when they detect
a connection failure, a temporary server error, or an unknown session. They retry
registration with exponential backoff (starting at one second, capped at 30
seconds, plus up to 255 ms jitter), then resume processing with a fresh session.
Workers do not need to be restarted. Pending requests do not survive the master
restart: a graceful shutdown returns `503`, and callers must retry once workers
have reconnected (`/readyz` returns success). Authentication errors (`401`/`403`)
and invalid requests (`400`) stop the worker instead of retrying.

Interactive jobs take precedence when selecting queued work. After eight
interactive assignments, a compatible waiting batch job gets a turn. Running
jobs are not preempted. There is no persistent result cache; applications can
cache evaluations using the complete history, actual engine configuration, and
search settings. Parallel requests are needed to use multiple worker slots.

## Configuration

Every setting supports environment variables and CLI flags; flags take precedence.
Run `stockfish-relay master --help` or `stockfish-relay worker --help` for the flags.
The binary does not automatically load `.env` files.

| Variable | Default | Applies to |
| --- | --- | --- |
| `STOCKFISH_RELAY_MODE` | `master` | Docker; `master` or `worker` (`slave` alias) |
| `STOCKFISH_RELAY_API_KEY` | Required | Master/client authentication |
| `STOCKFISH_RELAY_WORKER_KEY` | Required | Master/worker authentication |
| `STOCKFISH_RELAY_ADMIN_USERNAME` | Unset; dashboard disabled | Master; HTTP Basic Auth username |
| `STOCKFISH_RELAY_ADMIN_PASSWORD` | Unset; dashboard disabled | Master; separate HTTP Basic Auth password |
| `STOCKFISH_RELAY_LISTEN` | `127.0.0.1:8080` | Master |
| `STOCKFISH_RELAY_MASTER_URL` | Required | Worker; scheme, host, optional port |
| `STOCKFISH_RELAY_WORKER_NAME` | `worker` | Worker display name |
| `STOCKFISH_RELAY_CONCURRENCY` | `1` | Stockfish processes per worker |
| `STOCKFISH_PATH` | `stockfish` on PATH | Worker executable |
| `STOCKFISH_THREADS` | `1` | Threads per engine |
| `STOCKFISH_HASH_MB` | `64` | Hash memory per engine, plus engine overhead |
| `STOCKFISH_RELAY_QUEUE_CAPACITY` | `128` | Master; queued and executing requests combined |
| `STOCKFISH_RELAY_MAX_ATTEMPTS` | `3` | Master; includes initial attempt |
| `STOCKFISH_RELAY_LEASE_MS` | `15000` | Master; heartbeat interval is one third of this |
| `STOCKFISH_RELAY_LONG_POLL_MS` | `20000` | Master |
| `STOCKFISH_RELAY_REQUEST_TIMEOUT_MS` | `120000` | Master; total request deadline |
| `STOCKFISH_RELAY_SEARCH_TIMEOUT_MS` | `30000` | Per attempt; enforced on the worker |
| `STOCKFISH_RELAY_DEFAULT_NODES` | `1000000` | Master; omitted search limit |
| `STOCKFISH_RELAY_MAX_NODES` | `50000000` | Master |
| `STOCKFISH_RELAY_MAX_DEPTH` | `40` | Master |
| `STOCKFISH_RELAY_MAX_MULTIPV` | `5` | Master |
| `RUST_LOG` | `info` | Both |

For example, four processes with three threads each use roughly twelve search
threads. Hash memory also multiplies by concurrency; neural networks and process
overhead consume additional RAM. Set these values to leave capacity for CI builds.
Explicit move time must be below the hard search timeout. Configure the reverse
proxy and API client timeout to exceed the master's overall request deadline.

## Ubuntu worker service

Install the relay binary at `/usr/local/bin/stockfish-relay` and Stockfish at
`/opt/stockfish/stockfish`, both executable by the service user. The following are
deployment commands to run on the intended worker host:

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin stockfish-relay
sudo install -d -m 0750 -o root -g stockfish-relay /etc/stockfish-relay
sudo install -m 0640 -o root -g stockfish-relay deploy/worker.env.example /etc/stockfish-relay/worker.env
sudoedit /etc/stockfish-relay/worker.env
sudo install -m 0644 deploy/stockfish-relay-worker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now stockfish-relay-worker
journalctl -u stockfish-relay-worker -f
```

The service runs at lower CPU priority than normal builds. Its thread/process
limits still need to match the capacity you want to dedicate to analysis.

## Container and Render

GitHub Actions publishes the `linux/amd64` image to
`ghcr.io/pragmaplatform/stockfish-relay` after all CI checks pass:

- Pushes to the default branch (`main`) publish the package version from
  `Cargo.toml` (currently `0.3.6`), `latest`, and `sha-<full-commit-sha>`.
  The package-version tag updates on each successful main build until the version
  changes; use the commit tag or image digest to pin a specific build.
- Version tag pushes such as `v1.2.3` publish `1.2.3`. Prerelease tags such as
  `v1.2.3-rc.1` publish `1.2.3-rc.1`. Version tags do not move `latest`.
- Other branch pushes run checks without publishing. CI runs only on pushes;
  opening or updating a pull request does not trigger a duplicate run.

Publishing uses the workflow's `GITHUB_TOKEN` with `packages: write`; no extra
registry secret is needed. After the first publish, an organization/package admin
must open the [package settings](https://github.com/orgs/pragmaplatform/packages/container/stockfish-relay/settings)
and select **Change visibility → Public**. GHCR packages initially default to
private even when the source repository is public; see
[GitHub's package visibility documentation](https://docs.github.com/en/packages/learn-github-packages/configuring-a-packages-access-control-and-visibility#configuring-visibility-of-packages-for-an-organization).
This is a one-time setup step that enables pulls without authentication.

Pull the published image and give it the local tag used in the examples below:

```bash
docker pull ghcr.io/pragmaplatform/stockfish-relay:latest
docker tag ghcr.io/pragmaplatform/stockfish-relay:latest stockfish-relay
```

For a fixed release, use a version tag such as `:1.2.3`, or pin the image digest.
Alternatively, build from the repository root:

```bash
docker build -t stockfish-relay .
```

The image runs as UID 10001 and includes the relay and Debian's packaged
Stockfish. Configure every runtime setting with environment variables from the
table above. `STOCKFISH_RELAY_MODE` selects `master` (the default) or `worker`;
`slave` is also accepted as a worker alias. Mode selection through this variable
is provided by the container entrypoint.

Start a master using `.env` with your generated keys:

```bash
docker run --rm -p 8080:8080 --env-file .env \
  -e STOCKFISH_RELAY_MODE=master \
  -e STOCKFISH_RELAY_LISTEN=0.0.0.0:8080 \
  stockfish-relay
```

Start a worker pointed at a reachable master:

```bash
docker run --rm --env-file .env \
  -e STOCKFISH_RELAY_MODE=worker \
  -e STOCKFISH_RELAY_MASTER_URL=https://engine.example.com:443 \
  -e STOCKFISH_PATH=/usr/games/stockfish \
  -e STOCKFISH_RELAY_CONCURRENCY=2 \
  -e STOCKFISH_THREADS=1 \
  -e STOCKFISH_HASH_MB=64 \
  stockfish-relay
```

Workers need the master's worker key and require no published ports. The image
defaults to listening on `0.0.0.0:8080` and uses `/usr/games/stockfish`; the examples
override the host-specific paths and addresses in `.env.example`. A worker's
master URL must be reachable from its container; `127.0.0.1` refers to that
container itself.

To use a different engine version, mount a Linux-compatible Stockfish distribution
read-only and set `STOCKFISH_PATH` to its executable inside the container.
Explicit command arguments override the mode variable, for example
`docker run --rm stockfish-relay worker --help` or
`docker run --rm stockfish-relay openapi`. CLI flags still override environment
settings. The entrypoint executes the relay directly so it receives container
shutdown signals.

`render.yaml` is a master blueprint with generated client/worker keys.
Use `/healthz` for the platform health check so deployment does not
depend on the CI worker being online. Supply the generated worker key to the CI
host and the client key to API consumers. No Render deployment or resize is
performed by building this project.

## Verification and licensing

GitHub Actions runs formatting, Clippy, the ordinary test suite, and the real
Stockfish integration test on every push. CI uses Ubuntu 24.04,
Rust 1.98.1, and Ubuntu's packaged Stockfish executable.

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
STOCKFISH_RELAY_TEST_ENGINE=/opt/stockfish/stockfish cargo test --test end_to_end real_stockfish_roundtrip -- --ignored
```

Ordinary tests use local test engines and need no database or installed Stockfish.
They exercise disconnect recovery, stale results, engine crashes, limits, HTTP
authentication, master restart, and the worker protocol. The opt-in real-engine
test checks history, multiple variations, forced moves, castling, and checkmate.

Relay code is MIT licensed; see `LICENSE`. Stockfish is a separate GPLv3 program.
If distributing it with your deployment, include its license and corresponding
source as required by the [upstream terms](https://github.com/official-stockfish/Stockfish#terms-of-use).
