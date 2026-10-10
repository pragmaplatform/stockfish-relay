#![cfg(unix)]

use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::Duration,
};

use clap::Parser;
use reqwest::{Client, StatusCode};
use stockfish_relay::{
    config::{Cli, Command, MasterConfig, StockfishConfig, WorkerConfig},
    engine::Stockfish,
    error::Error,
    master,
    protocol::*,
    worker,
};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;

const API_KEY: &str = "client-key-for-tests";
const WORKER_KEY: &str = "worker-key-for-tests";

struct Server {
    origin: String,
    client: Client,
    stop: CancellationToken,
    task: JoinHandle<Result<(), Error>>,
}

impl Server {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}/", listener.local_addr().unwrap());
        Self::on(listener, origin).await
    }

    async fn on(listener: TcpListener, origin: String) -> Self {
        Self::with_config(listener, origin, master_config()).await
    }

    async fn with_config(listener: TcpListener, origin: String, config: MasterConfig) -> Self {
        let stop = CancellationToken::new();
        let shutdown = stop.clone();
        let task = tokio::spawn(async move { master::serve(listener, config, shutdown).await });
        Self {
            origin,
            client: Client::builder().timeout(Duration::from_secs(8)).build().unwrap(),
            stop,
            task,
        }
    }

    async fn status(&self) -> Status {
        self.client
            .get(format!("{}v1/status", self.origin))
            .bearer_auth(API_KEY)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    async fn wait_workers(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.status().await.workers.len() != count {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    async fn stop(self) {
        self.stop.cancel();
        tokio::time::timeout(Duration::from_secs(3), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn admin_dashboard_is_disabled_without_credentials() {
    let server = Server::start().await;
    for path in ["admin", "admin/"] {
        let response = server
            .client
            .get(format!("{}{path}", server.origin))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(response.headers().get("www-authenticate").is_none());
    }
    server.stop().await;
}

#[tokio::test]
async fn admin_dashboard_requires_separate_credentials_and_renders_live_workers_safely() {
    const PASSWORD: &str = "admin-password:for-tests";
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}/", listener.local_addr().unwrap());
    let mut config = master_config();
    config.admin_username = Some("admin".into());
    config.admin_password = Some(PASSWORD.into());
    config.lease_ms = 60_000;
    let server = Server::with_config(listener, origin, config).await;
    for path in ["admin", "admin/"] {
        let url = format!("{}{path}", server.origin);
        for request in [
            server.client.get(&url),
            server.client.get(&url).basic_auth("admin", Some("wrong")),
            server.client.get(&url).basic_auth("wrong", Some(PASSWORD)),
            server.client.get(&url).bearer_auth(API_KEY),
            server.client.get(&url).bearer_auth(WORKER_KEY),
            server.client.get(&url).header("Authorization", "Basic !!!"),
        ] {
            let response = request.send().await.unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert_eq!(
                response.headers()["www-authenticate"],
                "Basic realm=\"Stockfish Relay admin\""
            );
            assert_eq!(response.headers()["cache-control"], "no-store");
            assert!(!response.text().await.unwrap().contains("Relay statistics"));
        }
        let response = server
            .client
            .get(&url)
            .basic_auth("admin", Some(PASSWORD))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "text/html; charset=utf-8");
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert!(
            response.headers()["content-security-policy"]
                .to_str()
                .unwrap()
                .contains("default-src 'none'")
        );
        let body = response.text().await.unwrap();
        assert!(body.contains("No workers connected"));
        assert!(body.contains("<meta http-equiv=\"refresh\" content=\"5\">"));
        assert!(!body.contains(PASSWORD));
        assert!(!body.contains(API_KEY));
        assert!(!body.contains(WORKER_KEY));
    }

    let registration = RegisterWorker {
        protocol_version: PROTOCOL_VERSION,
        name: "<script>alert('worker')</script>".into(),
        capacity: 3,
        engine: EngineInfo {
            name: "Stockfish <19> & \"test\"".into(),
            threads: 2,
            hash_mb: 128,
        },
    };
    let session: WorkerSession = server
        .client
        .post(format!("{}v1/workers/register", server.origin))
        .bearer_auth(WORKER_KEY)
        .json(&registration)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let url = format!("{}admin", server.origin);
    let body = server
        .client
        .get(&url)
        .basic_auth("admin", Some(PASSWORD))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains("&lt;script&gt;alert(&#39;worker&#39;)&lt;/script&gt;"));
    assert!(body.contains("Stockfish &lt;19&gt; &amp; &quot;test&quot;"));
    assert!(!body.contains("<script>"));
    assert!(body.contains(&session.worker_id.to_string()));
    assert!(body.contains("<td>0 / 3</td><td>2</td><td>128 MB</td>"));
    assert!(body.contains("Engine slots</h2><p class=\"value\">3</p>"));
    assert!(!body.contains("No workers connected"));

    // Admin credentials do not grant access to either API role.
    for request in [
        server.client.get(format!("{}v1/status", server.origin)),
        server
            .client
            .post(format!("{}v1/workers/register", server.origin))
            .json(&registration),
    ] {
        assert_eq!(
            request
                .basic_auth("admin", Some(PASSWORD))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(server.status().await.workers.len(), 1);
    server
        .client
        .delete(format!("{}v1/workers/{}", server.origin, session.worker_id))
        .bearer_auth(WORKER_KEY)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let body = server
        .client
        .get(url)
        .basic_auth("admin", Some(PASSWORD))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains("No workers connected"));
    server.stop().await;
}

fn master_config() -> MasterConfig {
    let Command::Master(mut config) = Cli::parse_from([
        "stockfish-relay",
        "master",
        "--api-key",
        API_KEY,
        "--worker-key",
        WORKER_KEY,
    ])
    .command
    else {
        unreachable!()
    };
    config.lease_ms = 900;
    config.long_poll_ms = 50;
    config.search_timeout_ms = 2000;
    config.request_timeout_ms = 7000;
    config
}

fn worker_config(server: &Server, engine: &Path) -> WorkerConfig {
    WorkerConfig {
        master: server.origin.parse().unwrap(),
        worker_key: WORKER_KEY.into(),
        name: "integration".into(),
        concurrency: 2,
        stockfish: StockfishConfig {
            stockfish_path: engine.into(),
            threads: 1,
            hash_mb: 16,
        },
    }
}

fn fixture(dir: &Path, crash_once: bool) -> PathBuf {
    let path = dir.join("stockfish");
    let marker = dir.join("crashed");
    let failure = if crash_once {
        format!(
            "if [ ! -f '{}' ]; then touch '{}'; exit 7; fi",
            marker.display(),
            marker.display()
        )
    } else {
        String::new()
    };
    let script = format!(
        r#"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        uci) printf '%s\n' 'id name Stockfish fixture' 'uciok' ;;
        isready) printf '%s\n' 'readyok' ;;
        go*)
            {failure}
            printf '%s\n' 'info depth 10 score cp 25 nodes 1000 pv e2e4 e7e5' 'bestmove e2e4'
            ;;
    esac
done
"#
    );
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    path
}

fn start_worker(server: &Server, engine: &Path) -> (CancellationToken, JoinHandle<Result<(), Error>>) {
    let stop = CancellationToken::new();
    let shutdown = stop.clone();
    let config = worker_config(server, engine);
    (stop, tokio::spawn(async move { worker::run(config, shutdown).await }))
}

async fn stop_worker(stop: CancellationToken, worker: JoinHandle<Result<(), Error>>) {
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(3), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

async fn evaluate(server: &Server) -> EvaluateResponse {
    server
        .client
        .post(format!("{}v1/evaluate", server.origin))
        .bearer_auth(API_KEY)
        .json(&EvaluateRequest::default())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[tokio::test]
async fn http_roles_health_validation_and_missing_capacity() {
    let server = Server::start().await;
    assert_eq!(
        server
            .client
            .get(format!("{}healthz", server.origin))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        server
            .client
            .get(format!("{}readyz", server.origin))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        server
            .client
            .get(format!("{}v1/status", server.origin))
            .bearer_auth(WORKER_KEY)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        server
            .client
            .post(format!("{}v1/workers/register", server.origin))
            .bearer_auth(API_KEY)
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let response = server
        .client
        .post(format!("{}v1/evaluate", server.origin))
        .bearer_auth(API_KEY)
        .json(&EvaluateRequest::default())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let invalid = server
        .client
        .post(format!("{}v1/evaluate", server.origin))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({"moves":["e2e5"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    server.stop().await;
}

#[tokio::test]
async fn real_http_worker_restarts_a_crashed_engine_and_completes_the_original_call() {
    let dir = tempfile::tempdir().unwrap();
    let engine = fixture(dir.path(), true);
    let server = Server::start().await;
    let (stop, task) = start_worker(&server, &engine);
    server.wait_workers(1).await;
    let response = evaluate(&server).await;
    assert_eq!(response.attempts, 2);
    assert_eq!(response.result.best_move.as_deref(), Some("e2e4"));
    assert_eq!(response.result.engine.hash_mb, 16);
    assert_eq!(server.status().await.retried, 1);
    let (a, b, c) = tokio::join!(evaluate(&server), evaluate(&server), evaluate(&server));
    assert_eq!(a.attempts + b.attempts + c.attempts, 3);
    stop_worker(stop, task).await;
    server.wait_workers(0).await;
    server.stop().await;
}

#[tokio::test]
async fn a_silent_worker_is_replaced_over_http_and_its_late_result_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = fixture(dir.path(), false);
    let server = Server::start().await;
    let registration = RegisterWorker {
        protocol_version: PROTOCOL_VERSION,
        name: "disconnected".into(),
        capacity: 1,
        engine: EngineInfo {
            name: "Stockfish fixture".into(),
            threads: 1,
            hash_mb: 16,
        },
    };
    let disconnected: WorkerSession = server
        .client
        .post(format!("{}v1/workers/register", server.origin))
        .bearer_auth(WORKER_KEY)
        .json(&registration)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let client = server.client.clone();
    let origin = server.origin.clone();
    let request = tokio::spawn(async move {
        client
            .post(format!("{origin}v1/evaluate"))
            .bearer_auth(API_KEY)
            .json(&EvaluateRequest::default())
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<EvaluateResponse>()
            .await
            .unwrap()
    });
    let abandoned: Assignment = loop {
        let response = server
            .client
            .post(format!("{}v1/workers/{}/claim", server.origin, disconnected.worker_id))
            .bearer_auth(WORKER_KEY)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        if response.status() == StatusCode::OK {
            break response.json().await.unwrap();
        }
    };
    let (stop, task) = start_worker(&server, &engine);
    let response = request.await.unwrap();
    assert_eq!(response.job_id, abandoned.id.job_id);
    assert_eq!(response.attempts, 2);
    let late = server
        .client
        .post(format!(
            "{}v1/workers/{}/complete",
            server.origin, disconnected.worker_id
        ))
        .bearer_auth(WORKER_KEY)
        .json(&Completion {
            assignment: abandoned.id,
            outcome: Outcome::Failed {
                error: "late failure".into(),
            },
        })
        .send()
        .await
        .unwrap();
    assert_eq!(late.status(), StatusCode::CONFLICT);
    stop_worker(stop, task).await;
    server.stop().await;
}

#[tokio::test]
async fn worker_reconnects_after_master_restart() {
    let dir = tempfile::tempdir().unwrap();
    let engine = fixture(dir.path(), false);
    let server = Server::start().await;
    let address = server
        .origin
        .strip_prefix("http://")
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let origin = server.origin.clone();
    let (stop, task) = start_worker(&server, &engine);
    server.wait_workers(1).await;
    assert_eq!(evaluate(&server).await.attempts, 1);
    server.stop().await;
    let listener = TcpListener::bind(address).await.unwrap();
    let replacement = Server::on(listener, origin).await;
    replacement.wait_workers(1).await;
    assert_eq!(evaluate(&replacement).await.attempts, 1);
    stop_worker(stop, task).await;
    replacement.stop().await;
}

#[tokio::test]
async fn worker_reconnects_after_repeated_deployment_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = fixture(dir.path(), false);
    let server = Server::start().await;
    let origin = server.origin.clone();
    let address = origin.strip_prefix("http://").unwrap().trim_end_matches('/');
    let (stop, task) = start_worker(&server, &engine);
    server.wait_workers(1).await;
    let previous_id = server.status().await.workers[0].id;
    assert_eq!(evaluate(&server).await.attempts, 1);
    server.stop().await;

    // A deployment proxy can remain reachable while its master is unavailable.
    let listener = TcpListener::bind(address).await.unwrap();
    let (attempted, mut attempts) = tokio::sync::mpsc::unbounded_channel();
    let unavailable = axum::Router::new()
        .route(
            "/v1/workers/register",
            axum::routing::post(move || {
                let attempted = attempted.clone();
                async move {
                    attempted.send(()).unwrap();
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }),
        )
        .fallback(|| async { StatusCode::BAD_GATEWAY });
    let outage_stop = CancellationToken::new();
    let outage_shutdown = outage_stop.clone();
    let outage = tokio::spawn(async move {
        axum::serve(listener, unavailable)
            .with_graceful_shutdown(outage_shutdown.cancelled_owned())
            .await
            .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(8), async {
        for _ in 0..2 {
            attempts.recv().await.unwrap();
        }
    })
    .await
    .expect("worker must keep retrying registration throughout the outage");
    assert!(!task.is_finished());
    outage_stop.cancel();
    tokio::time::timeout(Duration::from_secs(3), outage)
        .await
        .unwrap()
        .unwrap();

    let listener = TcpListener::bind(address).await.unwrap();
    let replacement = Server::on(listener, origin).await;
    replacement.wait_workers(1).await;
    assert_ne!(replacement.status().await.workers[0].id, previous_id);
    assert_eq!(evaluate(&replacement).await.attempts, 1);
    stop_worker(stop, task).await;
    replacement.stop().await;
}

#[tokio::test]
async fn busy_worker_reconnects_after_master_deployment() {
    let dir = tempfile::tempdir().unwrap();
    let engine = dir.path().join("stockfish");
    let resume = dir.path().join("resume");
    let started = dir.path().join("started");
    std::fs::write(
        &engine,
        format!(
            r#"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        uci) printf '%s\n' 'id name Stockfish fixture' 'uciok' ;;
        isready) printf '%s\n' 'readyok' ;;
        go*)
            if [ ! -f '{}' ]; then
                printf '%s\n' started >> '{}'
                exec sleep 30
            fi
            printf '%s\n' 'info depth 10 score cp 25 nodes 1000 pv e2e4 e7e5' 'bestmove e2e4'
            ;;
    esac
done
"#,
            resume.display(),
            started.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&engine, std::fs::Permissions::from_mode(0o700)).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let origin = format!("http://{address}/");
    let mut config = master_config();
    // Recovery must cancel the old searches, rather than wait for their timeout.
    config.search_timeout_ms = 30_000;
    config.request_timeout_ms = 60_000;
    let server = Server::with_config(listener, origin.clone(), config).await;
    let (stop, task) = start_worker(&server, &engine);
    server.wait_workers(1).await;
    let previous_id = server.status().await.workers[0].id;
    let mut requests = Vec::new();
    for _ in 0..2 {
        let client = server.client.clone();
        let origin = origin.clone();
        requests.push(tokio::spawn(async move {
            client
                .post(format!("{origin}v1/evaluate"))
                .bearer_auth(API_KEY)
                .json(&EvaluateRequest::default())
                .send()
                .await
                .unwrap()
        }));
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while std::fs::read_to_string(&started).unwrap_or_default().lines().count() != 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(server.status().await.running, 2);
    server.stop().await;
    for request in requests {
        assert_eq!(request.await.unwrap().status(), StatusCode::SERVICE_UNAVAILABLE);
    }
    std::fs::write(resume, "").unwrap();

    let listener = TcpListener::bind(address).await.unwrap();
    let replacement = Server::on(listener, origin).await;
    replacement.wait_workers(1).await;
    let status = replacement.status().await;
    assert_ne!(status.workers[0].id, previous_id);
    assert_eq!(status.workers[0].capacity, 2);
    assert_eq!(status.running, 0);
    assert!(!task.is_finished());
    let (first, second) = tokio::join!(evaluate(&replacement), evaluate(&replacement));
    assert_eq!(first.attempts, 1);
    assert_eq!(second.attempts, 1);
    assert_eq!(replacement.status().await.completed, 2);
    stop_worker(stop, task).await;
    replacement.stop().await;
}

#[tokio::test]
async fn worker_pool_runs_to_capacity_and_releases_busy_jobs_on_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("busy");
    std::fs::write(
        &path,
        r#"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        uci) printf '%s\n' 'id name Stockfish busy' 'uciok' ;;
        isready) printf '%s\n' 'readyok' ;;
        go*) exec sleep 30 ;;
    esac
done
"#,
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    let server = Server::start().await;
    let (stop, worker) = start_worker(&server, &path);
    server.wait_workers(1).await;
    let mut requests = vec![];
    for _ in 0..3 {
        let client = server.client.clone();
        let origin = server.origin.clone();
        requests.push(tokio::spawn(async move {
            client
                .post(format!("{origin}v1/evaluate"))
                .bearer_auth(API_KEY)
                .json(&EvaluateRequest::default())
                .send()
                .await
                .unwrap()
                .status()
        }));
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let status = server.status().await;
            if status.running == 2 && status.queued_batch == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    stop_worker(stop, worker).await;
    let status = server.status().await;
    assert_eq!(status.retried, 2);
    assert_eq!(status.queued_batch, 3);
    assert_eq!(status.running, 0);
    server.stop().await;
    for request in requests {
        assert_eq!(request.await.unwrap(), StatusCode::SERVICE_UNAVAILABLE);
    }
}

#[tokio::test]
#[ignore = "requires STOCKFISH_RELAY_TEST_ENGINE pointing to a real Stockfish executable"]
async fn real_stockfish_roundtrip_supports_history_multipv_forced_moves_and_mate() {
    let path = std::env::var("STOCKFISH_RELAY_TEST_ENGINE").unwrap();
    let server = Server::start().await;
    let (stop, task) = start_worker(&server, Path::new(&path));
    server.wait_workers(1).await;
    for request in [
        EvaluateRequest {
            moves: vec!["e2e4".into(), "e7e5".into()],
            multipv: 3,
            search: Some(SearchLimit::Nodes(30_000)),
            ..Default::default()
        },
        EvaluateRequest {
            search_moves: vec!["g1f3".into()],
            search: Some(SearchLimit::Depth(5)),
            ..Default::default()
        },
        EvaluateRequest {
            search: Some(SearchLimit::MovetimeMs(50)),
            ..Default::default()
        },
        EvaluateRequest {
            fen: Some("r3k2r/8/8/8/8/8/8/R3K2R w KQkq - 0 1".into()),
            search_moves: vec!["e1g1".into()],
            search: Some(SearchLimit::Nodes(10_000)),
            ..Default::default()
        },
        EvaluateRequest {
            moves: vec!["f2f3".into(), "e7e5".into(), "g2g4".into(), "d8h4".into()],
            search: Some(SearchLimit::Nodes(1000)),
            ..Default::default()
        },
    ] {
        let response = server
            .client
            .post(format!("{}v1/evaluate", server.origin))
            .bearer_auth(API_KEY)
            .json(&request)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body = response.text().await.unwrap();
        assert!(status.is_success(), "{body}");
        let response: EvaluateResponse = serde_json::from_str(&body).unwrap();
        let normalized = request.normalize(&master_config()).unwrap();
        response.result.validate(&normalized, &response.result.engine).unwrap();
        if let Some(forced) = normalized.search_moves.first() {
            assert_eq!(response.result.best_move.as_ref(), Some(forced));
        }
    }
    stop_worker(stop, task).await;
    server.stop().await;
}

#[tokio::test]
async fn interrupted_multipv_keeps_the_last_complete_exact_batch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("interrupted-multipv");
    // Captured from Stockfish 19 with three threads, MultiPV 3, and 30,000 nodes.
    let transcript = include_str!("fixtures/multipv-interrupted.uci");
    let script = format!(
        r#"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        uci) printf '%s\n' 'id name Stockfish fixture' 'uciok' ;;
        isready) printf '%s\n' 'readyok' ;;
        go*) cat <<'STOCKFISH_OUTPUT'
{transcript}STOCKFISH_OUTPUT
            ;;
    esac
done
"#
    );
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    let server = Server::start().await;
    let (stop, task) = start_worker(&server, &path);
    server.wait_workers(1).await;
    let response: EvaluateResponse = server
        .client
        .post(format!("{}v1/evaluate", server.origin))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "moves": ["d2d4", "d7d5", "c2c4", "e7e6", "b1c3", "g8f6"],
            "multipv": 3,
            "search": {"nodes": 30000}
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    stop_worker(stop, task).await;
    server.stop().await;

    assert_eq!(response.attempts, 1);
    assert_eq!(response.result.best_move.as_deref(), Some("g1f3"));
    let variations = response.result.variations;
    assert_eq!(
        variations.iter().map(|v| v.pv[0].as_str()).collect::<Vec<_>>(),
        ["g1f3", "c1g5", "c4d5"]
    );
    assert_eq!(
        variations.iter().map(|v| v.score).collect::<Vec<_>>(),
        [Score::Centipawns(24), Score::Centipawns(22), Score::Centipawns(18)]
    );
    assert!(variations.iter().all(|v| v.depth == 8 && v.nodes == 29307));
}

#[tokio::test]
async fn engine_timeout_terminates_the_process_and_never_returns_a_partial_score() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stalled");
    std::fs::write(
        &path,
        r#"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        uci) printf '%s\n' 'id name Stockfish stalled' 'uciok' ;;
        isready) printf '%s\n' 'readyok' ;;
        go*) printf '%s\n' 'info depth 1 score cp 50 nodes 5 pv e2e4'; exec sleep 30 ;;
    esac
done
"#,
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = StockfishConfig {
        stockfish_path: path,
        threads: 1,
        hash_mb: 16,
    };
    let mut engine = Stockfish::spawn(&config).await.unwrap();
    let request = EvaluateRequest::default().normalize(&master_config()).unwrap();
    let error = engine.evaluate(&request, 50).await.unwrap_err();
    assert!(error.to_string().contains("timed out"));
    engine.terminate().await.unwrap();
}
