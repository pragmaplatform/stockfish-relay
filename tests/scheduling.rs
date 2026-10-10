use std::time::Duration;

use axum::http::StatusCode;
use stockfish_relay::{config::MasterConfig, master::Master, protocol::*};
use tokio::task::JoinHandle;

fn config() -> MasterConfig {
    MasterConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        api_key: "client-key-for-tests".into(),
        worker_key: "worker-key-for-tests".into(),
        queue_capacity: 16,
        max_attempts: 3,
        lease_ms: 1000,
        long_poll_ms: 10,
        request_timeout_ms: 10_000,
        search_timeout_ms: 5000,
        default_nodes: 1000,
        max_nodes: 50_000_000,
        max_depth: 40,
        max_multipv: 5,
    }
}

fn worker() -> RegisterWorker {
    RegisterWorker {
        protocol_version: PROTOCOL_VERSION,
        name: "test worker".into(),
        capacity: 1,
        engine: EngineInfo {
            name: "Stockfish fixture".into(),
            threads: 1,
            hash_mb: 16,
        },
    }
}

async fn submit(
    master: &Master,
    request: EvaluateRequest,
) -> JoinHandle<Result<EvaluateResponse, stockfish_relay::error::ApiError>> {
    let master = master.clone();
    let task = tokio::spawn(async move { master.evaluate(request).await });
    tokio::task::yield_now().await;
    task
}

fn success(assignment: &Assignment) -> Completion {
    Completion {
        assignment: assignment.id,
        outcome: Outcome::Succeeded {
            result: Evaluation {
                engine: worker().engine,
                search: assignment.request.search.unwrap(),
                best_move: Some("e2e4".into()),
                variations: vec![Variation {
                    multipv: 1,
                    score: Score::Centipawns(30),
                    depth: 12,
                    nodes: 1000,
                    pv: vec!["e2e4".into(), "e7e5".into()],
                }],
                elapsed_ms: 5,
            },
        },
    }
}

#[tokio::test(start_paused = true)]
async fn disconnected_worker_retries_the_same_request_and_rejects_late_results() {
    let master = Master::new(config()).unwrap();
    let disconnected = master.register(worker()).await.unwrap();
    let healthy = master.register(worker()).await.unwrap();
    let response = submit(&master, EvaluateRequest::default()).await;
    let first = master.claim(disconnected.worker_id).await.unwrap().unwrap();
    tokio::time::advance(Duration::from_millis(700)).await;
    master
        .heartbeat(healthy.worker_id, Heartbeat { assignments: vec![] })
        .await
        .unwrap();
    tokio::time::advance(Duration::from_millis(400)).await;
    let retry = master.claim(healthy.worker_id).await.unwrap().unwrap();
    assert_eq!(first.id.job_id, retry.id.job_id);
    assert_ne!(first.id.attempt_id, retry.id.attempt_id);
    assert_eq!(retry.attempt, 2);
    assert_eq!(
        serde_json::to_value(first.request.clone()).unwrap(),
        serde_json::to_value(retry.request.clone()).unwrap()
    );
    assert_eq!(
        master
            .complete(disconnected.worker_id, success(&first))
            .await
            .unwrap_err()
            .status,
        StatusCode::CONFLICT
    );
    master.complete(healthy.worker_id, success(&retry)).await.unwrap();
    let result = response.await.unwrap().unwrap();
    assert_eq!(result.attempts, 2);
    assert_eq!(result.job_id, first.id.job_id);
    assert_eq!(master.status().await.retried, 1);
}

#[tokio::test(start_paused = true)]
async fn worker_errors_retry_immediately_and_duplicate_completions_cannot_finish_another_attempt() {
    let master = Master::new(config()).unwrap();
    let worker = master.register(worker()).await.unwrap();
    let response = submit(&master, EvaluateRequest::default()).await;
    let first = master.claim(worker.worker_id).await.unwrap().unwrap();
    master
        .complete(
            worker.worker_id,
            Completion {
                assignment: first.id,
                outcome: Outcome::Failed {
                    error: "engine process exited".into(),
                },
            },
        )
        .await
        .unwrap();
    let second = master.claim(worker.worker_id).await.unwrap().unwrap();
    assert_eq!(
        master
            .complete(worker.worker_id, success(&first))
            .await
            .unwrap_err()
            .status,
        StatusCode::CONFLICT
    );
    master.complete(worker.worker_id, success(&second)).await.unwrap();
    assert_eq!(response.await.unwrap().unwrap().attempts, 2);
    assert_eq!(
        master
            .complete(worker.worker_id, success(&second))
            .await
            .unwrap_err()
            .status,
        StatusCode::CONFLICT
    );
}

#[tokio::test(start_paused = true)]
async fn only_acknowledged_assignments_are_renewed_by_heartbeats() {
    let master = Master::new(config()).unwrap();
    let worker = master.register(worker()).await.unwrap();
    let response = submit(&master, EvaluateRequest::default()).await;
    let lost_delivery = master.claim(worker.worker_id).await.unwrap().unwrap();
    tokio::time::advance(Duration::from_millis(700)).await;
    master
        .heartbeat(worker.worker_id, Heartbeat { assignments: vec![] })
        .await
        .unwrap();
    tokio::time::advance(Duration::from_millis(400)).await;
    let retry = master.claim(worker.worker_id).await.unwrap().unwrap();
    assert_eq!(retry.attempt, 2);
    master
        .heartbeat(
            worker.worker_id,
            Heartbeat {
                assignments: vec![retry.id],
            },
        )
        .await
        .unwrap();
    for _ in 0..3 {
        tokio::time::advance(Duration::from_millis(700)).await;
        let heartbeat = master
            .heartbeat(
                worker.worker_id,
                Heartbeat {
                    assignments: vec![lost_delivery.id],
                },
            )
            .await
            .unwrap();
        assert_eq!(heartbeat.cancelled, vec![lost_delivery.id]);
        master
            .heartbeat(
                worker.worker_id,
                Heartbeat {
                    assignments: vec![retry.id],
                },
            )
            .await
            .unwrap();
    }
    assert_eq!(master.status().await.running, 1);
    assert_eq!(master.status().await.retried, 1);
    master.complete(worker.worker_id, success(&retry)).await.unwrap();
    assert_eq!(response.await.unwrap().unwrap().attempts, 2);
}

#[tokio::test(start_paused = true)]
async fn repeated_worker_failure_is_bounded_by_the_attempt_limit() {
    let mut config = config();
    config.max_attempts = 2;
    let master = Master::new(config).unwrap();
    let worker = master.register(worker()).await.unwrap();
    let response = submit(&master, EvaluateRequest::default()).await;
    for _ in 0..2 {
        let job = master.claim(worker.worker_id).await.unwrap().unwrap();
        master
            .complete(
                worker.worker_id,
                Completion {
                    assignment: job.id,
                    outcome: Outcome::Failed {
                        error: "engine failed".into(),
                    },
                },
            )
            .await
            .unwrap();
    }
    let error = response.await.unwrap().unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_GATEWAY);
    assert_eq!(error.body.code, "attempts_exhausted");
    let status = master.status().await;
    assert_eq!(status.failed, 1);
    assert_eq!(status.running + status.queued_batch, 0);
}

#[tokio::test(start_paused = true)]
async fn graceful_worker_departure_requeues_without_waiting_for_the_lease() {
    let master = Master::new(config()).unwrap();
    let departing = master.register(worker()).await.unwrap();
    let healthy = master.register(worker()).await.unwrap();
    let response = submit(&master, EvaluateRequest::default()).await;
    let first = master.claim(departing.worker_id).await.unwrap().unwrap();
    master.unregister(departing.worker_id).await.unwrap();
    let retry = master.claim(healthy.worker_id).await.unwrap().unwrap();
    assert_eq!(first.id.job_id, retry.id.job_id);
    master.complete(healthy.worker_id, success(&retry)).await.unwrap();
    assert_eq!(response.await.unwrap().unwrap().attempts, 2);
}

#[tokio::test(start_paused = true)]
async fn queue_capacity_and_worker_capacity_are_enforced() {
    let mut config = config();
    config.queue_capacity = 1;
    let master = Master::new(config).unwrap();
    assert_eq!(
        master.evaluate(EvaluateRequest::default()).await.unwrap_err().status,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let worker = master.register(worker()).await.unwrap();
    let response = submit(&master, EvaluateRequest::default()).await;
    let job = master.claim(worker.worker_id).await.unwrap().unwrap();
    assert_eq!(
        master.claim(worker.worker_id).await.unwrap_err().body.code,
        "worker_full"
    );
    assert_eq!(
        master.evaluate(EvaluateRequest::default()).await.unwrap_err().status,
        StatusCode::TOO_MANY_REQUESTS
    );
    master.complete(worker.worker_id, success(&job)).await.unwrap();
    response.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn interactive_jobs_run_first_without_starving_batch_work() {
    let master = Master::new(config()).unwrap();
    let worker = master.register(worker()).await.unwrap();
    let batch = submit(&master, EvaluateRequest::default()).await;
    let mut interactive = vec![];
    for _ in 0..9 {
        interactive.push(
            submit(
                &master,
                EvaluateRequest {
                    priority: Priority::Interactive,
                    ..Default::default()
                },
            )
            .await,
        );
    }
    for index in 0..10 {
        let assignment = master.claim(worker.worker_id).await.unwrap().unwrap();
        assert_eq!(
            assignment.request.priority,
            if index == 8 {
                Priority::Batch
            } else {
                Priority::Interactive
            }
        );
        master.complete(worker.worker_id, success(&assignment)).await.unwrap();
    }
    batch.await.unwrap().unwrap();
    for task in interactive {
        task.await.unwrap().unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn cancelled_callers_and_master_shutdown_release_waiting_requests() {
    let master = Master::new(config()).unwrap();
    let worker = master.register(worker()).await.unwrap();
    let abandoned = submit(&master, EvaluateRequest::default()).await;
    let job = master.claim(worker.worker_id).await.unwrap().unwrap();
    abandoned.abort();
    let _ = abandoned.await;
    let heartbeat = master
        .heartbeat(
            worker.worker_id,
            Heartbeat {
                assignments: vec![job.id],
            },
        )
        .await
        .unwrap();
    assert_eq!(heartbeat.cancelled, vec![job.id]);
    assert_eq!(master.status().await.running, 0);
    let pending = submit(&master, EvaluateRequest::default()).await;
    master.close().await;
    assert_eq!(
        pending.await.unwrap().unwrap_err().status,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test(start_paused = true)]
async fn retries_cannot_extend_the_original_request_deadline() {
    let mut config = config();
    config.request_timeout_ms = 1000;
    config.search_timeout_ms = 500;
    let master = Master::new(config).unwrap();
    let worker = master.register(worker()).await.unwrap();
    let response = submit(&master, EvaluateRequest::default()).await;
    master.claim(worker.worker_id).await.unwrap().unwrap();
    tokio::time::advance(Duration::from_millis(1001)).await;
    assert_eq!(response.await.unwrap().unwrap_err().status, StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(master.status().await.running, 0);
}

#[tokio::test(start_paused = true)]
async fn engine_requirements_route_only_to_matching_workers() {
    let master = Master::new(config()).unwrap();
    let wrong = master.register(worker()).await.unwrap();
    let request = EvaluateRequest {
        engine: Some("Stockfish required".into()),
        ..Default::default()
    };
    assert_eq!(
        master.evaluate(request.clone()).await.unwrap_err().status,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let mut registration = worker();
    registration.engine.name = "Stockfish required".into();
    let matching = master.register(registration).await.unwrap();
    let response = submit(&master, request).await;
    assert!(master.claim(wrong.worker_id).await.unwrap().is_none());
    let job = master.claim(matching.worker_id).await.unwrap().unwrap();
    let mut completion = success(&job);
    if let Outcome::Succeeded { result } = &mut completion.outcome {
        result.engine.name = "Stockfish required".into();
    }
    master.complete(matching.worker_id, completion).await.unwrap();
    response.await.unwrap().unwrap();
}
