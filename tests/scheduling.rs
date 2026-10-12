use std::time::Duration;

use axum::http::StatusCode;
use stockfish_relay::{config::MasterConfig, master::Master, protocol::*};
use tokio::task::JoinHandle;

fn config() -> MasterConfig {
    MasterConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        api_key: "client-key-for-tests".into(),
        worker_key: "worker-key-for-tests".into(),
        admin_username: None,
        admin_password: None,
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

fn assert_timing(metric: &Option<TimingMetric>, expected: f64) {
    let metric = metric.as_ref().unwrap();
    assert!((metric.average_ms - expected).abs() < 1e-9);
    assert!((metric.p95_ms - expected).abs() < 1e-9);
}

#[tokio::test(start_paused = true)]
async fn job_timings_measure_queue_execution_overhead_and_retries_without_counting_stale_results() {
    let master = Master::new(config()).unwrap();
    let first_worker = master.register(worker(), None).await.unwrap();
    let second_worker = master.register(worker(), None).await.unwrap();
    let response = submit(&master, EvaluateRequest::default()).await;
    tokio::time::advance(Duration::from_millis(10)).await;
    let first = master.claim(first_worker.worker_id).await.unwrap().unwrap();
    tokio::time::advance(Duration::from_millis(20)).await;
    master.complete(first_worker.worker_id, success(&first)).await.unwrap();
    response.await.unwrap().unwrap();
    let status = master.status().await;
    let summary = &status.timings.first_attempt;
    assert_eq!(summary.samples, 1);
    assert_timing(&summary.queue_wait, 10.0);
    assert_timing(&summary.engine, 5.0);
    assert_timing(&summary.overhead, 15.0);
    assert_timing(&summary.total, 30.0);

    let response = submit(&master, EvaluateRequest::default()).await;
    tokio::time::advance(Duration::from_millis(7)).await;
    let failed = master.claim(first_worker.worker_id).await.unwrap().unwrap();
    tokio::time::advance(Duration::from_millis(40)).await;
    master
        .complete(
            first_worker.worker_id,
            Completion {
                assignment: failed.id,
                outcome: Outcome::Failed {
                    error: "engine exited".into(),
                },
            },
        )
        .await
        .unwrap();
    tokio::time::advance(Duration::from_millis(11)).await;
    let retry = master.claim(second_worker.worker_id).await.unwrap().unwrap();
    tokio::time::advance(Duration::from_millis(25)).await;
    assert!(master.complete(first_worker.worker_id, success(&failed)).await.is_err());
    let mut invalid = success(&retry);
    if let Outcome::Succeeded { result } = &mut invalid.outcome {
        result.engine.name = "wrong engine".into();
    }
    assert!(master.complete(second_worker.worker_id, invalid).await.is_err());
    assert_eq!(master.status().await.timings.retried.samples, 0);
    master.complete(second_worker.worker_id, success(&retry)).await.unwrap();
    response.await.unwrap().unwrap();
    assert!(master.complete(second_worker.worker_id, success(&retry)).await.is_err());
    let status = master.status().await;
    assert_eq!(status.timings.first_attempt.samples, 1);
    let summary = &status.timings.retried;
    assert_eq!(summary.samples, 1);
    assert_timing(&summary.queue_wait, 18.0);
    assert_timing(&summary.engine, 5.0);
    assert_timing(&summary.overhead, 20.0);
    assert_timing(&summary.total, 83.0);
    let first_stats = status.workers.iter().find(|w| w.id == first_worker.worker_id).unwrap();
    let second_stats = status.workers.iter().find(|w| w.id == second_worker.worker_id).unwrap();
    assert_eq!(first_stats.timings.first_attempt.samples, 1);
    assert_eq!(first_stats.timings.retried.samples, 0);
    assert_eq!(second_stats.timings.first_attempt.samples, 0);
    assert_eq!(second_stats.timings.retried.samples, 1);

    master.unregister(first_worker.worker_id).await.unwrap();
    let fresh = master.register(worker(), None).await.unwrap();
    let status = master.status().await;
    assert_eq!(status.timings.first_attempt.samples, 1);
    assert_eq!(
        status
            .workers
            .iter()
            .find(|w| w.id == fresh.worker_id)
            .unwrap()
            .timings
            .first_attempt
            .samples,
        0
    );
    assert!(
        Master::new(config())
            .unwrap()
            .status()
            .await
            .timings
            .first_attempt
            .total
            .is_none()
    );
}

#[tokio::test(start_paused = true)]
async fn heartbeat_rtt_is_optional_validated_and_scoped_to_worker_sessions() {
    let master = Master::new(config()).unwrap();
    let session = master.register(worker(), None).await.unwrap();
    let legacy: Heartbeat = serde_json::from_str(r#"{"assignments":[]}"#).unwrap();
    master.heartbeat(session.worker_id, legacy).await.unwrap();
    assert!(master.status().await.workers[0].heartbeat_rtt.is_none());
    for invalid in [-1.0, f64::NAN, f64::INFINITY] {
        assert!(
            master
                .heartbeat(
                    session.worker_id,
                    Heartbeat {
                        round_trip_ms: Some(invalid),
                        ..Heartbeat::default()
                    }
                )
                .await
                .is_err()
        );
    }
    master
        .heartbeat(
            session.worker_id,
            Heartbeat {
                round_trip_ms: Some(12.5),
                ..Heartbeat::default()
            },
        )
        .await
        .unwrap();
    let status = master.status().await;
    assert_eq!(status.workers[0].heartbeat_rtt_samples, 1);
    assert_timing(&status.workers[0].heartbeat_rtt, 12.5);
    master.unregister(session.worker_id).await.unwrap();
    master.register(worker(), None).await.unwrap();
    assert_eq!(master.status().await.workers[0].heartbeat_rtt_samples, 0);
}

#[tokio::test(start_paused = true)]
async fn complete_and_claim_publishes_before_waiting_and_rejects_stale_completions() {
    let master = Master::new(config()).unwrap();
    let session = master.register(worker(), None).await.unwrap();
    assert!(session.complete_and_claim);
    let first = submit(&master, EvaluateRequest::default()).await;
    let assignment = master.claim(session.worker_id).await.unwrap().unwrap();
    let next = {
        let master = master.clone();
        let completion = success(&assignment);
        tokio::spawn(async move { master.complete_and_claim(session.worker_id, completion).await })
    };
    assert_eq!(first.await.unwrap().unwrap().job_id, assignment.id.job_id);
    assert!(!next.is_finished(), "Completed result must not wait for the next job");

    let second = submit(&master, EvaluateRequest::default()).await;
    let second_assignment = next.await.unwrap().unwrap().unwrap();
    assert_ne!(second_assignment.id.job_id, assignment.id.job_id);
    let third = submit(&master, EvaluateRequest::default()).await;
    assert_eq!(
        master
            .complete_and_claim(session.worker_id, success(&assignment))
            .await
            .unwrap_err()
            .status,
        StatusCode::CONFLICT
    );
    assert_eq!(master.status().await.queued_batch, 1);
    let third_assignment = master
        .complete_and_claim(session.worker_id, success(&second_assignment))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.await.unwrap().unwrap().job_id, second_assignment.id.job_id);
    master
        .complete(session.worker_id, success(&third_assignment))
        .await
        .unwrap();
    assert_eq!(third.await.unwrap().unwrap().job_id, third_assignment.id.job_id);
}

#[tokio::test(start_paused = true)]
async fn disconnected_worker_retries_the_same_request_and_rejects_late_results() {
    let master = Master::new(config()).unwrap();
    let disconnected = master.register(worker(), None).await.unwrap();
    let healthy = master.register(worker(), None).await.unwrap();
    let response = submit(&master, EvaluateRequest::default()).await;
    let first = master.claim(disconnected.worker_id).await.unwrap().unwrap();
    tokio::time::advance(Duration::from_millis(700)).await;
    master.heartbeat(healthy.worker_id, Heartbeat::default()).await.unwrap();
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
    let worker = master.register(worker(), None).await.unwrap();
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
    let worker = master.register(worker(), None).await.unwrap();
    let response = submit(&master, EvaluateRequest::default()).await;
    let lost_delivery = master.claim(worker.worker_id).await.unwrap().unwrap();
    tokio::time::advance(Duration::from_millis(700)).await;
    master.heartbeat(worker.worker_id, Heartbeat::default()).await.unwrap();
    tokio::time::advance(Duration::from_millis(400)).await;
    let retry = master.claim(worker.worker_id).await.unwrap().unwrap();
    assert_eq!(retry.attempt, 2);
    master
        .heartbeat(
            worker.worker_id,
            Heartbeat {
                round_trip_ms: None,
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
                    round_trip_ms: None,
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
                    round_trip_ms: None,
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
    let worker = master.register(worker(), None).await.unwrap();
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
    let departing = master.register(worker(), None).await.unwrap();
    let healthy = master.register(worker(), None).await.unwrap();
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
    let worker = master.register(worker(), None).await.unwrap();
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
    let worker = master.register(worker(), None).await.unwrap();
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
    let worker = master.register(worker(), None).await.unwrap();
    let abandoned = submit(&master, EvaluateRequest::default()).await;
    let job = master.claim(worker.worker_id).await.unwrap().unwrap();
    abandoned.abort();
    let _ = abandoned.await;
    let heartbeat = master
        .heartbeat(
            worker.worker_id,
            Heartbeat {
                round_trip_ms: None,
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
    let worker = master.register(worker(), None).await.unwrap();
    let response = submit(&master, EvaluateRequest::default()).await;
    master.claim(worker.worker_id).await.unwrap().unwrap();
    tokio::time::advance(Duration::from_millis(1001)).await;
    assert_eq!(response.await.unwrap().unwrap_err().status, StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(master.status().await.running, 0);
}

#[tokio::test(start_paused = true)]
async fn engine_requirements_route_only_to_matching_workers() {
    let master = Master::new(config()).unwrap();
    let wrong = master.register(worker(), None).await.unwrap();
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
    let matching = master.register(registration, None).await.unwrap();
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
