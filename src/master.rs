use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use axum::http::StatusCode;
use tokio::{
    net::TcpListener,
    sync::{Mutex, Notify, oneshot},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    config::MasterConfig,
    error::{ApiError, Error},
    protocol::*,
};

#[derive(Clone)]
pub struct Master {
    pub config: Arc<MasterConfig>,
    started_at: Instant,
    state: Arc<Mutex<State>>,
    changed: Arc<Notify>,
}

#[derive(Default)]
struct State {
    jobs: HashMap<Uuid, Job>,
    interactive: VecDeque<Uuid>,
    batch: VecDeque<Uuid>,
    workers: HashMap<Uuid, Worker>,
    interactive_streak: u8,
    completed: u64,
    retried: u64,
    failed: u64,
    closed: bool,
}

struct Job {
    request: EvaluateRequest,
    reply: oneshot::Sender<Result<EvaluateResponse, ApiError>>,
    deadline: Instant,
    attempts: u32,
    lease: Option<Lease>,
}

struct Lease {
    id: AssignmentId,
    worker_id: Uuid,
    expires: Instant,
    deadline: Instant,
}

struct Worker {
    registration: RegisterWorker,
    connected_at: Instant,
    seen: Instant,
}

pub async fn serve(listener: TcpListener, config: MasterConfig, shutdown: CancellationToken) -> Result<(), Error> {
    let master = Master::new(config)?;
    let cleanup = master.clone();
    let stop = shutdown.child_token();
    let maintenance_stop = stop.clone();
    let maintenance = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis((cleanup.config.lease_ms / 4).clamp(10, 1000)));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = maintenance_stop.cancelled() => break,
                _ = tick.tick() => {
                    cleanup.state.lock().await.maintain(&cleanup.config);
                    cleanup.changed.notify_waiters();
                }
            }
        }
    });
    let closing = master.clone();
    let result = axum::serve(listener, crate::http::router(master.clone()))
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
            closing.close().await;
        })
        .await;
    stop.cancel();
    master.close().await;
    maintenance.await.map_err(|error| Error::Protocol(error.to_string()))?;
    result.map_err(Error::from)
}

impl Master {
    pub fn new(config: MasterConfig) -> Result<Self, Error> {
        config.validate()?;
        Ok(Self {
            config: Arc::new(config),
            started_at: Instant::now(),
            state: Arc::new(Mutex::new(State::default())),
            changed: Arc::new(Notify::new()),
        })
    }

    pub async fn evaluate(&self, request: EvaluateRequest) -> Result<EvaluateResponse, ApiError> {
        let request = request.normalize(&self.config)?;
        let deadline = Instant::now() + Duration::from_millis(self.config.request_timeout_ms);
        let (sender, receiver) = oneshot::channel();
        let job_id = Uuid::new_v4();
        {
            let mut state = self.state.lock().await;
            state.maintain(&self.config);
            if state.closed || !state.workers.values().any(|worker| compatible(&request, worker)) {
                return Err(ApiError::unavailable("No compatible worker is connected"));
            }
            if state.jobs.len() >= self.config.queue_capacity {
                return Err(ApiError::new(
                    StatusCode::TOO_MANY_REQUESTS,
                    "queue_full",
                    "Evaluation queue is full",
                ));
            }
            state.enqueue(job_id, request.priority);
            state.jobs.insert(
                job_id,
                Job {
                    request,
                    reply: sender,
                    deadline,
                    attempts: 0,
                    lease: None,
                },
            );
        }
        self.changed.notify_waiters();
        let result = match tokio::time::timeout_at(deadline, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ApiError::unavailable("Master stopped")),
            Err(_) => Err(request_timeout()),
        };
        let mut state = self.state.lock().await;
        if state.jobs.remove(&job_id).is_some() {
            state.failed += 1;
        }
        state.prune_queues();
        drop(state);
        self.changed.notify_waiters();
        result
    }

    pub async fn register(&self, registration: RegisterWorker) -> Result<WorkerSession, ApiError> {
        if registration.protocol_version != PROTOCOL_VERSION {
            return Err(ApiError::invalid("Unsupported worker protocol version"));
        }
        if registration.name.is_empty()
            || registration.name.len() > 128
            || !(1..=256).contains(&registration.capacity)
            || registration.engine.name.is_empty()
            || registration.engine.name.len() > 128
            || registration.engine.threads == 0
            || registration.engine.hash_mb == 0
        {
            return Err(ApiError::invalid("Invalid worker registration"));
        }
        let mut state = self.state.lock().await;
        state.maintain(&self.config);
        if state.closed {
            return Err(ApiError::unavailable("Master is shutting down"));
        }
        let worker_id = Uuid::new_v4();
        let connected_at = Instant::now();
        tracing::info!(%worker_id, name = %registration.name, capacity = registration.capacity, engine = %registration.engine.name, "Worker registered");
        state.workers.insert(
            worker_id,
            Worker {
                registration,
                connected_at,
                seen: connected_at,
            },
        );
        drop(state);
        self.changed.notify_waiters();
        Ok(WorkerSession {
            worker_id,
            lease_ms: self.config.lease_ms,
            heartbeat_ms: self.config.lease_ms / 3,
            long_poll_ms: self.config.long_poll_ms,
        })
    }

    pub async fn claim(&self, worker_id: Uuid) -> Result<Option<Assignment>, ApiError> {
        let until = Instant::now() + Duration::from_millis(self.config.long_poll_ms);
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.state.lock().await;
                state.maintain(&self.config);
                if state.closed {
                    return Err(ApiError::unavailable("Master is shutting down"));
                }
                let worker = state.workers.get(&worker_id).ok_or_else(ApiError::unknown_worker)?;
                let busy = state.running(worker_id);
                if busy >= usize::from(worker.registration.capacity) {
                    return Err(ApiError::new(
                        StatusCode::CONFLICT,
                        "worker_full",
                        "All worker slots are assigned",
                    ));
                }
                let interactive = state
                    .interactive
                    .iter()
                    .position(|id| state.jobs.get(id).is_some_and(|job| compatible(&job.request, worker)));
                let batch = state
                    .batch
                    .iter()
                    .position(|id| state.jobs.get(id).is_some_and(|job| compatible(&job.request, worker)));
                let selected =
                    if let Some(index) = interactive.filter(|_| state.interactive_streak < 8 || batch.is_none()) {
                        state.interactive_streak = state.interactive_streak.saturating_add(1);
                        state.interactive.remove(index)
                    } else if let Some(index) = batch {
                        state.interactive_streak = 0;
                        state.batch.remove(index)
                    } else {
                        None
                    };
                if let Some(job_id) = selected {
                    let job = state.jobs.get_mut(&job_id).ok_or_else(ApiError::stale)?;
                    let now = Instant::now();
                    let id = AssignmentId {
                        job_id,
                        attempt_id: Uuid::new_v4(),
                    };
                    job.attempts += 1;
                    job.lease = Some(Lease {
                        id,
                        worker_id,
                        expires: now + Duration::from_millis(self.config.lease_ms),
                        deadline: (now + Duration::from_millis(self.config.search_timeout_ms + self.config.lease_ms))
                            .min(job.deadline),
                    });
                    return Ok(Some(Assignment {
                        id,
                        attempt: job.attempts,
                        request: job.request.clone(),
                        search_timeout_ms: self.config.search_timeout_ms,
                    }));
                }
            }
            if tokio::time::timeout_at(until, notified).await.is_err() {
                return Ok(None);
            }
        }
    }

    pub async fn heartbeat(&self, worker_id: Uuid, heartbeat: Heartbeat) -> Result<HeartbeatResponse, ApiError> {
        let mut state = self.state.lock().await;
        state.maintain(&self.config);
        let worker = state.workers.get_mut(&worker_id).ok_or_else(ApiError::unknown_worker)?;
        if heartbeat.assignments.len() > usize::from(worker.registration.capacity) {
            return Err(ApiError::invalid("Heartbeat exceeds worker capacity"));
        }
        let now = Instant::now();
        worker.seen = now;
        let mut cancelled = Vec::new();
        for id in heartbeat.assignments {
            if let Some(lease) = state.jobs.get_mut(&id.job_id).and_then(|job| job.lease.as_mut())
                && lease.id == id
                && lease.worker_id == worker_id
            {
                lease.expires = (now + Duration::from_millis(self.config.lease_ms)).min(lease.deadline);
            } else {
                cancelled.push(id);
            }
        }
        Ok(HeartbeatResponse { cancelled })
    }

    pub async fn complete(&self, worker_id: Uuid, completion: Completion) -> Result<(), ApiError> {
        let mut state = self.state.lock().await;
        state.maintain(&self.config);
        let job = state
            .jobs
            .get(&completion.assignment.job_id)
            .ok_or_else(ApiError::stale)?;
        if !job
            .lease
            .as_ref()
            .is_some_and(|lease| lease.id == completion.assignment && lease.worker_id == worker_id)
        {
            return Err(ApiError::stale());
        }
        match completion.outcome {
            Outcome::Succeeded { result } => {
                let worker = state.workers.get(&worker_id).ok_or_else(ApiError::unknown_worker)?;
                result.validate(&job.request, &worker.registration.engine)?;
                let id = completion.assignment.job_id;
                let job = state.jobs.remove(&id).ok_or_else(ApiError::stale)?;
                state.completed += 1;
                let _ = job.reply.send(Ok(EvaluateResponse {
                    job_id: id,
                    attempts: job.attempts,
                    result,
                }));
            }
            Outcome::Failed { error } => {
                if error.len() > 1024 {
                    return Err(ApiError::invalid("Worker error is too long"));
                }
                state.retry(completion.assignment.job_id, &self.config, &error);
            }
        }
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    pub async fn unregister(&self, worker_id: Uuid) -> Result<(), ApiError> {
        let mut state = self.state.lock().await;
        if state.workers.remove(&worker_id).is_none() {
            return Err(ApiError::unknown_worker());
        }
        let jobs: Vec<_> = state
            .jobs
            .iter()
            .filter_map(|(id, job)| {
                job.lease
                    .as_ref()
                    .filter(|lease| lease.worker_id == worker_id)
                    .map(|_| *id)
            })
            .collect();
        for id in jobs {
            state.retry(id, &self.config, "Worker disconnected");
        }
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    pub async fn status(&self) -> Status {
        let mut state = self.state.lock().await;
        state.maintain(&self.config);
        Status {
            uptime_seconds: self.started_at.elapsed().as_secs(),
            queued_interactive: state.interactive.len(),
            queued_batch: state.batch.len(),
            running: state.jobs.values().filter(|job| job.lease.is_some()).count(),
            completed: state.completed,
            retried: state.retried,
            failed: state.failed,
            workers: state
                .workers
                .iter()
                .map(|(id, worker)| WorkerStatus {
                    id: *id,
                    name: worker.registration.name.clone(),
                    uptime_seconds: worker.connected_at.elapsed().as_secs(),
                    capacity: worker.registration.capacity,
                    running: state.running(*id),
                    engine: worker.registration.engine.clone(),
                })
                .collect(),
        }
    }

    pub async fn close(&self) {
        let mut state = self.state.lock().await;
        state.closed = true;
        for (_, job) in state.jobs.drain() {
            let _ = job
                .reply
                .send(Err(ApiError::unavailable("Master is shutting down; retry the request")));
        }
        state.workers.clear();
        state.interactive.clear();
        state.batch.clear();
        drop(state);
        self.changed.notify_waiters();
    }
}

impl State {
    fn enqueue(&mut self, id: Uuid, priority: Priority) {
        match priority {
            Priority::Interactive => self.interactive.push_back(id),
            Priority::Batch => self.batch.push_back(id),
        }
    }

    fn running(&self, worker_id: Uuid) -> usize {
        self.jobs
            .values()
            .filter(|job| job.lease.as_ref().is_some_and(|lease| lease.worker_id == worker_id))
            .count()
    }

    fn maintain(&mut self, config: &MasterConfig) {
        let now = Instant::now();
        let expired: Vec<_> = self
            .jobs
            .iter()
            .filter_map(|(id, job)| (job.deadline <= now || job.reply.is_closed()).then_some(*id))
            .collect();
        for id in expired {
            if let Some(job) = self.jobs.remove(&id) {
                self.failed += 1;
                let _ = job.reply.send(Err(request_timeout()));
            }
        }
        self.workers
            .retain(|_, worker| worker.seen + Duration::from_millis(config.lease_ms) > now);
        let abandoned: Vec<_> = self
            .jobs
            .iter()
            .filter_map(|(id, job)| {
                job.lease
                    .as_ref()
                    .filter(|lease| {
                        lease.expires <= now || lease.deadline <= now || !self.workers.contains_key(&lease.worker_id)
                    })
                    .map(|_| *id)
            })
            .collect();
        for id in abandoned {
            self.retry(id, config, "Worker assignment expired");
        }
        self.prune_queues();
    }

    fn retry(&mut self, id: Uuid, config: &MasterConfig, reason: &str) {
        let Some(job) = self.jobs.get_mut(&id) else { return };
        if job.attempts >= config.max_attempts {
            if let Some(job) = self.jobs.remove(&id) {
                self.failed += 1;
                tracing::warn!(job_id = %id, attempts = job.attempts, %reason, "Evaluation exhausted retries");
                let _ = job.reply.send(Err(ApiError::new(
                    StatusCode::BAD_GATEWAY,
                    "attempts_exhausted",
                    "Stockfish work failed after the configured attempt limit",
                )));
            }
        } else {
            job.lease = None;
            let priority = job.request.priority;
            tracing::warn!(job_id = %id, attempts = job.attempts, %reason, "Requeuing evaluation");
            self.retried += 1;
            self.enqueue(id, priority);
        }
    }

    fn prune_queues(&mut self) {
        self.interactive.retain(|id| self.jobs.contains_key(id));
        self.batch.retain(|id| self.jobs.contains_key(id));
    }
}

fn compatible(request: &EvaluateRequest, worker: &Worker) -> bool {
    request
        .engine
        .as_ref()
        .is_none_or(|name| *name == worker.registration.engine.name)
}

fn request_timeout() -> ApiError {
    ApiError::new(
        StatusCode::GATEWAY_TIMEOUT,
        "request_timeout",
        "Evaluation deadline exceeded; retry the request",
    )
}
