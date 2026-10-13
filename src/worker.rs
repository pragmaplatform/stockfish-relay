use std::{collections::HashMap, sync::Arc, time::Duration};

use reqwest::{
    Client, StatusCode,
    header::{AUTHORIZATION, HeaderMap, HeaderValue},
};
use tokio::{sync::Mutex, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::{config::WorkerConfig, engine::Stockfish, error::Error, protocol::*};

#[derive(Clone)]
struct Session {
    client: Client,
    config: Arc<WorkerConfig>,
    registration: WorkerSession,
    engine: EngineInfo,
    active: Arc<Mutex<HashMap<AssignmentId, CancellationToken>>>,
    stop: CancellationToken,
}

pub async fn run(config: WorkerConfig, shutdown: CancellationToken) -> Result<(), Error> {
    config.validate()?;
    tracing::info!(version = env!("CARGO_PKG_VERSION"), name = %config.name, "Stockfish Relay worker starting");
    let mut headers = HeaderMap::new();
    let mut authorization = HeaderValue::from_str(&format!("Bearer {}", config.worker_key))
        .map_err(|_| Error::Configuration("Invalid worker key".into()))?;
    authorization.set_sensitive(true);
    headers.insert(AUTHORIZATION, authorization);
    let client = Client::builder()
        .default_headers(headers)
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .user_agent(concat!("stockfish-relay/", env!("CARGO_PKG_VERSION")))
        .build()?;
    let config = Arc::new(config);
    let mut backoff_ms = 1000;
    let mut initial = None;
    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }
        if initial.is_none() {
            initial = Some(tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                result = Stockfish::spawn(&config.stockfish) => result?,
            });
        }
        let engine = initial
            .as_ref()
            .ok_or_else(|| Error::Engine("Missing initial process".into()))?
            .info
            .clone();
        let request = RegisterWorker {
            protocol_version: PROTOCOL_VERSION,
            name: config.name.clone(),
            capacity: config.concurrency,
            engine: engine.clone(),
        };
        let registration = tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            result = register(&client, &config, &request) => result,
        };
        let result = match registration {
            Ok(registration) => {
                backoff_ms = 1000;
                tracing::info!(worker_id = %registration.worker_id, capacity = config.concurrency, engine = %engine.name, "Connected to master");
                let session = Session {
                    client: client.clone(),
                    config: Arc::clone(&config),
                    registration,
                    engine,
                    active: Arc::new(Mutex::new(HashMap::new())),
                    stop: shutdown.child_token(),
                };
                run_session(session, initial.take()).await
            }
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            if matches!(&error, Error::Http(error) if error.status().is_some_and(|status| matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN | StatusCode::BAD_REQUEST)))
            {
                return Err(error);
            }
            tracing::warn!(%error, "Worker disconnected; reconnecting");
        }
        let jitter = u64::from(uuid::Uuid::new_v4().as_bytes()[0]);
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_millis(backoff_ms + jitter)) => {},
        }
        backoff_ms = (backoff_ms * 2).min(30_000);
    }
}

async fn register(client: &Client, config: &WorkerConfig, request: &RegisterWorker) -> Result<WorkerSession, Error> {
    let registration: WorkerSession = client
        .post(format!("{}v1/workers/register", config.master))
        .json(request)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if registration.heartbeat_ms == 0
        || registration.heartbeat_ms >= registration.lease_ms
        || registration.long_poll_ms > 60_000
        || registration.lease_ms > 300_000
    {
        return Err(Error::Protocol("Invalid master timing configuration".into()));
    }
    Ok(registration)
}

async fn run_session(session: Session, initial: Option<Stockfish>) -> Result<(), Error> {
    let mut tasks = JoinSet::new();
    let heartbeats = session.clone();
    tasks.spawn(async move { heartbeats.heartbeat_loop().await });
    let mut initial = initial;
    for slot in 0..session.config.concurrency {
        let worker = session.clone();
        let engine = if slot == 0 { initial.take() } else { None };
        tasks.spawn(async move { worker.slot(engine).await });
    }
    let result = tokio::select! {
        _ = session.stop.cancelled() => Ok(()),
        result = tasks.join_next() => match result {
            Some(Ok(result)) => result,
            Some(Err(error)) => Err(Error::Protocol(error.to_string())),
            None => Ok(()),
        },
    };
    session.stop.cancel();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::debug!(%error, "Stopping worker slot"),
            Err(error) => tracing::error!(%error, "Worker task failed"),
        }
    }
    // Best effort: the lease also expires if the master cannot be reached.
    match session
        .client
        .delete(format!(
            "{}v1/workers/{}",
            session.config.master, session.registration.worker_id
        ))
        .timeout(Duration::from_secs(2))
        .send()
        .await
    {
        Ok(response) if response.status().is_success() || response.status() == StatusCode::NOT_FOUND => {}
        Ok(response) => {
            tracing::debug!(status = %response.status(), "Could not release worker session; leases will expire")
        }
        Err(error) => tracing::debug!(%error, "Could not release worker session; leases will expire"),
    }
    result
}

impl Session {
    fn endpoint(&self, action: &str) -> String {
        format!(
            "{}v1/workers/{}/{action}",
            self.config.master, self.registration.worker_id
        )
    }

    fn network_timeout(&self) -> Duration {
        Duration::from_millis(self.registration.heartbeat_ms.min(5000))
    }

    async fn heartbeat_loop(&self) -> Result<(), Error> {
        let mut tick = tokio::time::interval(Duration::from_millis(self.registration.heartbeat_ms));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut round_trip_ms = None;
        loop {
            tokio::select! {
                _ = self.stop.cancelled() => return Ok(()),
                _ = tick.tick() => {},
            }
            let assignments = self.active.lock().await.keys().copied().collect();
            let started = tokio::time::Instant::now();
            let response = tokio::select! {
                _ = self.stop.cancelled() => return Ok(()),
                response = self.client.post(self.endpoint("heartbeat")).timeout(self.network_timeout())
                    .json(&Heartbeat { assignments, round_trip_ms }).send() => response?,
            };
            let heartbeat: HeartbeatResponse = response.error_for_status()?.json().await?;
            round_trip_ms = Some(started.elapsed().as_secs_f64() * 1000.0);
            let active = self.active.lock().await;
            for id in heartbeat.cancelled {
                if let Some(stop) = active.get(&id) {
                    stop.cancel();
                }
            }
        }
    }

    async fn slot(&self, mut engine: Option<Stockfish>) -> Result<(), Error> {
        let result = self.slot_loop(&mut engine).await;
        if let Some(engine) = engine.as_mut() {
            engine.terminate().await?;
        }
        result
    }

    async fn slot_loop(&self, engine: &mut Option<Stockfish>) -> Result<(), Error> {
        let mut next_assignment: Option<Assignment> = None;
        loop {
            let assignment = if let Some(assignment) = next_assignment.take() {
                assignment
            } else {
                let response = tokio::select! {
                    _ = self.stop.cancelled() => return Ok(()),
                    response = self.client.post(self.endpoint("claim"))
                        .timeout(Duration::from_millis(self.registration.long_poll_ms + 5000)).send() => response?,
                };
                if response.status() == StatusCode::NO_CONTENT {
                    continue;
                }
                response.error_for_status()?.json().await?
            };
            let stop = self.stop.child_token();
            self.active.lock().await.insert(assignment.id, stop.clone());
            let searched = tokio::select! {
                _ = stop.cancelled() => None,
                result = tokio::time::timeout(Duration::from_millis(assignment.search_timeout_ms), async {
                    if engine.is_none() {
                        *engine = Some(Stockfish::spawn(&self.config.stockfish).await?);
                    }
                    let engine = engine.as_mut().ok_or_else(|| Error::Engine("Process unavailable".into()))?;
                    if engine.info != self.engine {
                        return Err(Error::Engine("Engine configuration changed since registration".into()));
                    }
                    engine.evaluate(&assignment.request, assignment.search_timeout_ms)
                        .instrument(tracing::info_span!("evaluation",
                            worker_id = %self.registration.worker_id,
                            job_id = %assignment.id.job_id, attempt_id = %assignment.id.attempt_id,
                            attempt = assignment.attempt, timeout_ms = assignment.search_timeout_ms))
                        .await
                }) => Some(match result {
                    Ok(result) => result,
                    Err(_) => Err(Error::SearchTimeout),
                }),
            };
            if !matches!(searched, Some(Ok(_)))
                && let Some(mut stopped) = engine.take()
            {
                stopped.terminate().await?;
            }
            let Some(searched) = searched else {
                self.active.lock().await.remove(&assignment.id);
                continue;
            };
            let outcome = match searched {
                Ok(result) => Outcome::Succeeded { result },
                Err(error) => {
                    tracing::warn!(worker_id = %self.registration.worker_id,
                        job_id = %assignment.id.job_id, attempt_id = %assignment.id.attempt_id,
                        attempt = assignment.attempt, kind = ?error.failure_kind(), %error,
                        engine = %self.engine.name, threads = self.engine.threads, hash_mb = self.engine.hash_mb,
                        search = ?assignment.request.search, multipv = assignment.request.multipv,
                        timeout_ms = assignment.search_timeout_ms, "Stockfish job failed");
                    Outcome::Failed {
                        kind: error.failure_kind(),
                        error: error.to_string().chars().take(256).collect(),
                    }
                }
            };
            let completion = Completion {
                assignment: assignment.id,
                outcome,
            };
            let (action, timeout) = if self.registration.complete_and_claim {
                (
                    "complete-and-claim",
                    Duration::from_millis(self.registration.long_poll_ms + 5000),
                )
            } else {
                ("complete", self.network_timeout())
            };
            // The completed assignment must not remain in heartbeats during the next long-poll.
            self.active.lock().await.remove(&assignment.id);
            let response = tokio::select! {
                _ = self.stop.cancelled() => return Ok(()),
                response = self.client.post(self.endpoint(action)).timeout(timeout)
                    .json(&completion).send() => response?,
            };
            if response.status() == StatusCode::CONFLICT {
                continue;
            }
            let response = response.error_for_status()?;
            if self.registration.complete_and_claim && response.status() != StatusCode::NO_CONTENT {
                next_assignment = Some(response.json().await?);
            }
        }
    }
}
