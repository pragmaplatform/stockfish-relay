use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Request, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use subtle::ConstantTimeEq;
use utoipa::{
    Modify, OpenApi,
    openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme},
};
use uuid::Uuid;

use crate::{
    error::{ApiError, ErrorBody},
    master::Master,
    protocol::*,
};

pub fn router(master: Master) -> Router {
    let clients = Router::new()
        .route("/v1/evaluate", post(evaluate))
        .route("/v1/status", get(status))
        .route_layer(middleware::from_fn_with_state(
            Arc::new(master.config.api_key.clone()),
            authorize,
        ));
    let workers = Router::new()
        .route("/v1/workers/register", post(register))
        .route("/v1/workers/{worker_id}/claim", post(claim))
        .route("/v1/workers/{worker_id}/heartbeat", post(heartbeat))
        .route("/v1/workers/{worker_id}/complete", post(complete))
        .route("/v1/workers/{worker_id}/complete-and-claim", post(complete_and_claim))
        .route("/v1/workers/{worker_id}", delete(unregister))
        .route_layer(middleware::from_fn_with_state(
            Arc::new(master.config.worker_key.clone()),
            authorize,
        ));
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route("/openapi.json", get(|| async { Json(openapi()) }))
        .merge(clients)
        .merge(workers)
        .merge(crate::admin::router(&master))
        .layer(DefaultBodyLimit::max(256 * 1024))
        .with_state(master)
}

pub fn openapi() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}

#[derive(OpenApi)]
#[openapi(
    info(title = "Stockfish Relay", description = "Stockfish evaluation over HTTP with remote workers. Worker protocol version 1."),
    paths(health, ready, evaluate, status, register, claim, heartbeat, complete, complete_and_claim, unregister),
    components(schemas(ErrorBody)),
    modifiers(&Security)
)]
struct ApiDoc;

struct Security;

impl Modify for Security {
    fn modify(&self, doc: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = doc.components.as_mut() {
            for name in ["client_key", "worker_key"] {
                components.add_security_scheme(
                    name,
                    SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Bearer).build()),
                );
            }
        }
    }
}

#[utoipa::path(get, path = "/healthz", tag = "Health", responses((status = 200, description = "Master process is running")))]
async fn health() -> StatusCode {
    StatusCode::OK
}

#[utoipa::path(get, path = "/readyz", tag = "Health", responses((status = 200, description = "At least one worker is connected"), (status = 503)))]
async fn ready(State(master): State<Master>) -> StatusCode {
    if master.status().await.workers.is_empty() {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

#[utoipa::path(
    post, path = "/v1/evaluate", tag = "Evaluations", request_body = EvaluateRequest,
    security(("client_key" = [])),
    responses(
        (status = 200, body = EvaluateResponse),
        (status = 400, body = ErrorBody),
        (status = 401, body = ErrorBody),
        (status = 429, body = ErrorBody),
        (status = 502, body = ErrorBody),
        (status = 503, body = ErrorBody),
        (status = 504, body = ErrorBody)
    )
)]
async fn evaluate(
    State(master): State<Master>,
    body: Result<Json<EvaluateRequest>, JsonRejection>,
) -> Result<Json<EvaluateResponse>, ApiError> {
    Ok(Json(master.evaluate(payload(body)?).await?))
}

#[utoipa::path(get, path = "/v1/status", tag = "Evaluations", security(("client_key" = [])), responses((status = 200, body = Status), (status = 401, body = ErrorBody)))]
async fn status(State(master): State<Master>) -> Json<Status> {
    Json(master.status().await)
}

#[utoipa::path(post, path = "/v1/workers/register", tag = "Workers", request_body = RegisterWorker, security(("worker_key" = [])), responses((status = 200, body = WorkerSession), (status = 400, body = ErrorBody), (status = 401, body = ErrorBody)))]
async fn register(
    State(master): State<Master>,
    headers: HeaderMap,
    body: Result<Json<RegisterWorker>, JsonRejection>,
) -> Result<Json<WorkerSession>, ApiError> {
    let relay_version = headers
        .get(header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("stockfish-relay/"))
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .map(str::to_owned);
    Ok(Json(master.register(payload(body)?, relay_version).await?))
}

#[utoipa::path(post, path = "/v1/workers/{worker_id}/claim", tag = "Workers", params(("worker_id" = Uuid, Path)), security(("worker_key" = [])), responses((status = 200, body = Assignment), (status = 204, description = "No compatible work within the long-poll interval"), (status = 404, body = ErrorBody), (status = 409, body = ErrorBody)))]
async fn claim(State(master): State<Master>, Path(worker_id): Path<Uuid>) -> Result<Response, ApiError> {
    Ok(match master.claim(worker_id).await? {
        Some(assignment) => Json(assignment).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    })
}

#[utoipa::path(post, path = "/v1/workers/{worker_id}/heartbeat", tag = "Workers", params(("worker_id" = Uuid, Path)), request_body = Heartbeat, security(("worker_key" = [])), responses((status = 200, body = HeartbeatResponse), (status = 404, body = ErrorBody)))]
async fn heartbeat(
    State(master): State<Master>,
    Path(worker_id): Path<Uuid>,
    body: Result<Json<Heartbeat>, JsonRejection>,
) -> Result<Json<HeartbeatResponse>, ApiError> {
    Ok(Json(master.heartbeat(worker_id, payload(body)?).await?))
}

#[utoipa::path(post, path = "/v1/workers/{worker_id}/complete", tag = "Workers", params(("worker_id" = Uuid, Path)), request_body = Completion, security(("worker_key" = [])), responses((status = 204), (status = 400, body = ErrorBody), (status = 409, body = ErrorBody)))]
async fn complete(
    State(master): State<Master>,
    Path(worker_id): Path<Uuid>,
    body: Result<Json<Completion>, JsonRejection>,
) -> Result<StatusCode, ApiError> {
    master.complete(worker_id, payload(body)?).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Publishes the completed result immediately, then long-polls for the next job.
#[utoipa::path(post, path = "/v1/workers/{worker_id}/complete-and-claim", tag = "Workers", params(("worker_id" = Uuid, Path)), request_body = Completion, security(("worker_key" = [])), responses((status = 200, body = Assignment), (status = 204, description = "Result accepted; no compatible work within the long-poll interval"), (status = 400, body = ErrorBody), (status = 404, body = ErrorBody), (status = 409, body = ErrorBody)))]
async fn complete_and_claim(
    State(master): State<Master>,
    Path(worker_id): Path<Uuid>,
    body: Result<Json<Completion>, JsonRejection>,
) -> Result<Response, ApiError> {
    Ok(match master.complete_and_claim(worker_id, payload(body)?).await? {
        Some(assignment) => Json(assignment).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    })
}

#[utoipa::path(delete, path = "/v1/workers/{worker_id}", tag = "Workers", params(("worker_id" = Uuid, Path)), security(("worker_key" = [])), responses((status = 204), (status = 404, body = ErrorBody)))]
async fn unregister(State(master): State<Master>, Path(worker_id): Path<Uuid>) -> Result<StatusCode, ApiError> {
    master.unregister(worker_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn authorize(State(key): State<Arc<String>>, request: Request, next: Next) -> Result<Response, ApiError> {
    let supplied = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if !supplied.is_some_and(|value| bool::from(value.as_bytes().ct_eq(key.as_bytes()))) {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Invalid bearer token",
        ));
    }
    Ok(next.run(request).await)
}

fn payload<T>(body: Result<Json<T>, JsonRejection>) -> Result<T, ApiError> {
    body.map(|Json(value)| value)
        .map_err(|error| ApiError::new(error.status(), "invalid_json", error.body_text()))
}
