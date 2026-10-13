use axum::{Json, http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid configuration: {0}")]
    Configuration(String),
    #[error("Stockfish: {0}")]
    Engine(String),
    #[error("Stockfish: No completed exact principal variation")]
    IncompleteEvaluation,
    #[error("Stockfish: Search timed out")]
    SearchTimeout,
    #[error("Worker protocol: {0}")]
    Protocol(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

impl Error {
    pub fn failure_kind(&self) -> FailureKind {
        match self {
            Self::IncompleteEvaluation => FailureKind::IncompleteEvaluation,
            Self::SearchTimeout => FailureKind::SearchTimeout,
            _ => FailureKind::EngineEvaluationFailed,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    // Older workers report only an error string.
    #[default]
    EngineEvaluationFailed,
    IncompleteEvaluation,
    SearchTimeout,
    WorkerUnavailable,
}

impl FailureKind {
    pub fn api_error(self) -> ApiError {
        let (status, code, message) = match self {
            Self::EngineEvaluationFailed => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "engine_evaluation_failed",
                "Stockfish evaluation failed after the configured attempt limit",
            ),
            Self::IncompleteEvaluation => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "incomplete_evaluation",
                "Stockfish returned no completed exact principal variation after the configured attempt limit",
            ),
            Self::SearchTimeout => (
                StatusCode::GATEWAY_TIMEOUT,
                "search_timeout",
                "Stockfish searches timed out after the configured attempt limit",
            ),
            Self::WorkerUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "worker_unavailable",
                "Worker assignments were lost after the configured attempt limit",
            ),
        };
        ApiError::new(status, code, message)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempts: Option<u32>,
}

#[derive(Clone, Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub body: ErrorBody,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &str, message: impl Into<String>) -> Self {
        Self {
            status,
            body: ErrorBody {
                code: code.into(),
                message: message.into(),
                job_id: None,
                attempts: None,
            },
        }
    }

    pub fn with_job(mut self, job_id: Uuid, attempts: u32) -> Self {
        self.body.job_id = Some(job_id);
        self.body.attempts = Some(attempts);
        self
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request", message)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, "unavailable", message)
    }

    pub fn stale() -> Self {
        Self::new(
            StatusCode::CONFLICT,
            "stale_assignment",
            "Assignment is no longer active",
        )
    }

    pub fn unknown_worker() -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "unknown_worker",
            "Worker session expired; register again",
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.status, Json(self.body)).into_response()
    }
}
