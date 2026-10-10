use std::collections::HashSet;

use cozy_chess::{Board, util};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{config::MasterConfig, error::ApiError};

pub const PROTOCOL_VERSION: u16 = 1;

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct EvaluateRequest {
    /// Starting position; omitted means the standard initial position.
    pub fen: Option<String>,
    /// Legal UCI moves from the starting position, preserving repetition history.
    #[serde(default)]
    pub moves: Vec<String>,
    /// Exactly one search limit. Omitted uses the master's default node budget.
    pub search: Option<SearchLimit>,
    #[serde(default)]
    pub search_moves: Vec<String>,
    #[serde(default = "one")]
    pub multipv: u16,
    #[serde(default)]
    pub priority: Priority,
    /// Optionally require an exact UCI engine name.
    pub engine: Option<String>,
}

impl Default for EvaluateRequest {
    fn default() -> Self {
        Self {
            fen: None,
            moves: vec![],
            search: None,
            search_moves: vec![],
            multipv: 1,
            priority: Priority::Batch,
            engine: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SearchLimit {
    Nodes(u64),
    Depth(u16),
    MovetimeMs(u64),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Interactive,
    #[default]
    Batch,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
pub struct EngineInfo {
    pub name: String,
    pub threads: u16,
    pub hash_mb: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Score {
    Centipawns(i32),
    Mate(i32),
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct Variation {
    pub multipv: u16,
    pub score: Score,
    pub depth: u16,
    pub nodes: u64,
    pub pv: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct Evaluation {
    pub engine: EngineInfo,
    pub search: SearchLimit,
    /// Null for a terminal position.
    pub best_move: Option<String>,
    /// Scores are from the evaluated position's side-to-move perspective.
    pub variations: Vec<Variation>,
    pub elapsed_ms: u64,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct EvaluateResponse {
    pub job_id: Uuid,
    pub attempts: u32,
    pub result: Evaluation,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RegisterWorker {
    pub protocol_version: u16,
    pub name: String,
    pub capacity: u16,
    pub engine: EngineInfo,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct WorkerSession {
    pub worker_id: Uuid,
    pub lease_ms: u64,
    pub heartbeat_ms: u64,
    pub long_poll_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Deserialize, Serialize, ToSchema)]
pub struct AssignmentId {
    pub job_id: Uuid,
    pub attempt_id: Uuid,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct Assignment {
    pub id: AssignmentId,
    pub attempt: u32,
    pub request: EvaluateRequest,
    pub search_timeout_ms: u64,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct Heartbeat {
    pub assignments: Vec<AssignmentId>,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct HeartbeatResponse {
    pub cancelled: Vec<AssignmentId>,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Outcome {
    Succeeded { result: Evaluation },
    Failed { error: String },
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct Completion {
    pub assignment: AssignmentId,
    pub outcome: Outcome,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct Status {
    /// Elapsed seconds since the master started; resets on restart.
    pub uptime_seconds: u64,
    pub queued_interactive: usize,
    pub queued_batch: usize,
    pub running: usize,
    pub completed: u64,
    pub retried: u64,
    pub failed: u64,
    pub workers: Vec<WorkerStatus>,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct WorkerStatus {
    pub id: Uuid,
    pub name: String,
    /// Elapsed seconds since this worker registered; resets on reconnection.
    pub uptime_seconds: u64,
    pub capacity: u16,
    pub running: usize,
    pub engine: EngineInfo,
}

impl EvaluateRequest {
    pub fn normalize(mut self, config: &MasterConfig) -> Result<Self, ApiError> {
        if self.moves.len() > 1024
            || self.search_moves.len() > 256
            || self.fen.as_ref().is_some_and(|fen| fen.len() > 128)
        {
            return Err(ApiError::invalid("Position or move list is too large"));
        }
        if self.multipv == 0 || self.multipv > config.max_multipv {
            return Err(ApiError::invalid("multipv is outside configured limits"));
        }
        if self
            .engine
            .as_ref()
            .is_some_and(|name| name.is_empty() || name.len() > 128)
        {
            return Err(ApiError::invalid("Invalid engine name"));
        }
        let limit = self.search.unwrap_or(SearchLimit::Nodes(config.default_nodes));
        let valid = match limit {
            SearchLimit::Nodes(nodes) => nodes > 0 && nodes <= config.max_nodes,
            SearchLimit::Depth(depth) => depth > 0 && depth <= config.max_depth,
            SearchLimit::MovetimeMs(ms) => ms > 0 && ms < config.search_timeout_ms,
        };
        if !valid {
            return Err(ApiError::invalid("Search limit is outside configured limits"));
        }
        self.search = Some(limit);
        let mut board = self.starting_board()?;
        if self.fen.is_some() {
            self.fen = Some(board.to_string());
        }
        for uci in &mut self.moves {
            let mv = legal_move(&board, uci)?;
            *uci = util::display_uci_move(&board, mv).to_string();
            board.play(mv);
        }
        for uci in &mut self.search_moves {
            let mv = legal_move(&board, uci)?;
            *uci = util::display_uci_move(&board, mv).to_string();
        }
        self.search_moves.sort();
        self.search_moves.dedup();
        Ok(self)
    }

    pub fn starting_board(&self) -> Result<Board, ApiError> {
        match &self.fen {
            Some(fen) => Board::from_fen(fen, false).map_err(|_| ApiError::invalid("Invalid standard-chess FEN")),
            None => Ok(Board::default()),
        }
    }

    pub fn board(&self) -> Result<Board, ApiError> {
        let mut board = self.starting_board()?;
        for uci in &self.moves {
            board.play(legal_move(&board, uci)?);
        }
        Ok(board)
    }
}

pub fn legal_move(board: &Board, uci: &str) -> Result<cozy_chess::Move, ApiError> {
    if !(4..=5).contains(&uci.len()) || !uci.is_ascii() {
        return Err(ApiError::invalid("Invalid UCI move"));
    }
    let mv = util::parse_uci_move(board, uci).map_err(|_| ApiError::invalid("Invalid UCI move"))?;
    if !board.is_legal(mv) {
        return Err(ApiError::invalid(format!("Illegal move: {uci}")));
    }
    Ok(mv)
}

fn one() -> u16 {
    1
}

impl Evaluation {
    pub fn validate(&self, request: &EvaluateRequest, engine: &EngineInfo) -> Result<(), ApiError> {
        if &self.engine != engine || request.search != Some(self.search) {
            return Err(ApiError::invalid(
                "Result engine or search settings do not match the assignment",
            ));
        }
        let board = request.board()?;
        let terminal_score = match board.status() {
            cozy_chess::GameStatus::Won => Some(Score::Mate(0)),
            cozy_chess::GameStatus::Drawn => Some(Score::Centipawns(0)),
            cozy_chess::GameStatus::Ongoing => None,
        };
        if let Some(score) = terminal_score {
            if self.best_move.is_none()
                && self.variations.len() == 1
                && self
                    .variations
                    .first()
                    .is_some_and(|v| v.score == score && v.pv.is_empty())
            {
                return Ok(());
            }
            return Err(ApiError::invalid("Invalid terminal result"));
        }
        if self.variations.is_empty()
            || self.variations.len() > usize::from(request.multipv)
            || self.best_move.as_ref() != self.variations.first().and_then(|v| v.pv.first())
        {
            return Err(ApiError::invalid("Invalid best move or variation count"));
        }
        let mut roots = HashSet::new();
        for (index, variation) in self.variations.iter().enumerate() {
            if usize::from(variation.multipv) != index + 1 || variation.pv.is_empty() || variation.pv.len() > 256 {
                return Err(ApiError::invalid("Invalid principal variation"));
            }
            let root = variation
                .pv
                .first()
                .ok_or_else(|| ApiError::invalid("Missing principal variation root"))?;
            if !roots.insert(legal_move(&board, root)?) {
                return Err(ApiError::invalid("Duplicate principal variation root"));
            }
            if !request.search_moves.is_empty() && !request.search_moves.contains(root) {
                return Err(ApiError::invalid("Result ignored the search move restriction"));
            }
            let mut continuation = board.clone();
            for uci in &variation.pv {
                continuation.play(legal_move(&continuation, uci)?);
            }
        }
        Ok(())
    }
}
