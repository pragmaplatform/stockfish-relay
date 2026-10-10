use std::{net::SocketAddr, path::PathBuf};

use clap::{Args, Parser, Subcommand};
use reqwest::Url;

use crate::error::Error;

#[derive(Parser)]
#[command(version, about = "Run Stockfish on remote machines through an HTTP API")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Serve the evaluation API and coordinate remote workers.
    Master(MasterConfig),
    /// Connect to a master and execute jobs using local Stockfish processes.
    #[command(visible_alias = "slave")]
    Worker(WorkerConfig),
    /// Print the OpenAPI document to stdout.
    Openapi,
}

#[derive(Args, Clone)]
pub struct MasterConfig {
    #[arg(long, env = "STOCKFISH_RELAY_LISTEN", default_value = "127.0.0.1:8080")]
    pub listen: SocketAddr,
    #[arg(long, env = "STOCKFISH_RELAY_API_KEY", hide_env_values = true)]
    pub api_key: String,
    #[arg(long, env = "STOCKFISH_RELAY_WORKER_KEY", hide_env_values = true)]
    pub worker_key: String,
    /// Maximum total queued and executing requests.
    #[arg(long, env = "STOCKFISH_RELAY_QUEUE_CAPACITY", default_value_t = 128)]
    pub queue_capacity: usize,
    /// Includes the initial attempt.
    #[arg(long, env = "STOCKFISH_RELAY_MAX_ATTEMPTS", default_value_t = 3)]
    pub max_attempts: u32,
    #[arg(long, env = "STOCKFISH_RELAY_LEASE_MS", default_value_t = 15_000)]
    pub lease_ms: u64,
    #[arg(long, env = "STOCKFISH_RELAY_LONG_POLL_MS", default_value_t = 20_000)]
    pub long_poll_ms: u64,
    /// Total deadline, including queuing and retries.
    #[arg(long, env = "STOCKFISH_RELAY_REQUEST_TIMEOUT_MS", default_value_t = 120_000)]
    pub request_timeout_ms: u64,
    #[arg(long, env = "STOCKFISH_RELAY_SEARCH_TIMEOUT_MS", default_value_t = 30_000)]
    pub search_timeout_ms: u64,
    #[arg(long, env = "STOCKFISH_RELAY_DEFAULT_NODES", default_value_t = 1_000_000)]
    pub default_nodes: u64,
    #[arg(long, env = "STOCKFISH_RELAY_MAX_NODES", default_value_t = 50_000_000)]
    pub max_nodes: u64,
    #[arg(long, env = "STOCKFISH_RELAY_MAX_DEPTH", default_value_t = 40)]
    pub max_depth: u16,
    #[arg(long, env = "STOCKFISH_RELAY_MAX_MULTIPV", default_value_t = 5)]
    pub max_multipv: u16,
}

impl MasterConfig {
    pub fn validate(&self) -> Result<(), Error> {
        valid_key(&self.api_key)?;
        valid_key(&self.worker_key)?;
        if self.api_key == self.worker_key {
            return Err(Error::Configuration("Client and worker keys must differ".into()));
        }
        if self.queue_capacity == 0
            || self.queue_capacity > 100_000
            || !(1..=100).contains(&self.max_attempts)
            || !(100..=300_000).contains(&self.lease_ms)
            || !(1..=60_000).contains(&self.long_poll_ms)
            || !(100..=3_600_000).contains(&self.request_timeout_ms)
            || !(1..=600_000).contains(&self.search_timeout_ms)
            || self.search_timeout_ms > self.request_timeout_ms
            || self.default_nodes == 0
            || self.default_nodes > self.max_nodes
            || !(1..=128).contains(&self.max_depth)
            || !(1..=32).contains(&self.max_multipv)
        {
            return Err(Error::Configuration(
                "Invalid queue, deadline, or search limits; see --help".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Args, Clone)]
pub struct WorkerConfig {
    /// Master origin, including scheme and optional port, e.g. https://engine.example.com:443.
    #[arg(long, env = "STOCKFISH_RELAY_MASTER_URL")]
    pub master: Url,
    #[arg(long, env = "STOCKFISH_RELAY_WORKER_KEY", hide_env_values = true)]
    pub worker_key: String,
    #[arg(long, env = "STOCKFISH_RELAY_WORKER_NAME", default_value = "worker")]
    pub name: String,
    #[arg(long, env = "STOCKFISH_RELAY_CONCURRENCY", default_value_t = 1)]
    pub concurrency: u16,
    #[command(flatten)]
    pub stockfish: StockfishConfig,
}

impl WorkerConfig {
    pub fn validate(&self) -> Result<(), Error> {
        valid_key(&self.worker_key)?;
        if !matches!(self.master.scheme(), "http" | "https")
            || self.master.host_str().is_none()
            || !self.master.username().is_empty()
            || self.master.password().is_some()
            || self.master.query().is_some()
            || self.master.fragment().is_some()
            || self.master.path() != "/"
        {
            return Err(Error::Configuration(
                "Master must be an HTTP(S) origin with no credentials, path, or query".into(),
            ));
        }
        if !(1..=256).contains(&self.concurrency) || self.name.is_empty() || self.name.len() > 128 {
            return Err(Error::Configuration(
                "Worker needs a name and 1–256 engine slots".into(),
            ));
        }
        self.stockfish.validate()
    }
}

#[derive(Args, Clone, Debug)]
pub struct StockfishConfig {
    #[arg(long, env = "STOCKFISH_PATH", default_value = "stockfish")]
    pub stockfish_path: PathBuf,
    #[arg(long, env = "STOCKFISH_THREADS", default_value_t = 1)]
    pub threads: u16,
    #[arg(long, env = "STOCKFISH_HASH_MB", default_value_t = 64)]
    pub hash_mb: u32,
}

impl StockfishConfig {
    pub fn validate(&self) -> Result<(), Error> {
        if !(1..=1024).contains(&self.threads) || !(1..=1_048_576).contains(&self.hash_mb) {
            return Err(Error::Configuration("Invalid Stockfish thread or hash setting".into()));
        }
        Ok(())
    }
}

fn valid_key(key: &str) -> Result<(), Error> {
    if key.len() < 16 || !key.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(Error::Configuration(
            "Authentication keys need at least 16 printable non-space ASCII characters".into(),
        ));
    }
    Ok(())
}
