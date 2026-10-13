use std::{
    collections::{BTreeMap, VecDeque},
    process::Stdio,
    time::Duration,
};

use cozy_chess::{GameStatus, util};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout},
    time::{Instant, timeout},
};

use crate::{config::StockfishConfig, error::Error, protocol::*};

pub struct Stockfish {
    child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
    pub info: EngineInfo,
}

impl Stockfish {
    pub async fn spawn(config: &StockfishConfig) -> Result<Self, Error> {
        config.validate()?;
        let mut child = tokio::process::Command::new(&config.stockfish_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        let input = child
            .stdin
            .take()
            .ok_or_else(|| Error::Engine("Missing stdin".into()))?;
        let output = child
            .stdout
            .take()
            .ok_or_else(|| Error::Engine("Missing stdout".into()))?;
        let mut engine = Self {
            child,
            input,
            output: BufReader::new(output).lines(),
            info: EngineInfo {
                name: String::new(),
                threads: config.threads,
                hash_mb: config.hash_mb,
            },
        };
        let handshake = timeout(Duration::from_secs(15), async {
            engine.command("uci").await?;
            loop {
                let line = engine.line().await?;
                if let Some(name) = line.strip_prefix("id name ") {
                    engine.info.name = name.to_string();
                }
                if line == "uciok" {
                    break;
                }
            }
            if engine.info.name.is_empty() || engine.info.name.len() > 128 {
                return Err(Error::Engine("No valid UCI engine name".into()));
            }
            engine
                .command(&format!("setoption name Threads value {}", config.threads))
                .await?;
            engine
                .command(&format!("setoption name Hash value {}", config.hash_mb))
                .await?;
            engine.ready().await
        })
        .await;
        match handshake {
            Ok(Ok(())) => Ok(engine),
            Ok(Err(error)) => {
                engine.terminate().await?;
                Err(error)
            }
            Err(_) => {
                engine.terminate().await?;
                Err(Error::Engine("Startup timed out".into()))
            }
        }
    }

    /// Cancellation invalidates the process; the caller must terminate it before reuse.
    pub async fn evaluate(&mut self, request: &EvaluateRequest, timeout_ms: u64) -> Result<Evaluation, Error> {
        match timeout(Duration::from_millis(timeout_ms), self.search(request)).await {
            Ok(result) => result,
            Err(_) => Err(Error::SearchTimeout),
        }
    }

    pub async fn terminate(&mut self) -> Result<(), Error> {
        self.child.kill().await?;
        Ok(())
    }

    async fn search(&mut self, request: &EvaluateRequest) -> Result<Evaluation, Error> {
        let started = Instant::now();
        let board = request.board().map_err(|error| Error::Engine(error.body.message))?;
        let search = request
            .search
            .ok_or_else(|| Error::Engine("Missing search limit".into()))?;
        if request.engine.as_ref().is_some_and(|name| *name != self.info.name) {
            return Err(Error::Engine("Engine identity changed".into()));
        }
        let terminal = match board.status() {
            GameStatus::Won => Some(Score::Mate(0)),
            GameStatus::Drawn => Some(Score::Centipawns(0)),
            GameStatus::Ongoing => None,
        };
        if let Some(score) = terminal {
            return Ok(Evaluation {
                engine: self.info.clone(),
                search,
                best_move: None,
                elapsed_ms: 0,
                variations: vec![Variation {
                    multipv: 1,
                    score,
                    depth: 0,
                    nodes: 0,
                    pv: vec![],
                }],
            });
        }
        for mv in &request.search_moves {
            legal_move(&board, mv).map_err(|error| Error::Engine(error.body.message))?;
        }
        let mut legal_count = 0;
        board.generate_moves(|moves| {
            legal_count += moves.len();
            false
        });
        let roots = if request.search_moves.is_empty() {
            legal_count
        } else {
            request.search_moves.len()
        };
        let expected = roots.min(usize::from(request.multipv));
        if expected == 0 {
            return Err(Error::Engine("No requested variations".into()));
        }
        self.command(&format!("setoption name MultiPV value {}", request.multipv))
            .await?;
        self.command("ucinewgame").await?;
        self.ready().await?;
        let start = match &request.fen {
            Some(_) => format!(
                "fen {}",
                request
                    .starting_board()
                    .map_err(|error| Error::Engine(error.body.message))?
            ),
            None => "startpos".to_string(),
        };
        self.command(&format!("position {start} moves {}", request.moves.join(" ")))
            .await?;
        let limit = match search {
            SearchLimit::Nodes(value) => format!("nodes {value}"),
            SearchLimit::Depth(value) => format!("depth {value}"),
            SearchLimit::MovetimeMs(value) => format!("movetime {value}"),
        };
        let restriction = if request.search_moves.is_empty() {
            String::new()
        } else {
            format!(" searchmoves {}", request.search_moves.join(" "))
        };
        self.command(&format!("go {limit}{restriction}")).await?;
        let mut snapshots = MultiPvSnapshots::default();
        let mut transcript = UciTranscript::default();
        let best = loop {
            let line = self.line().await?;
            transcript.record(&line);
            if let Some(rest) = line.strip_prefix("bestmove ") {
                let mv = rest
                    .split_whitespace()
                    .next()
                    .ok_or_else(|| Error::Engine("Missing best move".into()))?;
                let mv = legal_move(&board, mv).map_err(|error| Error::Engine(error.body.message))?;
                break util::display_uci_move(&board, mv).to_string();
            }
            snapshots.record(&line, expected);
        };
        let Some(mut variations) = snapshots.matching(&best) else {
            tracing::warn!(engine = %self.info.name, threads = self.info.threads, hash_mb = self.info.hash_mb,
                ?search, multipv = request.multipv, search_moves = ?request.search_moves,
                fen = ?request.fen, moves = ?request.moves, best_move = %best,
                elapsed_ms = started.elapsed().as_millis(),
                uci_tail = ?transcript.lines, "Stockfish returned no matching exact evaluation");
            return Err(Error::IncompleteEvaluation);
        };
        for variation in &mut variations {
            let mut continuation = board.clone();
            for uci in &mut variation.pv {
                let mv = legal_move(&continuation, uci).map_err(|error| Error::Engine(error.body.message))?;
                *uci = util::display_uci_move(&continuation, mv).to_string();
                continuation.play(mv);
            }
        }
        let result = Evaluation {
            engine: self.info.clone(),
            search,
            best_move: Some(best),
            variations,
            elapsed_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
        };
        result
            .validate(request, &self.info)
            .map_err(|error| Error::Engine(error.body.message))?;
        Ok(result)
    }

    async fn command(&mut self, command: &str) -> Result<(), Error> {
        self.input.write_all(command.as_bytes()).await?;
        self.input.write_all(b"\n").await?;
        self.input.flush().await?;
        Ok(())
    }

    async fn ready(&mut self) -> Result<(), Error> {
        self.command("isready").await?;
        while self.line().await? != "readyok" {}
        Ok(())
    }

    async fn line(&mut self) -> Result<String, Error> {
        self.output
            .next_line()
            .await?
            .ok_or_else(|| Error::Engine("Process exited before completing the search".into()))
    }
}

/// Retain bounded diagnostic output without logging successful searches.
#[derive(Default)]
struct UciTranscript {
    lines: VecDeque<String>,
}

impl UciTranscript {
    fn record(&mut self, line: &str) {
        if self.lines.len() == 16 {
            self.lines.pop_front();
        }
        self.lines.push_back(line.chars().take(512).collect());
    }
}

#[derive(Default)]
struct MultiPvSnapshots {
    pending: Vec<Variation>,
    complete: BTreeMap<(u16, String), Vec<Variation>>,
}

impl MultiPvSnapshots {
    fn record(&mut self, line: &str, expected: usize) {
        // Stockfish emits each MultiPV batch in rank order. Missing, bounded,
        // or mixed-depth ranks must never be filled from another batch.
        let Some(variation) = parse_info(line) else {
            self.pending.clear();
            return;
        };
        if variation.multipv == 1 {
            self.pending.clear();
        }
        let root = variation.pv.first();
        if root.is_none()
            || usize::from(variation.multipv) != self.pending.len() + 1
            || self.pending.first().is_some_and(|first| first.depth != variation.depth)
            || self.pending.iter().any(|previous| previous.pv.first() == root)
        {
            self.pending.clear();
            return;
        }
        self.pending.push(variation);
        if self.pending.len() == expected
            && let Some(first) = self.pending.first()
            && let Some(root) = first.pv.first()
        {
            self.complete
                .insert((first.depth, root.clone()), std::mem::take(&mut self.pending));
        }
    }

    fn matching(self, best: &str) -> Option<Vec<Variation>> {
        self.complete
            .into_iter()
            .rev()
            .find_map(|((_, root), variations)| (root == best).then_some(variations))
    }
}

fn parse_info(line: &str) -> Option<Variation> {
    let tokens: Vec<_> = line.split_whitespace().collect();
    if tokens.first() != Some(&"info") || tokens.contains(&"lowerbound") || tokens.contains(&"upperbound") {
        return None;
    }
    let value = |key| {
        tokens
            .iter()
            .position(|token| *token == key)
            .and_then(|index| tokens.get(index + 1))
            .copied()
    };
    let index = tokens.iter().position(|token| *token == "score")?;
    let number = tokens.get(index + 2)?.parse().ok()?;
    let score = match *tokens.get(index + 1)? {
        "cp" => Score::Centipawns(number),
        "mate" => Score::Mate(number),
        _ => return None,
    };
    let pv = tokens.iter().position(|token| *token == "pv")?;
    Some(Variation {
        multipv: value("multipv").unwrap_or("1").parse().ok()?,
        score,
        depth: value("depth")?.parse().ok()?,
        nodes: value("nodes")?.parse().ok()?,
        pv: tokens
            .get(pv + 1..)?
            .iter()
            .take(256)
            .map(|mv| (*mv).to_string())
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_transcript_keeps_only_a_bounded_tail() {
        let mut transcript = UciTranscript::default();
        for _ in 0..20 {
            transcript.record(&"é".repeat(1000));
        }
        transcript.record("bestmove e2e4");
        assert_eq!(transcript.lines.len(), 16);
        assert_eq!(transcript.lines.front().unwrap().chars().count(), 512);
        assert_eq!(transcript.lines.back().unwrap(), "bestmove e2e4");
    }

    #[test]
    fn bounds_and_incomplete_info_are_not_exact_evaluations() {
        assert!(parse_info("info depth 10 score cp 42 lowerbound nodes 100 pv e2e4").is_none());
        assert!(parse_info("info depth 10 nodes 100 currmove e2e4").is_none());
        let mate = parse_info("info depth 12 multipv 2 score mate -3 nodes 987 pv e2e4 e7e5").unwrap();
        assert_eq!(mate.score, Score::Mate(-3));
        assert_eq!(mate.multipv, 2);
    }

    #[test]
    fn incomplete_batches_cannot_share_ranks() {
        for incomplete in [
            "info depth 8 multipv 1 score cp 40 lowerbound nodes 110 pv d2d4",
            "info depth 8 multipv 1 score cp 40 upperbound nodes 110 pv d2d4",
            "info depth 8 multipv 1 score cp 40 nodes 110",
        ] {
            let mut snapshots = MultiPvSnapshots::default();
            for line in [
                "info depth 8 multipv 1 score cp 30 nodes 100 pv e2e4",
                incomplete,
                "info depth 8 multipv 2 score cp 20 nodes 110 pv g1f3",
            ] {
                snapshots.record(line, 2);
            }
            assert!(snapshots.matching("e2e4").is_none());
        }
    }

    #[test]
    fn mixed_depth_batches_do_not_overwrite_completed_snapshots() {
        let mut snapshots = MultiPvSnapshots::default();
        for line in [
            "info depth 8 multipv 1 score cp 30 nodes 100 pv e2e4",
            "info depth 8 multipv 2 score cp 20 nodes 100 pv d2d4",
            "info depth 9 multipv 1 score cp 40 nodes 200 pv e2e4",
            "info depth 8 multipv 2 score cp 15 nodes 200 pv g1f3",
        ] {
            snapshots.record(line, 2);
        }
        let variations = snapshots.matching("e2e4").unwrap();
        assert_eq!(variations[0].score, Score::Centipawns(30));
        assert_eq!(variations[1].pv, ["d2d4"]);
        assert!(variations.iter().all(|v| v.depth == 8 && v.nodes == 100));
    }
}
