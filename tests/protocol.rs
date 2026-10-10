use clap::Parser;
use stockfish_relay::{
    config::{Cli, Command},
    http,
    protocol::*,
};

fn config() -> stockfish_relay::config::MasterConfig {
    match Cli::parse_from([
        "stockfish-relay",
        "master",
        "--api-key",
        "client-key-for-tests",
        "--worker-key",
        "worker-key-for-tests",
    ])
    .command
    {
        Command::Master(config) => config,
        _ => unreachable!(),
    }
}

#[test]
fn admin_credentials_must_be_complete_valid_and_separate() {
    let mut config = config();
    assert!(config.validate().is_ok());
    config.admin_username = Some("admin".into());
    assert!(config.validate().is_err());
    for password in ["abcd", "short", "admin-password-for-tests"] {
        config.admin_password = Some(password.into());
        assert!(config.validate().is_ok());
    }
    for username in [None, Some(""), Some("user:name"), Some("two words"), Some("user\nname")] {
        config.admin_username = username.map(str::to_owned);
        assert!(config.validate().is_err());
    }
    config.admin_username = Some("admin".into());
    for password in [
        "",
        "abc",
        "password with spaces",
        "password-with-newline\n",
        &config.api_key.clone(),
        &config.worker_key.clone(),
    ] {
        config.admin_password = Some(password.into());
        assert!(config.validate().is_err());
    }
}

#[test]
fn requests_validate_history_and_search_limits_before_reaching_an_engine() {
    let config = config();
    let request: EvaluateRequest = serde_json::from_value(serde_json::json!({
        "moves": ["e2e4", "e7e5"], "search": {"nodes": 100000}, "search_moves": ["g1f3"]
    }))
    .unwrap();
    let normalized = request.normalize(&config).unwrap();
    assert_eq!(normalized.board().unwrap().side_to_move(), cozy_chess::Color::White);
    assert_eq!(normalized.multipv, 1);
    for body in [
        serde_json::json!({"moves": ["e2e5"]}),
        serde_json::json!({"moves": ["e2e4\nquit"]}),
        serde_json::json!({"fen": "not a fen"}),
        serde_json::json!({"search": {"nodes": 0}}),
        serde_json::json!({"search": {"nodes": 50000001}}),
        serde_json::json!({"search": {"depth": 100}}),
        serde_json::json!({"search_moves": ["e7e5"]}),
        serde_json::json!({"multipv": 0}),
    ] {
        assert!(
            serde_json::from_value::<EvaluateRequest>(body)
                .unwrap()
                .normalize(&config)
                .is_err()
        );
    }
    assert!(serde_json::from_value::<EvaluateRequest>(serde_json::json!({"command": "quit"})).is_err());
    assert!(
        serde_json::from_value::<EvaluateRequest>(serde_json::json!({"search": {"nodes": 100, "depth": 4}})).is_err()
    );
}

#[test]
fn fen_history_and_castling_are_preserved_in_standard_uci_notation() {
    let request = EvaluateRequest {
        fen: Some("r3k2r/8/8/8/8/8/8/R3K2R w KQkq - 0 1".into()),
        moves: vec!["e1g1".into()],
        search_moves: vec!["e8c8".into()],
        ..Default::default()
    }
    .normalize(&config())
    .unwrap();
    assert_eq!(request.moves, vec!["e1g1"]);
    assert_eq!(request.search_moves, vec!["e8c8"]);
    assert_eq!(request.board().unwrap().side_to_move(), cozy_chess::Color::Black);
}

#[test]
fn terminal_positions_and_mate_scores_keep_their_meaning() {
    let request = EvaluateRequest {
        moves: vec!["f2f3".into(), "e7e5".into(), "g2g4".into(), "d8h4".into()],
        ..Default::default()
    }
    .normalize(&config())
    .unwrap();
    let engine = EngineInfo {
        name: "Stockfish test".into(),
        threads: 1,
        hash_mb: 16,
    };
    let result = Evaluation {
        engine: engine.clone(),
        search: request.search.unwrap(),
        best_move: None,
        elapsed_ms: 0,
        variations: vec![Variation {
            multipv: 1,
            score: Score::Mate(0),
            depth: 0,
            nodes: 0,
            pv: vec![],
        }],
    };
    result.validate(&request, &engine).unwrap();
    let json = serde_json::to_value(&result).unwrap();
    assert_eq!(
        json["variations"][0]["score"],
        serde_json::json!({"kind":"mate","value":0})
    );
}

#[test]
fn duplicate_variation_roots_are_rejected() {
    let request = EvaluateRequest {
        multipv: 2,
        ..Default::default()
    }
    .normalize(&config())
    .unwrap();
    let engine = EngineInfo {
        name: "Stockfish test".into(),
        threads: 1,
        hash_mb: 16,
    };
    let mut result = Evaluation {
        engine: engine.clone(),
        search: request.search.unwrap(),
        best_move: Some("e2e4".into()),
        elapsed_ms: 1,
        variations: vec![
            Variation {
                multipv: 1,
                score: Score::Centipawns(30),
                depth: 8,
                nodes: 1000,
                pv: vec!["e2e4".into(), "e7e5".into()],
            },
            Variation {
                multipv: 2,
                score: Score::Centipawns(20),
                depth: 8,
                nodes: 1000,
                pv: vec!["d2d4".into(), "d7d5".into()],
            },
        ],
    };
    result.validate(&request, &engine).unwrap();
    result.variations[1].pv = result.variations[0].pv.clone();
    assert_eq!(
        result.validate(&request, &engine).unwrap_err().body.message,
        "Duplicate principal variation root"
    );
}

#[test]
fn openapi_describes_the_public_api_worker_protocol_and_separate_keys() {
    let schema = serde_json::to_value(http::openapi()).unwrap();
    assert!(schema["paths"]["/v1/evaluate"]["post"].is_object());
    assert!(schema["paths"]["/v1/workers/{worker_id}/heartbeat"]["post"].is_object());
    assert!(schema["components"]["schemas"]["Outcome"].is_object());
    assert!(schema["components"]["schemas"]["SearchLimit"].is_object());
    assert!(schema["components"]["securitySchemes"]["client_key"].is_object());
    assert!(schema["components"]["securitySchemes"]["worker_key"].is_object());
}

#[test]
fn worker_requires_a_master_and_supports_the_slave_alias() {
    assert!(Cli::try_parse_from(["stockfish-relay", "worker"]).is_err());
    let command = Cli::try_parse_from([
        "stockfish-relay",
        "slave",
        "--master",
        "https://engine.example.com:443",
        "--worker-key",
        "worker-key-for-tests",
    ])
    .unwrap()
    .command;
    let Command::Worker(config) = command else {
        panic!("Expected worker")
    };
    config.validate().unwrap();
    assert_eq!(config.master.host_str(), Some("engine.example.com"));
}
