use std::{fmt::Write, sync::Arc};

use axum::{
    Router,
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use subtle::ConstantTimeEq;

use crate::{master::Master, protocol::Status};

pub(crate) fn router(master: &Master) -> Router<Master> {
    let (Some(username), Some(password)) = (&master.config.admin_username, &master.config.admin_password) else {
        return Router::new();
    };
    let credentials = Arc::new(STANDARD.encode(format!("{username}:{password}")));
    Router::new()
        .route("/admin", get(dashboard))
        .route("/admin/", get(dashboard))
        .route_layer(middleware::from_fn_with_state(credentials, authorize))
}

async fn authorize(State(credentials): State<Arc<String>>, request: Request, next: Next) -> Response {
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("Basic"))
        .map(|(_, value)| value.trim_start_matches(' '));
    let mut response = if supplied.is_some_and(|value| bool::from(value.as_bytes().ct_eq(credentials.as_bytes()))) {
        next.run(request).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Basic realm=\"Stockfish Relay admin\"")],
            "Admin authentication required",
        )
            .into_response()
    };
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    headers.insert(header::X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        "default-src 'none'; style-src 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"
            .parse()
            .unwrap(),
    );
    response
}

async fn dashboard(State(master): State<Master>) -> Html<String> {
    Html(render(master.status().await))
}

fn render(mut status: Status) -> String {
    status.workers.sort_by(|a, b| a.name.cmp(&b.name).then(a.id.cmp(&b.id)));
    let capacity: u64 = status.workers.iter().map(|worker| u64::from(worker.capacity)).sum();
    let mut content = String::from("<section class=\"stats\" aria-label=\"Relay statistics\">");
    for (label, value, detail) in [
        (
            "Connected workers",
            status.workers.len() as u64,
            "Current worker sessions",
        ),
        ("Engine slots", capacity, "Total connected capacity"),
        ("Running", status.running as u64, "Jobs currently assigned"),
        (
            "Interactive queue",
            status.queued_interactive as u64,
            "Waiting for an engine slot",
        ),
        ("Batch queue", status.queued_batch as u64, "Waiting for an engine slot"),
        (
            "Available slots",
            capacity.saturating_sub(status.running as u64),
            "Connected slots not in use",
        ),
        ("Completed", status.completed, "Since master start"),
        ("Retried", status.retried, "Since master start"),
        ("Failed", status.failed, "Since master start"),
    ] {
        write!(content, "<article class=\"stat\"><h2>{label}</h2><p class=\"value\">{value}</p><p class=\"detail\">{detail}</p></article>").unwrap();
    }
    content.push_str("</section><section class=\"workers\" aria-labelledby=\"workers-heading\"><h2 id=\"workers-heading\">Workers</h2>");
    if status.workers.is_empty() {
        content.push_str(
            "<p class=\"empty\">No workers connected. Workers will appear here when they register with the master.</p>",
        );
    } else {
        content.push_str("<div class=\"table-wrap\" tabindex=\"0\" role=\"region\" aria-label=\"Connected workers\"><table><thead><tr><th scope=\"col\">Worker</th><th scope=\"col\">Engine</th><th scope=\"col\">Running / slots</th><th scope=\"col\">Threads / engine</th><th scope=\"col\">Hash / engine</th></tr></thead><tbody>");
        for worker in status.workers {
            write!(content, "<tr><th scope=\"row\">{}<small>{}</small></th><td>{}</td><td>{} / {}</td><td>{}</td><td>{} MB</td></tr>",
                escape(&worker.name), worker.id, escape(&worker.engine.name), worker.running, worker.capacity, worker.engine.threads, worker.engine.hash_mb).unwrap();
        }
        content.push_str("</tbody></table></div>");
    }
    content.push_str("</section>");
    include_str!("admin.html").replace("{{content}}", &content)
}

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
