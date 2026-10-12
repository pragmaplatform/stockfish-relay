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

use crate::{
    master::Master,
    protocol::{Status, TimingMetric},
};

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
    content.push_str("</section><section class=\"workers\" aria-labelledby=\"timings-heading\"><h2 id=\"timings-heading\">Job timings</h2>");
    content.push_str("<p class=\"detail timing-note\">Latest 1,000 successful completions across all workers. Average / p95 in milliseconds; p95 is the time at or below which 95% of samples fall. Failed and unfinished jobs are excluded. Timings reset on master restart.</p>");
    let timings = &status.timings;
    write!(content, "<div class=\"table-wrap\" tabindex=\"0\" role=\"region\" aria-label=\"Job timings\"><table><thead><tr><th scope=\"col\">Metric</th><th scope=\"col\">First attempt (samples: {})</th><th scope=\"col\">After retries (samples: {})</th></tr></thead><tbody>", timings.first_attempt.samples, timings.retried.samples).unwrap();
    for (label, first, retried) in [
        (
            "Queue wait",
            &timings.first_attempt.queue_wait,
            &timings.retried.queue_wait,
        ),
        ("Engine time", &timings.first_attempt.engine, &timings.retried.engine),
        (
            "Dispatch/result overhead",
            &timings.first_attempt.overhead,
            &timings.retried.overhead,
        ),
        ("Total job time", &timings.first_attempt.total, &timings.retried.total),
    ] {
        write!(
            content,
            "<tr><th scope=\"row\">{label}</th><td>{}</td><td>{}</td></tr>",
            format_metric(first),
            format_metric(retried)
        )
        .unwrap();
    }
    content.push_str("</tbody></table></div><p class=\"detail timing-note\">Queue wait includes every attempt's queue wait. Engine time and overhead cover the successful attempt only. Overhead is assignment-to-completion minus worker-reported engine time, clamped to zero; it includes network travel, worker scheduling, serialization, and any engine startup. It is not pure network latency. Total job time includes retries but excludes API client network travel. Missing samples show —.</p></section>");
    content.push_str(
        "<section class=\"workers\" aria-labelledby=\"workers-heading\"><h2 id=\"workers-heading\">Workers</h2>",
    );
    if status.workers.is_empty() {
        content.push_str(
            "<p class=\"empty\">No workers connected. Workers will appear here when they register with the master.</p>",
        );
    } else {
        content.push_str("<div class=\"table-wrap\" tabindex=\"0\" role=\"region\" aria-label=\"Connected workers\"><table><thead><tr><th scope=\"col\">Worker</th><th scope=\"col\">Heartbeat RTT</th><th scope=\"col\">Relay version</th><th scope=\"col\">Uptime</th><th scope=\"col\">Engine</th><th scope=\"col\">Running / slots</th><th scope=\"col\">Threads / engine</th><th scope=\"col\">Hash / engine</th></tr></thead><tbody>");
        for worker in &status.workers {
            write!(content, "<tr><th scope=\"row\">{}<small>{}</small></th><td>{}<small>Samples: {}</small></td><td>{}</td><td>{}</td><td>{}</td><td>{} / {}</td><td>{}</td><td>{} MB</td></tr>",
                escape(&worker.name), worker.id, format_metric(&worker.heartbeat_rtt), worker.heartbeat_rtt_samples, escape(worker.relay_version.as_deref().unwrap_or("Unknown")), format_uptime(worker.uptime_seconds), escape(&worker.engine.name), worker.running, worker.capacity, worker.engine.threads, worker.engine.hash_mb).unwrap();
        }
        content.push_str("</tbody></table></div><p class=\"detail timing-note\">Heartbeat RTT: average / p95 in milliseconds over this session’s latest 1,000 successful heartbeat round trips. Includes HTTP handling, excludes long-poll waits; reported one heartbeat later. Older workers show — until upgraded.</p>");
    }
    content.push_str("</section>");
    if !status.workers.is_empty() {
        content.push_str("<section class=\"workers\" aria-labelledby=\"worker-timings-heading\"><h2 id=\"worker-timings-heading\">Worker timings</h2><p class=\"detail timing-note\">Average / p95 in milliseconds. Each connected session's successful completions within the same 1,000-job window; a new session starts with no samples.</p><div class=\"table-wrap\" tabindex=\"0\" role=\"region\" aria-label=\"Worker timings\"><table><thead><tr><th scope=\"col\">Worker</th><th scope=\"col\">Completion</th><th scope=\"col\">Samples</th><th scope=\"col\">Engine time</th><th scope=\"col\">Dispatch/result overhead</th></tr></thead><tbody>");
        for worker in &status.workers {
            for (label, summary) in [
                ("First attempt", &worker.timings.first_attempt),
                ("After retries", &worker.timings.retried),
            ] {
                write!(content, "<tr><th scope=\"row\">{}<small>{}</small></th><td>{label}</td><td>{}</td><td>{}</td><td>{}</td></tr>", escape(&worker.name), worker.id, summary.samples, format_metric(&summary.engine), format_metric(&summary.overhead)).unwrap();
            }
        }
        content.push_str("</tbody></table></div></section>");
    }
    include_str!("admin.html")
        .replace("{{version}}", &escape(&status.relay_version))
        .replace("{{uptime}}", &format_uptime(status.uptime_seconds))
        .replace("{{content}}", &content)
}

fn format_metric(metric: &Option<TimingMetric>) -> String {
    match metric {
        Some(metric) => format!("{:.1} / {:.1}", metric.average_ms, metric.p95_ms),
        None => "—".into(),
    }
}

fn format_uptime(seconds: u64) -> String {
    format!(
        "{}d {}h {}m {}s",
        seconds / 86_400,
        seconds / 3600 % 24,
        seconds / 60 % 60,
        seconds % 60
    )
}

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
