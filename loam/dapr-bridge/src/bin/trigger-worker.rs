//! `trigger-worker`: a Resonate worker for the bridge's workflows, used by the
//! exactly-once validation.
//!
//! Resonate pushes `execute` messages (`POST /`) to it. For each one it runs
//! `task.acquire`; a successful acquire is **one execution**, counted per
//! promise id. It then settles the workflow promise through `task.fulfill`.
//! `GET /executions` reports the counts, so the validation can assert that
//! every trigger ran exactly once however often Dapr delivered it.
//!
//! Environment: `WORKER_LISTEN` (default `0.0.0.0:8080`), `WORKER_RESONATE_URL`
//! (default: the message's `serverUrl`), `WORKER_RESONATE_APP_ID` (route through
//! the sidecar to that Dapr app), `WORKER_PID` (default: hostname).

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::{extract::State, routing::get, routing::post, Json, Router};
use resonate_dapr_bridge::Resonate;
use serde_json::{json, Value};
use tokio::sync::Mutex;

#[derive(Default)]
struct Counts {
    executions: BTreeMap<String, u64>,
    duplicate_messages: u64,
    fulfilled: u64,
    fulfill_failures: u64,
}

#[derive(Clone)]
struct App {
    counts: Arc<Mutex<Counts>>,
    resonate_override: Option<String>,
    resonate_app_id: Option<String>,
    pid: String,
}

async fn execute(State(app): State<App>, Json(msg): Json<Value>) -> Json<Value> {
    if msg.get("kind").and_then(Value::as_str) != Some("execute") {
        return Json(json!({"ignored": true}));
    }
    let id = msg
        .pointer("/data/task/id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let version = msg
        .pointer("/data/task/version")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let url = app
        .resonate_override
        .clone()
        .or_else(|| {
            msg.pointer("/head/serverUrl")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "http://127.0.0.1:8001/".into());
    let url = if url.ends_with('/') {
        url
    } else {
        format!("{url}/")
    };
    let r = Resonate::new(url).via_dapr(app.resonate_app_id.clone());
    let acquired = r
        .call(
            "task.acquire",
            json!({"id": id, "version": version, "pid": app.pid, "ttl": 60000}),
        )
        .await;
    match acquired {
        Ok((200 | 201, _)) => {
            {
                let mut c = app.counts.lock().await;
                *c.executions.entry(id.clone()).or_default() += 1;
            }
            // The workflow body would run here. Settle the promise.
            // `task.acquire` bumps the task version: fulfill must name the
            // acquired version (message version + 1), or it is refused with
            // 409 and the task is re-dispatched when its lease lapses.
            // Retry transient failures (a Resonate pod going away mid-call):
            // a completion that is never recorded makes Resonate re-dispatch
            // the task after its lease, which is at-least-once execution.
            // 409 means the task moved on (already fulfilled, or lease lost).
            let mut status = 0;
            let mut delay = std::time::Duration::from_millis(250);
            for _ in 0..8 {
                let done = r
                    .call(
                        "task.fulfill",
                        json!({"id": id, "version": version + 1, "action": {
                            "kind": "promise.settle", "head": {},
                            "data": {"id": id, "state": "resolved", "value": {"headers": {}, "data": ""}}}}),
                    )
                    .await;
                status = done.as_ref().map(|(s, _)| *s).unwrap_or(0);
                if status == 200 || status == 409 {
                    break;
                }
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(std::time::Duration::from_secs(8));
            }
            {
                let mut c = app.counts.lock().await;
                if status == 200 {
                    c.fulfilled += 1;
                } else {
                    c.fulfill_failures += 1;
                }
            }
            Json(json!({"executed": id, "fulfill": status}))
        }
        Ok((status, _)) => {
            // Already acquired or finished: a redelivered message, not a new
            // execution.
            app.counts.lock().await.duplicate_messages += 1;
            Json(json!({"skipped": id, "status": status}))
        }
        Err(e) => Json(json!({"error": e})),
    }
}

async fn executions(State(app): State<App>) -> Json<Value> {
    let c = app.counts.lock().await;
    let total: u64 = c.executions.values().sum();
    let max = c.executions.values().copied().max().unwrap_or(0);
    Json(json!({
        "promises": c.executions.len(),
        "executions": total,
        "max_per_promise": max,
        "duplicate_messages": c.duplicate_messages,
        "fulfilled": c.fulfilled,
        "fulfill_failures": c.fulfill_failures,
        "by_promise": c.executions,
    }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app = App {
        counts: Default::default(),
        resonate_override: std::env::var("WORKER_RESONATE_URL").ok(),
        resonate_app_id: std::env::var("WORKER_RESONATE_APP_ID").ok(),
        pid: std::env::var("WORKER_PID")
            .or_else(|_| std::env::var("HOSTNAME"))
            .unwrap_or_else(|_| "trigger-worker".into()),
    };
    let listen = std::env::var("WORKER_LISTEN").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let router = Router::new()
        .route("/", post(execute))
        // Dapr service invocation of method `execute`.
        .route("/execute", post(execute))
        .route("/executions", get(executions))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(app);
    let l = tokio::net::TcpListener::bind(&listen).await?;
    eprintln!("trigger-worker on {listen}");
    axum::serve(l, router).await?;
    Ok(())
}
