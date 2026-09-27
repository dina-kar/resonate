//! `trigger-check`: the Phase 0 exit test, run as a Kubernetes Job with a
//! Dapr sidecar (app id `trigger-publisher`).
//!
//! 1. Publishes `CHECK_EVENTS` distinct CloudEvents to `triggers/webhooks`
//!    and `triggers/agent-events`, each `CHECK_DUPLICATES` times, all at once:
//!    Dapr pub/sub is at-least-once, and this adds deliberate duplicates on
//!    top of any redelivery.
//! 2. Starts `CHECK_INVOKES` triggers through service invocation of the
//!    bridge (`trigger/<key>`), each twice.
//! 3. Polls the worker's `/executions` through Dapr until every trigger ran,
//!    then asserts: executions == distinct triggers, and at most one execution
//!    per workflow promise.
//! 4. Shuts its sidecar down so the Job completes.
//!
//! Exit 0 on pass, 1 on fail. Prints one JSON line with the numbers.

use std::time::{Duration, Instant};

use serde_json::{json, Value};

fn env(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_owned())
}

#[tokio::main]
async fn main() {
    let sidecar = format!("http://127.0.0.1:{}", env("DAPR_HTTP_PORT", "3500"));
    let events: usize = env("CHECK_EVENTS", "200").parse().unwrap();
    let dups: usize = env("CHECK_DUPLICATES", "3").parse().unwrap();
    let invokes: usize = env("CHECK_INVOKES", "20").parse().unwrap();
    let run = env("CHECK_RUN", &format!("{}", std::process::id()));
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();

    // Wait for the sidecar.
    for _ in 0..120 {
        if http
            .get(format!("{sidecar}/v1.0/healthz/outbound"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let started = Instant::now();
    let mut tasks = Vec::new();
    for i in 0..events {
        let topic = if i % 2 == 0 {
            "webhooks"
        } else {
            "agent-events"
        };
        for d in 0..dups {
            let http = http.clone();
            let url = format!("{sidecar}/v1.0/publish/triggers/{topic}");
            let ce = json!({
                "specversion": "1.0",
                "id": format!("{run}-ev-{i}"),
                "source": "loam/trigger-check",
                "type": format!("loam.{topic}"),
                "datacontenttype": "application/json",
                "data": { "n": i, "copy": d },
            });
            tasks.push(tokio::spawn(async move {
                for attempt in 0..5 {
                    let r = http
                        .post(&url)
                        .header("content-type", "application/cloudevents+json")
                        .body(ce.to_string())
                        .send()
                        .await;
                    if matches!(&r, Ok(resp) if resp.status().is_success()) {
                        return true;
                    }
                    tokio::time::sleep(Duration::from_millis(200 * (attempt + 1))).await;
                }
                false
            }));
        }
    }
    for i in 0..invokes {
        for _ in 0..2 {
            let http = http.clone();
            let url = format!("{sidecar}/v1.0/invoke/bridge/method/trigger/{run}-inv-{i}");
            tasks.push(tokio::spawn(async move {
                for attempt in 0..5 {
                    let r = http.post(&url).json(&json!({"invoke": i})).send().await;
                    if matches!(&r, Ok(resp) if resp.status().is_success()) {
                        return true;
                    }
                    tokio::time::sleep(Duration::from_millis(200 * (attempt + 1))).await;
                }
                false
            }));
        }
    }
    let mut sent_failures = 0;
    for t in tasks {
        if !t.await.unwrap_or(false) {
            sent_failures += 1;
        }
    }
    let publish_secs = started.elapsed().as_secs_f64();

    // The worker counts executions per promise id, across runs; select this
    // run's triggers by comparing totals before and after is fragile, so the
    // check is on this run's expected count against the worker's totals
    // delta, taken from a baseline read first.
    let expected = events + invokes;
    let deadline =
        Instant::now() + Duration::from_secs(env("CHECK_TIMEOUT_SECS", "300").parse().unwrap());
    let mut last = Value::Null;
    loop {
        if let Ok(r) = http
            .get(format!("{sidecar}/v1.0/invoke/worker/method/executions"))
            .send()
            .await
        {
            if let Ok(v) = r.json::<Value>().await {
                last = v;
            }
        }
        let execs = last["executions"].as_u64().unwrap_or(0) as usize;
        let baseline: usize = env("CHECK_BASELINE", "0").parse().unwrap();
        if execs >= baseline + expected || Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    // Let late redeliveries land before judging "exactly once".
    tokio::time::sleep(Duration::from_secs(
        env("CHECK_SETTLE_SECS", "15").parse().unwrap(),
    ))
    .await;
    if let Ok(r) = http
        .get(format!("{sidecar}/v1.0/invoke/worker/method/executions"))
        .send()
        .await
    {
        if let Ok(v) = r.json::<Value>().await {
            last = v;
        }
    }
    let baseline: usize = env("CHECK_BASELINE", "0").parse().unwrap();
    let execs = last["executions"].as_u64().unwrap_or(0) as usize
        - baseline.min(last["executions"].as_u64().unwrap_or(0) as usize);
    let promises = last["promises"].as_u64().unwrap_or(0) as usize;
    let max_per = last["max_per_promise"].as_u64().unwrap_or(0);
    let pass = execs == expected && max_per <= 1 && sent_failures == 0;
    println!(
        "{}",
        json!({
            "pass": pass, "expected": expected, "executions": execs, "promises": promises,
            "max_per_promise": max_per, "deliveries_sent": events * dups + invokes * 2,
            "send_failures": sent_failures, "duplicate_messages_at_worker": last["duplicate_messages"],
            "publish_secs": publish_secs, "total_secs": started.elapsed().as_secs_f64(),
        })
    );
    let _ = http.post(format!("{sidecar}/v1.0/shutdown")).send().await;
    std::process::exit(if pass { 0 } else { 1 });
}
