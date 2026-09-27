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

use resonate_dapr_bridge::Trigger;
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
    // This run's workflow promise ids, derived exactly as the bridge derives
    // them. Only these are judged: late redeliveries of earlier runs' messages
    // may still arrive and are not this run's business.
    let mut ids: Vec<String> = (0..events)
        .map(|i| {
            let topic = if i % 2 == 0 {
                "webhooks"
            } else {
                "agent-events"
            };
            Trigger {
                source: format!("pubsub/triggers/{topic}"),
                key: format!("{run}-ev-{i}"),
                kind: String::new(),
                data: Value::Null,
            }
            .promise_id()
        })
        .collect();
    ids.extend((0..invokes).map(|i| {
        Trigger {
            source: "invoke/trigger".into(),
            key: format!("{run}-inv-{i}"),
            kind: String::new(),
            data: Value::Null,
        }
        .promise_id()
    }));
    let count = |v: &Value| -> (usize, usize, u64) {
        // (ids executed at least once, total executions of them, max per id)
        let by = &v["by_promise"];
        let mut seen = 0;
        let mut total = 0u64;
        let mut max = 0u64;
        for id in &ids {
            let n = by[id].as_u64().unwrap_or(0);
            if n > 0 {
                seen += 1;
            }
            total += n;
            max = max.max(n);
        }
        (seen, total as usize, max)
    };
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
        let (seen, _, _) = count(&last);
        if seen >= expected || Instant::now() > deadline {
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
    let (seen, execs, max_per) = count(&last);
    // Ask Resonate (through Dapr) for every workflow promise of this run.
    let mut resolved = 0usize;
    let mut not_found = 0usize;
    for id in &ids {
        let body = json!({"kind": "promise.get",
            "head": {"corrId": "check", "version": resonate_dapr_bridge::PROTOCOL_VERSION},
            "data": {"id": id}});
        let v: Value = match http
            .post(format!("{sidecar}/v1.0/invoke/resonate/method/"))
            .json(&body)
            .send()
            .await
        {
            Ok(r) => r.json().await.unwrap_or(Value::Null),
            Err(_) => Value::Null,
        };
        match v.pointer("/data/promise/state").and_then(Value::as_str) {
            Some("resolved") => resolved += 1,
            _ if v.pointer("/head/status").and_then(Value::as_u64) == Some(404) => not_found += 1,
            _ => {}
        }
    }
    // The claims, separately:
    //  - no trigger lost: every trigger's workflow promise exists and resolved;
    //  - exactly-once start: one promise per trigger identity (by construction
    //    of the id; duplicates can only show up as re-executions);
    //  - execution is at-least-once: a re-execution is allowed only when a
    //    completion was not recorded (a fulfill that failed), and is reported.
    let reexecutions = execs.saturating_sub(seen);
    let strict = env("CHECK_STRICT", "1") == "1";
    let pass =
        resolved == expected && seen == expected && sent_failures == 0 && (!strict || max_per == 1);
    println!(
        "{}",
        json!({
            "pass": pass, "strict": strict, "run": run, "expected": expected, "executed_ids": seen,
            "resolved": resolved, "not_found": not_found, "reexecutions": reexecutions,
            "executions": execs, "max_per_promise": max_per,
            "deliveries_sent": events * dups + invokes * 2, "send_failures": sent_failures,
            "worker_totals": {
                "promises": last["promises"], "executions": last["executions"],
                "fulfilled": last["fulfilled"], "fulfill_failures": last["fulfill_failures"],
                "duplicate_messages": last["duplicate_messages"],
            },
            "publish_secs": publish_secs, "total_secs": started.elapsed().as_secs_f64(),
        })
    );
    let _ = http.post(format!("{sidecar}/v1.0/shutdown")).send().await;
    std::process::exit(if pass { 0 } else { 1 });
}
