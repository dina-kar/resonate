//! Loam's Dapr ↔ Resonate bridge: the shared pieces.
//!
//! Dapr delivers external triggers (pub/sub messages, input-binding events,
//! service invocations) at least once. The bridge turns each one into a
//! **normalized trigger** and starts a Resonate workflow with a promise id
//! derived deterministically from the trigger's identity. A redelivery
//! derives the same id, `promise.create` of an existing id returns the
//! existing promise, and so a duplicate delivery never starts a second
//! execution. That id derivation is the whole exactly-once argument; Dapr's
//! own Workflow building block is never used (see [`guard`]).

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// The Resonate protocol version the server speaks.
pub const PROTOCOL_VERSION: &str = "2026-04-01";

/// One trigger, whatever carried it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Trigger {
    /// `pubsub/<name>/<topic>`, `binding/<name>` or `invoke/<method>`.
    pub source: String,
    /// The delivery-independent identity: the CloudEvent id, an
    /// `idempotency-key` metadata value, or a content hash.
    pub key: String,
    /// CloudEvent type, or the binding/method name.
    pub kind: String,
    /// The payload, as JSON when it parses, else a string.
    pub data: Value,
}

impl Trigger {
    /// The promise id: one origin per trigger (no `:`), stable across
    /// redeliveries. Hashed so arbitrary source and key bytes are safe.
    pub fn promise_id(&self) -> String {
        let mut h = Sha256::new();
        h.update(self.source.as_bytes());
        h.update([0]);
        h.update(self.key.as_bytes());
        format!("trig-{}", &hex::encode(h.finalize())[..32])
    }
}

/// Parse a payload as JSON, or keep it as a UTF-8 (lossy) string.
pub fn payload(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(bytes).into_owned()))
}

/// A content hash, for events that carry no identity of their own.
pub fn content_key(parts: &[&[u8]]) -> String {
    let mut h = Sha256::new();
    for p in parts {
        h.update((p.len() as u64).to_be_bytes());
        h.update(p);
    }
    format!("sha256-{}", hex::encode(h.finalize()))
}

/// What happened to a start request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Started {
    /// The workflow promise exists (created now, or by an earlier delivery).
    Accepted,
}

/// Why a start failed.
#[derive(Debug)]
pub enum StartError {
    /// Worth a retry (network, 5xx, 429).
    Retry(String),
    /// Will never succeed (4xx other than 409/429).
    Drop(String),
}

/// A minimal client for Resonate's HTTP protocol (`POST /`).
#[derive(Clone)]
pub struct Resonate {
    http: reqwest::Client,
    url: String,
    /// When set, every call carries `dapr-app-id: <id>`: `url` is then the
    /// local sidecar (`http://127.0.0.1:3500/`), which forwards the request
    /// to that app over mTLS (Dapr service invocation, HTTP proxy mode).
    app_id: Option<String>,
}

impl Resonate {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("http client"),
            url: url.into(),
            app_id: None,
        }
    }

    /// Route every call through the local Dapr sidecar to app `id`.
    pub fn via_dapr(mut self, id: Option<String>) -> Self {
        self.app_id = id.filter(|s| !s.is_empty());
        self
    }

    /// Send one request; returns (status, body).
    pub async fn call(&self, kind: &str, data: Value) -> Result<(u16, Value), String> {
        let body = json!({
            "kind": kind,
            "head": { "corrId": format!("bridge-{kind}"), "version": PROTOCOL_VERSION },
            "data": data,
        });
        let mut req = self.http.post(&self.url).json(&body);
        if let Some(id) = &self.app_id {
            req = req.header("dapr-app-id", id);
        }
        let resp = req.send().await.map_err(|e| e.to_string())?;
        let status = resp.status().as_u16();
        let v: Value = resp.json().await.unwrap_or(Value::Null);
        // The protocol carries its own status in the envelope's head.
        let inner = v
            .pointer("/head/status")
            .and_then(Value::as_u64)
            .map(|s| s as u16)
            .unwrap_or(status);
        Ok((inner, v))
    }

    /// Start the workflow for `t`, targeting `target` (a Resonate address,
    /// e.g. `http://trigger-worker:8080/` or `poll://any@workflows`).
    pub async fn start(
        &self,
        t: &Trigger,
        target: &str,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<Started, StartError> {
        let id = t.promise_id();
        let param = json!({
            "headers": {},
            "data": base64_json(&json!({ "source": t.source, "key": t.key, "kind": t.kind, "data": t.data })),
        });
        let tags = json!({
            "resonate:target": target,
            "resonate:origin": id,
            "loam:trigger": t.source,
        });
        let (status, body) = self
            .call(
                "promise.create",
                json!({ "id": id, "timeoutAt": now_ms + ttl_ms, "param": param, "tags": tags }),
            )
            .await
            .map_err(StartError::Retry)?;
        // Resonate answers 200 whether the promise was created now or
        // existed (create is idempotent), so the two are not told apart here;
        // exactly-once rests on the id, not on this status.
        match status {
            200 | 201 | 409 => Ok(Started::Accepted),
            429 | 500..=599 => Err(StartError::Retry(format!("{status}: {body}"))),
            _ => Err(StartError::Drop(format!("{status}: {body}"))),
        }
    }
}

fn base64_json(v: &Value) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = serde_json::to_vec(v).expect("json");
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for c in bytes.chunks(3) {
        let n = (c[0] as u32) << 16
            | (*c.get(1).unwrap_or(&0) as u32) << 8
            | *c.get(2).unwrap_or(&0) as u32;
        out.push(T[(n >> 18) as usize & 63] as char);
        out.push(T[(n >> 12) as usize & 63] as char);
        out.push(if c.len() > 1 {
            T[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if c.len() > 2 {
            T[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// The hard constraint: Dapr's Workflow building block must not be in use,
/// because Resonate is the only durability mechanism. Reads the sidecar's
/// metadata and refuses any workflow backend component.
pub mod guard {
    use serde_json::Value;

    /// Component types that mean "Dapr Workflow is configured".
    pub fn workflow_components(metadata: &Value) -> Vec<String> {
        metadata
            .get("components")
            .and_then(Value::as_array)
            .map(|cs| {
                cs.iter()
                    .filter_map(|c| {
                        let ty = c.get("type").and_then(Value::as_str).unwrap_or("");
                        let name = c.get("name").and_then(Value::as_str).unwrap_or("");
                        ty.starts_with("workflow").then(|| format!("{name} ({ty})"))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Fail unless the sidecar reports no workflow component and no
    /// connected workflow workers.
    pub async fn check(sidecar_http: &str) -> Result<(), String> {
        let url = format!("{sidecar_http}/v1.0/metadata");
        let md: Value = reqwest::get(&url)
            .await
            .map_err(|e| format!("cannot read {url}: {e}"))?
            .json()
            .await
            .map_err(|e| format!("bad metadata from {url}: {e}"))?;
        let found = workflow_components(&md);
        if !found.is_empty() {
            return Err(format!(
                "Dapr Workflow is configured ({}); Resonate must be the only durability mechanism",
                found.join(", ")
            ));
        }
        let workers = md
            .pointer("/workflows/connectedWorkers")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if workers > 0 {
            return Err(format!(
                "{workers} Dapr Workflow workers are connected to this sidecar"
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_redelivery_derives_the_same_promise_id() {
        let a = Trigger {
            source: "pubsub/p/t".into(),
            key: "e1".into(),
            kind: "x".into(),
            data: json!({"n":1}),
        };
        let mut b = a.clone();
        b.data = json!({"n": 2}); // payload does not enter the identity
        assert_eq!(a.promise_id(), b.promise_id());
        let mut c = a.clone();
        c.key = "e2".into();
        assert_ne!(a.promise_id(), c.promise_id());
        assert!(!a.promise_id().contains(':'), "one origin per trigger");
    }

    #[test]
    fn content_keys_are_length_prefixed() {
        assert_ne!(content_key(&[b"ab", b"c"]), content_key(&[b"a", b"bc"]));
    }

    #[test]
    fn base64_matches_the_standard_alphabet() {
        assert_eq!(base64_json(&json!("hi")), "ImhpIg==");
    }

    #[test]
    fn workflow_components_are_detected() {
        let md = json!({"components":[
            {"name":"triggers","type":"pubsub.redis"},
            {"name":"wf","type":"workflowbackend.actors"}]});
        assert_eq!(
            guard::workflow_components(&md),
            vec!["wf (workflowbackend.actors)"]
        );
        assert!(guard::workflow_components(&json!({"components":[]})).is_empty());
    }
}
