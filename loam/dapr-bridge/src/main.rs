//! `resonate-dapr-bridge`: a Dapr app (gRPC app callback) that turns every
//! trigger Dapr delivers into an idempotent Resonate workflow start.
//!
//! Configuration (environment):
//!
//! | variable | meaning | default |
//! |---|---|---|
//! | `BRIDGE_LISTEN` | gRPC app-callback address (the sidecar's `app-port`) | `0.0.0.0:50051` |
//! | `BRIDGE_RESONATE_URL` | Resonate's HTTP protocol endpoint | `http://127.0.0.1:8001/` |
//! | `BRIDGE_TARGET` | Resonate address of the workflow's worker | `poll://any@workflows` |
//! | `BRIDGE_SUBSCRIPTIONS` | `pubsub:topic,…` to subscribe to | empty |
//! | `BRIDGE_BINDINGS` | input binding names to accept | empty |
//! | `BRIDGE_RESONATE_APP_ID` | route Resonate calls through the sidecar to this Dapr app id | unset |
//! | `BRIDGE_TTL_MS` | workflow promise timeout | `3600000` |
//! | `DAPR_HTTP_PORT` | the sidecar's HTTP port, for the workflow guard | `3500` |
//! | `BRIDGE_SKIP_GUARD` | `1` skips the guard (local runs without a sidecar) | unset |
//!
//! Every Resonate call that the sidecar's delivery drives goes to
//! `BRIDGE_RESONATE_URL`. In the cluster that URL is the Resonate service
//! reached through Dapr service invocation (`http://127.0.0.1:3500/v1.0/invoke/resonate/method/`),
//! so the hop is mTLS'd and traced by the sidecars.

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use resonate_dapr_bridge::{content_key, guard, payload, Resonate, StartError, Started, Trigger};
use tonic::{Request, Response, Status};
use tracing::{info, warn};

pub mod pb {
    pub mod common {
        pub mod v1 {
            tonic::include_proto!("dapr.proto.common.v1");
        }
    }
    pub mod runtime {
        pub mod v1 {
            tonic::include_proto!("dapr.proto.runtime.v1");
        }
    }
}

use pb::common::v1 as common;
use pb::runtime::v1 as rt;
use rt::app_callback_health_check_server::{AppCallbackHealthCheck, AppCallbackHealthCheckServer};
use rt::app_callback_server::{AppCallback, AppCallbackServer};

struct Bridge {
    resonate: Resonate,
    target: String,
    ttl_ms: i64,
    subscriptions: Vec<(String, String)>,
    bindings: HashSet<String>,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

impl Bridge {
    async fn start(&self, t: Trigger) -> Result<Started, StartError> {
        let r = self
            .resonate
            .start(&t, &self.target, now_ms(), self.ttl_ms)
            .await;
        match &r {
            Ok(s) => {
                info!(source = %t.source, key = %t.key, promise = %t.promise_id(), outcome = ?s, "trigger")
            }
            Err(e) => warn!(source = %t.source, key = %t.key, error = ?e, "trigger failed"),
        }
        r
    }
}

#[tonic::async_trait]
impl AppCallback for Bridge {
    async fn on_invoke(
        &self,
        req: Request<common::InvokeRequest>,
    ) -> Result<Response<common::InvokeResponse>, Status> {
        // Service invocation: `POST /v1.0/invoke/bridge/method/trigger/<key>`
        // with the payload as body. The key is the caller's idempotency key.
        let r = req.into_inner();
        let (method, key) = r.method.split_once('/').unwrap_or((r.method.as_str(), ""));
        if method != "trigger" {
            return Err(Status::not_found(format!("no method {method}")));
        }
        let data = r.data.map(|a| a.value).unwrap_or_default();
        let key = if key.is_empty() {
            content_key(&[r.method.as_bytes(), &data])
        } else {
            key.to_owned()
        };
        let t = Trigger {
            source: "invoke/trigger".into(),
            key,
            kind: "invoke".into(),
            data: payload(&data),
        };
        let id = t.promise_id();
        match self.start(t).await {
            Ok(s) => Ok(Response::new(common::InvokeResponse {
                data: Some(prost_types::Any {
                    type_url: String::new(),
                    value: serde_json::to_vec(
                        &serde_json::json!({"promise": id, "created": s == Started::Created}),
                    )
                    .unwrap(),
                }),
                content_type: "application/json".into(),
            })),
            Err(StartError::Retry(m)) => Err(Status::unavailable(m)),
            Err(StartError::Drop(m)) => Err(Status::invalid_argument(m)),
        }
    }

    async fn list_topic_subscriptions(
        &self,
        _: Request<()>,
    ) -> Result<Response<rt::ListTopicSubscriptionsResponse>, Status> {
        Ok(Response::new(rt::ListTopicSubscriptionsResponse {
            subscriptions: self
                .subscriptions
                .iter()
                .map(|(p, t)| rt::TopicSubscription {
                    pubsub_name: p.clone(),
                    topic: t.clone(),
                    ..Default::default()
                })
                .collect(),
        }))
    }

    async fn on_topic_event(
        &self,
        req: Request<rt::TopicEventRequest>,
    ) -> Result<Response<rt::TopicEventResponse>, Status> {
        let e = req.into_inner();
        let key = if e.id.is_empty() {
            content_key(&[e.topic.as_bytes(), &e.data])
        } else {
            e.id.clone()
        };
        let t = Trigger {
            source: format!("pubsub/{}/{}", e.pubsub_name, e.topic),
            key,
            kind: e.r#type.clone(),
            data: payload(&e.data),
        };
        use rt::topic_event_response::TopicEventResponseStatus as S;
        let status = match self.start(t).await {
            Ok(_) => S::Success,
            Err(StartError::Retry(_)) => S::Retry,
            Err(StartError::Drop(_)) => S::Drop,
        };
        Ok(Response::new(rt::TopicEventResponse {
            status: status as i32,
        }))
    }

    async fn list_input_bindings(
        &self,
        _: Request<()>,
    ) -> Result<Response<rt::ListInputBindingsResponse>, Status> {
        Ok(Response::new(rt::ListInputBindingsResponse {
            bindings: self.bindings.iter().cloned().collect(),
        }))
    }

    async fn on_binding_event(
        &self,
        req: Request<rt::BindingEventRequest>,
    ) -> Result<Response<rt::BindingEventResponse>, Status> {
        let e = req.into_inner();
        // Bindings carry no event id. An `idempotency-key` metadata value
        // (set by the producer, or a Kafka key) is the identity; without one
        // the content is, so identical payloads collapse into one start.
        let key = e
            .metadata
            .get("idempotency-key")
            .cloned()
            .unwrap_or_else(|| content_key(&[e.name.as_bytes(), &e.data]));
        let t = Trigger {
            source: format!("binding/{}", e.name),
            key,
            kind: e.name.clone(),
            data: payload(&e.data),
        };
        match self.start(t).await {
            Ok(_) => Ok(Response::new(rt::BindingEventResponse::default())),
            Err(StartError::Retry(m)) => Err(Status::unavailable(m)),
            // A binding has no "drop" status; an error makes the component
            // retry or dead-letter it, and a malformed event is logged.
            Err(StartError::Drop(m)) => Err(Status::invalid_argument(m)),
        }
    }

    async fn on_bulk_topic_event(
        &self,
        _: Request<rt::TopicEventBulkRequest>,
    ) -> Result<Response<rt::TopicEventBulkResponse>, Status> {
        Err(Status::unimplemented("bulk subscribe is not used"))
    }

    async fn on_job_event(
        &self,
        _: Request<rt::JobEventRequest>,
    ) -> Result<Response<rt::JobEventResponse>, Status> {
        // Dapr Jobs would be a second scheduler beside Resonate's schedules.
        Err(Status::unimplemented(
            "Dapr jobs are not used; schedule with Resonate",
        ))
    }
}

struct Health;

#[tonic::async_trait]
impl AppCallbackHealthCheck for Health {
    async fn health_check(
        &self,
        _: Request<()>,
    ) -> Result<Response<rt::HealthCheckResponse>, Status> {
        Ok(Response::new(rt::HealthCheckResponse {}))
    }
}

fn env(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_owned())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    if std::env::var("BRIDGE_SKIP_GUARD").as_deref() != Ok("1") {
        let sidecar = format!("http://127.0.0.1:{}", env("DAPR_HTTP_PORT", "3500"));
        // The sidecar may start after us; its metadata endpoint answers once
        // it is up. Components are loaded before it answers.
        let mut last = String::new();
        let mut ok = false;
        for _ in 0..60 {
            match guard::check(&sidecar).await {
                Ok(()) => {
                    ok = true;
                    break;
                }
                Err(e) if e.starts_with("cannot read") || e.starts_with("bad metadata") => {
                    last = e;
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                Err(e) => {
                    eprintln!("refusing to start: {e}");
                    std::process::exit(2);
                }
            }
        }
        if !ok {
            eprintln!("refusing to start: the Dapr sidecar never answered ({last})");
            std::process::exit(2);
        }
        info!("workflow guard passed: no Dapr Workflow component is configured");
    }

    let subscriptions = env("BRIDGE_SUBSCRIPTIONS", "")
        .split(',')
        .filter_map(|s| s.split_once(':').map(|(p, t)| (p.to_owned(), t.to_owned())))
        .collect();
    let bindings = env("BRIDGE_BINDINGS", "")
        .split(',')
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    let bridge = Bridge {
        resonate: Resonate::new(env("BRIDGE_RESONATE_URL", "http://127.0.0.1:8001/"))
            .via_dapr(std::env::var("BRIDGE_RESONATE_APP_ID").ok()),
        target: env("BRIDGE_TARGET", "poll://any@workflows"),
        ttl_ms: env("BRIDGE_TTL_MS", "3600000").parse()?,
        subscriptions,
        bindings,
    };
    let addr = env("BRIDGE_LISTEN", "0.0.0.0:50051").parse()?;
    info!(%addr, target = %bridge.target, "bridge listening");
    tonic::transport::Server::builder()
        .add_service(AppCallbackServer::new(bridge))
        .add_service(AppCallbackHealthCheckServer::new(Health))
        .serve_with_shutdown(addr, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
