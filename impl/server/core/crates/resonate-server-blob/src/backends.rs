//! Opening a [`Store`] from a one-line spec, for tests, the differential
//! suite and the dev binary.
//!
//! ```text
//! memory                                   in-process, lost at exit
//! redb:/path/to/file.redb                  durable, one process (feature `redb`)
//! tikv://pd1:2379,pd2:2379/ns/?mode=optimistic&keyspace=k   (feature `tikv`)
//! ```
//!
//! For `tikv`, the path is the key namespace (default `resonate/`); `mode` is
//! `pessimistic` (default) or `optimistic`; `keyspace` selects an API v2
//! keyspace.

use std::sync::Arc;

use super::store::{ObjectStoreAdapter, Store, StoreError};

/// Open the store `spec` names.
pub async fn open(spec: &str) -> Result<Arc<dyn Store>, StoreError> {
    let bad = |m: &str| StoreError::Unavailable(format!("store spec {spec:?}: {m}"));
    if spec == "memory" || spec.is_empty() {
        return Ok(Arc::new(ObjectStoreAdapter::in_memory()));
    }
    if let Some(path) = spec.strip_prefix("redb:") {
        #[cfg(feature = "redb")]
        {
            return Ok(Arc::new(super::store_redb::RedbStore::open(path)?));
        }
        #[cfg(not(feature = "redb"))]
        {
            let _ = path;
            return Err(bad("this build has no redb store (feature `redb`)"));
        }
    }
    if let Some(rest) = spec.strip_prefix("tikv://") {
        #[cfg(feature = "tikv")]
        {
            return Ok(Arc::new(
                super::store_tikv::TikvStore::connect(parse_tikv(rest).map_err(|m| bad(&m))?)
                    .await?,
            ));
        }
        #[cfg(not(feature = "tikv"))]
        {
            let _ = rest;
            return Err(bad("this build has no TiKV store (feature `tikv`)"));
        }
    }
    Err(bad(
        "expected memory, redb:<path> or tikv://<pd>[/<namespace>]",
    ))
}

/// Parse the part of a `tikv://` spec after the scheme.
#[cfg(feature = "tikv")]
pub fn parse_tikv(rest: &str) -> Result<super::store_tikv::TikvStoreCfg, String> {
    use super::store_tikv::{TikvStoreCfg, TxnMode};
    let (main, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (hosts, ns) = main.split_once('/').unwrap_or((main, ""));
    let pd_endpoints: Vec<String> = hosts
        .split(',')
        .filter(|h| !h.is_empty())
        .map(str::to_owned)
        .collect();
    if pd_endpoints.is_empty() {
        return Err("no PD endpoint".into());
    }
    let mut cfg = TikvStoreCfg {
        pd_endpoints,
        ..Default::default()
    };
    if !ns.is_empty() {
        cfg.namespace = if ns.ends_with('/') {
            ns.to_owned()
        } else {
            format!("{ns}/")
        };
    }
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        match pair.split_once('=') {
            Some(("mode", "pessimistic")) => cfg.txn_mode = TxnMode::Pessimistic,
            Some(("mode", "optimistic")) => cfg.txn_mode = TxnMode::Optimistic,
            Some(("keyspace", k)) => cfg.keyspace = Some(k.to_owned()),
            _ => return Err(format!("unknown option {pair:?}")),
        }
    }
    Ok(cfg)
}

#[cfg(all(test, feature = "tikv"))]
mod tests {
    use super::*;
    use crate::store_tikv::TxnMode;

    #[test]
    fn parses_a_full_spec() {
        let c = parse_tikv("a:1,b:2/run7?mode=optimistic&keyspace=k").unwrap();
        assert_eq!(c.pd_endpoints, vec!["a:1", "b:2"]);
        assert_eq!(c.namespace, "run7/");
        assert_eq!(c.txn_mode, TxnMode::Optimistic);
        assert_eq!(c.keyspace.as_deref(), Some("k"));
        let d = parse_tikv("a:1").unwrap();
        assert_eq!(d.namespace, "resonate/");
        assert_eq!(d.txn_mode, TxnMode::Pessimistic);
        assert!(parse_tikv("/x").is_err());
    }
}
