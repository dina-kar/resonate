//! [`Store`] over TiKV's transactional API, through Loam's `tikv-client` fork.
//!
//! # Why the blob server, and why this port
//!
//! The blob server already commits every transition as one conditional write
//! of one document per origin (see [`store`](super::store)). TiKV gives the
//! same thing without an object store: a transaction that reads a key, checks
//! its version and writes it commits atomically, or reports that someone else
//! wrote first. So this module is the whole TiKV backend: six operations of
//! the [`Store`] contract, no SQL layer and no `tidb-server` in the path.
//!
//! # Encoding
//!
//! Every key lives under a configurable namespace (`resonate/` by default),
//! and optionally inside an API v2 keyspace. A value is an 8-byte big-endian
//! **version** followed by the object's bytes. The version is the start
//! timestamp of the transaction that wrote it: PD's TSO hands out each one
//! once, so a version never repeats, even after a delete and a re-create.
//! That is stronger than S3's content-hash ETags, which do repeat when the
//! same bytes are written twice. The [`Etag`] is the version in decimal.
//!
//! # Error taxonomy
//!
//! - A **write conflict** in an optimistic transaction means another writer
//!   committed first, and nothing of ours landed: [`StoreError::PreconditionFailed`],
//!   so the caller re-decides.
//! - A version mismatch that we observe ourselves is also
//!   [`StoreError::PreconditionFailed`].
//! - An **undetermined** commit (the primary's commit result was lost), a
//!   failed pessimistic lock or a lock-wait timeout leaves nothing known, or
//!   nothing written: [`StoreError::Conflict`], so the caller retries the same
//!   conditional write. If the first attempt did land, the retry sees the new
//!   version and falls into the re-decide path, exactly as with S3.
//! - Everything else is [`StoreError::Unavailable`], a 503 to the caller.
//!
//! # Transaction mode
//!
//! Pessimistic by default: `get_for_update` locks the key before the version
//! check, so concurrent writers to one origin queue instead of aborting.
//! Optimistic mode is kept for comparison and for the benchmark matrix.

use std::sync::Arc;

use async_trait::async_trait;
use tikv_client::{
    BoundRange, CheckLevel, Config as TikvConfig, Error as TikvError, Key, TimestampExt,
    Transaction, TransactionClient, TransactionOptions,
};

use super::store::{Etag, Store, StoreError};

/// How transactions take their locks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TxnMode {
    /// Lock on read (`get_for_update`); writers to one key queue.
    #[default]
    Pessimistic,
    /// Detect conflicts at prewrite; the loser aborts.
    Optimistic,
}

/// How to reach TiKV and where to put the keys.
#[derive(Debug, Clone)]
pub struct TikvStoreCfg {
    /// PD endpoints, `host:port`.
    pub pd_endpoints: Vec<String>,
    /// API v2 keyspace; `None` for API v1 (a cluster without keyspaces).
    pub keyspace: Option<String>,
    /// Namespace every key is written under, e.g. `resonate/`.
    pub namespace: String,
    pub txn_mode: TxnMode,
    /// Per-request timeout for the client.
    pub timeout: std::time::Duration,
}

impl Default for TikvStoreCfg {
    fn default() -> Self {
        Self {
            pd_endpoints: vec!["127.0.0.1:2379".into()],
            keyspace: None,
            namespace: "resonate/".into(),
            txn_mode: TxnMode::Pessimistic,
            timeout: std::time::Duration::from_secs(5),
        }
    }
}

/// The TiKV store.
pub struct TikvStore {
    client: Arc<TransactionClient>,
    namespace: Vec<u8>,
    mode: TxnMode,
}

const VERSION_LEN: usize = 8;

impl TikvStore {
    /// Connect to PD. Fails if no PD endpoint answers.
    pub async fn connect(cfg: TikvStoreCfg) -> Result<Self, StoreError> {
        let mut config = TikvConfig::default().with_timeout(cfg.timeout);
        if let Some(ks) = &cfg.keyspace {
            config = config.with_keyspace(ks);
        }
        let client = TransactionClient::new_with_config(cfg.pd_endpoints.clone(), config)
            .await
            .map_err(|e| StoreError::Unavailable(format!("cannot connect to PD: {e}")))?;
        Ok(Self {
            client: Arc::new(client),
            namespace: cfg.namespace.into_bytes(),
            mode: cfg.txn_mode,
        })
    }

    fn key(&self, key: &str) -> Key {
        let mut k = self.namespace.clone();
        k.extend_from_slice(key.as_bytes());
        k.into()
    }

    fn strip(&self, key: &Key) -> Option<String> {
        let bytes: &[u8] = key.into();
        bytes
            .strip_prefix(self.namespace.as_slice())
            .and_then(|rest| String::from_utf8(rest.to_vec()).ok())
    }

    fn options(&self) -> TransactionOptions {
        let base = match self.mode {
            TxnMode::Pessimistic => TransactionOptions::new_pessimistic(),
            TxnMode::Optimistic => TransactionOptions::new_optimistic(),
        };
        // Every path below commits or rolls back; a forgotten one must not
        // take the process down.
        base.drop_check(CheckLevel::Warn)
    }

    async fn begin(&self) -> Result<Transaction, StoreError> {
        self.client
            .begin_with_options(self.options())
            .await
            .map_err(classify)
    }

    /// A read-only snapshot at a fresh timestamp: linearizable reads, since the
    /// TSO is ahead of every commit already acknowledged.
    async fn snapshot(&self) -> Result<tikv_client::Snapshot, StoreError> {
        let ts = self.client.current_timestamp().await.map_err(classify)?;
        Ok(self.client.snapshot(
            ts,
            TransactionOptions::new_optimistic()
                .read_only()
                .drop_check(CheckLevel::None),
        ))
    }

    /// The conditional write: read (locking in pessimistic mode), check, write,
    /// commit. `expect` is `None` for "must be absent".
    async fn cas(
        &self,
        key: &str,
        body: Vec<u8>,
        expect: Option<&Etag>,
    ) -> Result<Etag, StoreError> {
        let k = self.key(key);
        let mut txn = self.begin().await?;
        let current = match self.mode {
            TxnMode::Pessimistic => txn.get_for_update(k.clone()).await,
            TxnMode::Optimistic => txn.get(k.clone()).await,
        };
        let current = match current {
            Ok(v) => v,
            Err(e) => {
                let _ = txn.rollback().await;
                return Err(classify(e));
            }
        };
        let matches = match (expect, &current) {
            (None, None) => true,
            (None, Some(_)) => false,
            (Some(_), None) => false,
            (Some(etag), Some(v)) => decode(v).map(|(ver, _)| ver.to_string() == etag.0)?,
        };
        if !matches {
            let _ = txn.rollback().await;
            return Err(StoreError::PreconditionFailed);
        }
        let version = txn.start_timestamp().version();
        if let Err(e) = txn.put(k, encode(version, &body)).await {
            let _ = txn.rollback().await;
            return Err(classify(e));
        }
        commit(txn).await?;
        Ok(Etag(version.to_string()))
    }
}

async fn commit(mut txn: Transaction) -> Result<(), StoreError> {
    match txn.commit().await {
        Ok(_) => Ok(()),
        Err(e) => {
            // A failed prewrite leaves locks the client cleans up on rollback;
            // an undetermined commit must not be rolled back (it may have
            // landed), and rollback refuses it anyway.
            let err = classify(e);
            if err != StoreError::Conflict {
                let _ = txn.rollback().await;
            }
            Err(err)
        }
    }
}

fn encode(version: u64, body: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(VERSION_LEN + body.len());
    v.extend_from_slice(&version.to_be_bytes());
    v.extend_from_slice(body);
    v
}

fn decode(value: &[u8]) -> Result<(u64, &[u8]), StoreError> {
    if value.len() < VERSION_LEN {
        return Err(StoreError::Unavailable(format!(
            "corrupt value: {} bytes, shorter than its version header",
            value.len()
        )));
    }
    let (head, body) = value.split_at(VERSION_LEN);
    Ok((u64::from_be_bytes(head.try_into().expect("8 bytes")), body))
}

/// Map a client error onto the store taxonomy.
pub fn classify(e: TikvError) -> StoreError {
    if is_write_conflict(&e) {
        return StoreError::PreconditionFailed;
    }
    if is_retryable_unknown(&e) {
        return StoreError::Conflict;
    }
    StoreError::Unavailable(e.to_string())
}

fn is_write_conflict(e: &TikvError) -> bool {
    match e {
        TikvError::KeyError(ke) => ke.conflict.is_some() || ke.already_exist.is_some(),
        TikvError::MultipleKeyErrors(es) | TikvError::ExtractedErrors(es) => {
            !es.is_empty() && es.iter().all(is_write_conflict)
        }
        TikvError::DuplicateKeyInsertion => true,
        _ => false,
    }
}

fn is_retryable_unknown(e: &TikvError) -> bool {
    match e {
        TikvError::UndeterminedError(_) => true,
        TikvError::PessimisticLockError { .. } => true,
        TikvError::ResolveLockError(_) => true,
        TikvError::KeyError(ke) => {
            ke.deadlock.is_some() || ke.locked.is_some() || ke.retryable.len() > 0
        }
        TikvError::MultipleKeyErrors(es) | TikvError::ExtractedErrors(es) => es
            .iter()
            .any(|e| is_retryable_unknown(e) || is_write_conflict(e)),
        _ => false,
    }
}

#[async_trait]
impl Store for TikvStore {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Etag)>, StoreError> {
        let mut snap = self.snapshot().await?;
        match snap.get(self.key(key)).await.map_err(classify)? {
            None => Ok(None),
            Some(v) => {
                let (ver, body) = decode(&v)?;
                Ok(Some((body.to_vec(), Etag(ver.to_string()))))
            }
        }
    }

    async fn put_if_match(
        &self,
        key: &str,
        body: Vec<u8>,
        etag: &Etag,
    ) -> Result<Etag, StoreError> {
        self.cas(key, body, Some(etag)).await
    }

    async fn put_if_none_match(&self, key: &str, body: Vec<u8>) -> Result<Etag, StoreError> {
        self.cas(key, body, None).await
    }

    async fn put(&self, key: &str, body: Vec<u8>) -> Result<Etag, StoreError> {
        let mut txn = self.begin().await?;
        let version = txn.start_timestamp().version();
        if let Err(e) = txn.put(self.key(key), encode(version, &body)).await {
            let _ = txn.rollback().await;
            return Err(classify(e));
        }
        match commit(txn).await {
            Ok(()) => Ok(Etag(version.to_string())),
            // A blind write that lost a race to another blind write is still
            // idempotent for this store's callers (timer keys carry their
            // value in the key), so it is retried by the caller like any
            // unknown outcome.
            Err(StoreError::PreconditionFailed) => Err(StoreError::Conflict),
            Err(e) => Err(e),
        }
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        let mut txn = self.begin().await?;
        if let Err(e) = txn.delete(self.key(key)).await {
            let _ = txn.rollback().await;
            return Err(classify(e));
        }
        match commit(txn).await {
            Ok(()) => Ok(()),
            Err(StoreError::PreconditionFailed) => Err(StoreError::Conflict),
            Err(e) => Err(e),
        }
    }

    async fn list(&self, prefix: &str, max_keys: usize) -> Result<Vec<String>, StoreError> {
        if max_keys == 0 {
            return Ok(Vec::new());
        }
        let start: Vec<u8> = self.key(prefix).into();
        let end = prefix_end(&start);
        let mut snap = self.snapshot().await?;
        let limit = u32::try_from(max_keys).unwrap_or(u32::MAX);
        let range: BoundRange = match end {
            Some(end) => (start..end).into(),
            None => (start..).into(),
        };
        let keys = snap.scan_keys(range, limit).await.map_err(classify)?;
        // TiKV scans in key order, which is what the timer poller relies on.
        Ok(keys.filter_map(|k| self.strip(&k)).collect())
    }
}

/// The smallest key greater than every key starting with `prefix`, or `None`
/// if there is none (the prefix is all `0xff`).
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoding_round_trips() {
        let v = encode(42, b"hello");
        let (ver, body) = decode(&v).unwrap();
        assert_eq!(ver, 42);
        assert_eq!(body, b"hello");
        assert!(decode(b"short").is_err());
    }

    #[test]
    fn prefix_end_is_the_next_key() {
        assert_eq!(prefix_end(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(prefix_end(b"a\xff"), Some(b"b".to_vec()));
        assert_eq!(prefix_end(b"\xff\xff"), None);
    }
}
