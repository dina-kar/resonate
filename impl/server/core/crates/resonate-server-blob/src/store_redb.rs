//! [`Store`] over a local `redb` file: the durable, single-process backend
//! for development and for the embedded mode.
//!
//! The in-memory store loses everything at exit, and `object_store`'s local
//! filesystem has no conditional replace, so neither gives a developer a
//! server that survives a restart. `redb` (MIT or Apache-2.0) is an embedded,
//! crash-safe B-tree with serializable write transactions: every operation
//! here is one write transaction, which makes the conditional write a plain
//! read-compare-write, and a crash leaves the last committed state.
//!
//! A value is an 8-byte big-endian version followed by the bytes. Versions
//! come from a counter in the same file, so they never repeat.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use super::store::{Etag, Store, StoreError};

const OBJECTS: TableDefinition<&str, &[u8]> = TableDefinition::new("objects");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");
const COUNTER: &str = "version";

pub struct RedbStore {
    db: Arc<Database>,
}

fn unavailable(e: impl std::fmt::Display) -> StoreError {
    StoreError::Unavailable(format!("redb: {e}"))
}

impl RedbStore {
    /// Open or create the file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let db = Database::create(path).map_err(unavailable)?;
        let tx = db.begin_write().map_err(unavailable)?;
        {
            tx.open_table(OBJECTS).map_err(unavailable)?;
            tx.open_table(META).map_err(unavailable)?;
        }
        tx.commit().map_err(unavailable)?;
        Ok(Self { db: Arc::new(db) })
    }

    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Database) -> Result<T, StoreError> + Send + 'static,
    ) -> Result<T, StoreError> {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || f(&db))
            .await
            .map_err(unavailable)?
    }

    /// One write transaction: `check` sees the current version (or `None`) and
    /// says whether to write; `body` of `None` deletes.
    async fn write(
        &self,
        key: &str,
        body: Option<Vec<u8>>,
        check: impl FnOnce(Option<u64>) -> bool + Send + 'static,
    ) -> Result<Etag, StoreError> {
        let key = key.to_owned();
        self.blocking(move |db| {
            let tx = db.begin_write().map_err(unavailable)?;
            let version;
            {
                let mut objects = tx.open_table(OBJECTS).map_err(unavailable)?;
                let current = objects
                    .get(key.as_str())
                    .map_err(unavailable)?
                    .map(|v| u64::from_be_bytes(v.value()[..8].try_into().expect("8 bytes")));
                if !check(current) {
                    return Err(StoreError::PreconditionFailed);
                }
                let mut meta = tx.open_table(META).map_err(unavailable)?;
                let next = meta
                    .get(COUNTER)
                    .map_err(unavailable)?
                    .map(|v| v.value())
                    .unwrap_or(0)
                    + 1;
                meta.insert(COUNTER, next).map_err(unavailable)?;
                version = next;
                match body {
                    Some(body) => {
                        let mut v = Vec::with_capacity(8 + body.len());
                        v.extend_from_slice(&next.to_be_bytes());
                        v.extend_from_slice(&body);
                        objects
                            .insert(key.as_str(), v.as_slice())
                            .map_err(unavailable)?;
                    }
                    None => {
                        objects.remove(key.as_str()).map_err(unavailable)?;
                    }
                }
            }
            tx.commit().map_err(unavailable)?;
            Ok(Etag(version.to_string()))
        })
        .await
    }
}

#[async_trait]
impl Store for RedbStore {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Etag)>, StoreError> {
        let key = key.to_owned();
        self.blocking(move |db| {
            let tx = db.begin_read().map_err(unavailable)?;
            let objects = tx.open_table(OBJECTS).map_err(unavailable)?;
            Ok(objects.get(key.as_str()).map_err(unavailable)?.map(|v| {
                let bytes = v.value();
                let ver = u64::from_be_bytes(bytes[..8].try_into().expect("8 bytes"));
                (bytes[8..].to_vec(), Etag(ver.to_string()))
            }))
        })
        .await
    }

    async fn put_if_match(
        &self,
        key: &str,
        body: Vec<u8>,
        etag: &Etag,
    ) -> Result<Etag, StoreError> {
        let want = etag.0.clone();
        self.write(key, Some(body), move |cur| {
            cur.map(|v| v.to_string()) == Some(want)
        })
        .await
    }

    async fn put_if_none_match(&self, key: &str, body: Vec<u8>) -> Result<Etag, StoreError> {
        self.write(key, Some(body), |cur| cur.is_none()).await
    }

    async fn put(&self, key: &str, body: Vec<u8>) -> Result<Etag, StoreError> {
        self.write(key, Some(body), |_| true).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.write(key, None, |_| true).await.map(|_| ())
    }

    async fn list(&self, prefix: &str, max_keys: usize) -> Result<Vec<String>, StoreError> {
        let prefix = prefix.to_owned();
        self.blocking(move |db| {
            let tx = db.begin_read().map_err(unavailable)?;
            let objects = tx.open_table(OBJECTS).map_err(unavailable)?;
            let mut out = Vec::new();
            for entry in objects.range(prefix.as_str()..).map_err(unavailable)? {
                if out.len() >= max_keys {
                    break;
                }
                let (k, _) = entry.map_err(unavailable)?;
                let k = k.value();
                if !k.starts_with(prefix.as_str()) {
                    break;
                }
                out.push(k.to_owned());
            }
            Ok(out)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn conditional_writes_and_ordered_lists() {
        let dir = std::env::temp_dir().join(format!("resonate-redb-{:016x}", fastrand::u64(..)));
        let store = RedbStore::open(&dir).unwrap();
        assert_eq!(store.get("a").await.unwrap(), None);
        let e1 = store.put_if_none_match("a", b"1".to_vec()).await.unwrap();
        assert_eq!(
            store.put_if_none_match("a", b"x".to_vec()).await,
            Err(StoreError::PreconditionFailed)
        );
        let e2 = store.put_if_match("a", b"2".to_vec(), &e1).await.unwrap();
        assert_ne!(e1, e2);
        assert_eq!(
            store.put_if_match("a", b"3".to_vec(), &e1).await,
            Err(StoreError::PreconditionFailed)
        );
        store.delete("a").await.unwrap();
        // A re-create never reuses a version, so a stale etag cannot win.
        let e3 = store.put_if_none_match("a", b"4".to_vec()).await.unwrap();
        assert_ne!(e3, e1);
        assert_eq!(
            store.put_if_match("a", b"5".to_vec(), &e1).await,
            Err(StoreError::PreconditionFailed)
        );
        for k in ["t/2", "t/1", "u/1", "t/3"] {
            store.put(k, vec![]).await.unwrap();
        }
        assert_eq!(store.list("t/", 2).await.unwrap(), vec!["t/1", "t/2"]);
        drop(store);
        let reopened = RedbStore::open(&dir).unwrap();
        assert_eq!(reopened.get("a").await.unwrap().unwrap().0, b"4");
        let _ = std::fs::remove_file(&dir);
    }
}
