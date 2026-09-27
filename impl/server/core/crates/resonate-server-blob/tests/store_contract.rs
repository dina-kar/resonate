//! Loam: the [`Store`] contract against whatever `TEST_BLOB_STORE` names
//! (`redb:<path>`, `tikv://<pd>`), including a concurrent compare-and-swap
//! race that would expose a lost update.
//!
//! Skipped when `TEST_BLOB_STORE` is unset or `memory`:
//!
//! ```text
//! TEST_BLOB_STORE=tikv://127.0.0.1:2379 \
//!   cargo test -p resonate-server-blob --features tikv --test store_contract -- --nocapture
//! ```

use std::sync::Arc;

use resonate_server_blob::backends;
use resonate_server_blob::store::{Store, StoreError};

/// `name` keeps each test on its own redb file: redb allows one open handle
/// per file, and the tests run concurrently in one process.
async fn store(name: &str) -> Option<(Arc<dyn Store>, String)> {
    let mut spec = std::env::var("TEST_BLOB_STORE").ok()?;
    if spec.is_empty() || spec == "memory" {
        return None;
    }
    if spec.starts_with("redb:") {
        spec = format!("{spec}.{name}");
    }
    let store = backends::open(&spec).await.expect("TEST_BLOB_STORE");
    let prefix = format!("contract-{:016x}/", fastrand::u64(..));
    eprintln!("[contract] store={spec} prefix={prefix}");
    Some((store, prefix))
}

#[tokio::test(flavor = "multi_thread")]
async fn conditional_writes_behave_as_compare_and_swap() {
    let Some((s, p)) = store("cas").await else {
        eprintln!("[contract] TEST_BLOB_STORE not set — skipped");
        return;
    };
    let k = format!("{p}doc");
    assert_eq!(s.get(&k).await.unwrap(), None);
    let e1 = s.put_if_none_match(&k, b"one".to_vec()).await.unwrap();
    let (body, read) = s.get(&k).await.unwrap().unwrap();
    assert_eq!(body, b"one");
    assert_eq!(read, e1, "the etag a read reports is the one put returned");
    assert_eq!(
        s.put_if_none_match(&k, b"two".to_vec()).await,
        Err(StoreError::PreconditionFailed)
    );
    let e2 = s.put_if_match(&k, b"two".to_vec(), &e1).await.unwrap();
    assert_ne!(e1, e2);
    assert_eq!(
        s.put_if_match(&k, b"stale".to_vec(), &e1).await,
        Err(StoreError::PreconditionFailed)
    );
    s.delete(&k).await.unwrap();
    s.delete(&k).await.unwrap(); // deleting what is not there succeeds
    assert_eq!(s.get(&k).await.unwrap(), None);
    // A re-create gets a fresh version: the old etag cannot win.
    let e3 = s.put_if_none_match(&k, b"three".to_vec()).await.unwrap();
    assert_ne!(e3, e1);
    assert_eq!(
        s.put_if_match(&k, b"x".to_vec(), &e1).await,
        Err(StoreError::PreconditionFailed)
    );
    s.delete_prefix(&p).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn lists_are_ordered_bounded_and_read_after_write() {
    let Some((s, p)) = store("list").await else {
        return;
    };
    for d in [30u32, 10, 20, 50, 40] {
        s.put(&format!("{p}t/{d:010}"), vec![]).await.unwrap();
    }
    s.put(&format!("{p}u/0000000001"), vec![]).await.unwrap();
    let got = s.list(&format!("{p}t/"), 3).await.unwrap();
    assert_eq!(
        got,
        vec![
            format!("{p}t/0000000010"),
            format!("{p}t/0000000020"),
            format!("{p}t/0000000030")
        ]
    );
    assert_eq!(
        s.list(&format!("{p}t/"), 0).await.unwrap(),
        Vec::<String>::new()
    );
    s.delete_prefix(&p).await.unwrap();
    assert!(s.list(&p, 10).await.unwrap().is_empty());
}

/// N writers each add 1 to a counter by read–CAS, retrying on
/// `PreconditionFailed` (re-read) and `Conflict` (same write). No update may
/// be lost: the counter must end at exactly N × rounds.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_cas_loses_no_update() {
    let Some((s, p)) = store("race").await else {
        return;
    };
    let k = format!("{p}counter");
    s.put_if_none_match(&k, b"0".to_vec()).await.unwrap();
    const WRITERS: usize = 8;
    const ROUNDS: usize = 25;
    let mut handles = Vec::new();
    for _ in 0..WRITERS {
        let s = Arc::clone(&s);
        let k = k.clone();
        handles.push(tokio::spawn(async move {
            let mut lost_races = 0u32;
            for _ in 0..ROUNDS {
                loop {
                    let (body, etag) = s.get(&k).await.unwrap().unwrap();
                    let n: u64 = std::str::from_utf8(&body).unwrap().parse().unwrap();
                    let mut attempt = s
                        .put_if_match(&k, (n + 1).to_string().into_bytes(), &etag)
                        .await;
                    while attempt == Err(StoreError::Conflict) {
                        attempt = s
                            .put_if_match(&k, (n + 1).to_string().into_bytes(), &etag)
                            .await;
                    }
                    match attempt {
                        Ok(_) => break,
                        Err(StoreError::PreconditionFailed) => lost_races += 1,
                        Err(e) => panic!("unexpected {e}"),
                    }
                }
            }
            lost_races
        }));
    }
    let mut races = 0;
    for h in handles {
        races += h.await.unwrap();
    }
    let (body, _) = s.get(&k).await.unwrap().unwrap();
    let n: u64 = std::str::from_utf8(&body).unwrap().parse().unwrap();
    eprintln!("[contract] counter={n} lost_races={races}");
    assert_eq!(n, (WRITERS * ROUNDS) as u64, "a CAS update was lost");
    s.delete_prefix(&p).await.unwrap();
}
