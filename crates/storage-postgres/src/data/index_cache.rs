// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Process-wide cache of the index sets the write path reads.
//!
//! Every item write reads the table's GSI/LSI definitions and its vector indexes
//! from the catalog before it opens its data transaction
//! ([`super::index::fetch_write_path_indexes`]). Those definitions change only on
//! DDL, so under a steady write load the two reads return the same rows on every
//! call: two extra round trips and an extra pool checkout per item write.
//!
//! This cache keeps the last read per table and serves it until either its TTL
//! ends or this process changes the catalog. It is off unless
//! `storage.postgres.write_path_index_cache_ttl_ms` is set.
//!
//! # Why a hit is as good as a fresh read
//!
//! A hit stands for a catalog read taken at the moment of the hit, so it returns a
//! [`WriteStamp`] taken then, not at the original read. That is what keeps a hit
//! cheap: an entry older than [`super::gsi_build::GRACE_WRITE`] would otherwise
//! send every write down the slow-write repair path, which reads the catalog again.
//!
//! The claim holds because every operation in this process that changes what the
//! write path reads holds a [`CatalogChange`] guard across its catalog commit:
//!
//! 1. The guard raises `in_flight` before the operation runs.
//! 2. When the guard drops, after the commit, it bumps `generation`, then lowers
//!    `in_flight`.
//!
//! A fill records `generation` *before* its catalog read. A lookup takes its stamp
//! first, then serves the entry only when `in_flight` is zero and `generation` is
//! unchanged from the fill. Suppose some change committed after the fill's read
//! and no later than the stamp. Its guard was raised before that commit, so at the
//! lookup either it is still raised (`in_flight` is not zero) or it has dropped,
//! and then it bumped `generation` before lowering `in_flight` (`generation`
//! differs). Both refuse the entry, so a served entry equals the catalog as of the
//! stamp, which is exactly what a fresh read at the stamp would have returned.
//!
//! # What it does not cover
//!
//! A change made by another process (a second server on the same catalog, or a
//! CLI command beside one) raises no guard here. A writer in this process would
//! then use the old index set for up to the TTL, and because its stamp is fresh
//! the GSI slow-write repair does not cover it: an index built by the other
//! process can miss rows written in that window. Enable the cache only where one
//! server process makes every DDL change on its catalog.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::{Duration, Instant};

use extenddb_storage::vector_lifecycle::VectorIndexMeta;

use super::gsi_build::WriteStamp;
use super::index::IndexMeta;

/// The index sets one write path read returns.
pub(crate) type WritePathIndexes = (Vec<IndexMeta>, Vec<(VectorIndexMeta, String)>);

struct Entry {
    generation: u64,
    filled_at: Instant,
    indexes: Arc<WritePathIndexes>,
}

struct IndexCache {
    /// TTL in milliseconds; zero disables the cache.
    ttl_ms: AtomicU64,
    /// Bumped by every [`CatalogChange`] after its commit.
    generation: AtomicU64,
    /// [`CatalogChange`] guards currently held.
    in_flight: AtomicUsize,
    entries: RwLock<HashMap<String, Entry>>,
}

static CACHE: LazyLock<IndexCache> = LazyLock::new(|| IndexCache {
    ttl_ms: AtomicU64::new(0),
    generation: AtomicU64::new(0),
    in_flight: AtomicUsize::new(0),
    entries: RwLock::new(HashMap::new()),
});

/// Set the cache TTL for this process. `None` or zero turns the cache off and
/// drops every entry.
pub fn set_ttl(ttl: Option<Duration>) {
    let ms = ttl.map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
    CACHE.ttl_ms.store(ms, Ordering::SeqCst);
    if ms == 0 {
        clear();
    }
}

fn ttl() -> Option<Duration> {
    match CACHE.ttl_ms.load(Ordering::SeqCst) {
        0 => None,
        ms => Some(Duration::from_millis(ms)),
    }
}

fn clear() {
    if let Ok(mut entries) = CACHE.entries.write() {
        entries.clear();
    }
}

/// A cached index set for `table_id`, with a stamp taken for this lookup, or
/// `None` when the caller must read the catalog.
pub(crate) fn lookup(table_id: &str) -> Option<(Arc<WritePathIndexes>, WriteStamp)> {
    let ttl = ttl()?;
    // The stamp comes before the checks: the entry is proven current as of the
    // checks, so a stamp taken earlier can only make the write look slower.
    let stamp = WriteStamp::now();
    if CACHE.in_flight.load(Ordering::SeqCst) != 0 {
        return None;
    }
    let generation = CACHE.generation.load(Ordering::SeqCst);
    let entries = CACHE.entries.read().ok()?;
    let entry = entries.get(table_id)?;
    if entry.generation != generation || entry.filled_at.elapsed() >= ttl {
        return None;
    }
    Some((Arc::clone(&entry.indexes), stamp))
}

/// Taken before a catalog read whose result may be cached.
#[derive(Clone, Copy)]
pub(crate) struct FillToken {
    generation: u64,
}

/// Call before the catalog read; pass the token to [`fill`] after it. `None`
/// when the cache is off.
pub(crate) fn begin_fill() -> Option<FillToken> {
    ttl()?;
    Some(FillToken {
        generation: CACHE.generation.load(Ordering::SeqCst),
    })
}

/// Store the result of a catalog read that began with `token`.
pub(crate) fn fill(token: FillToken, table_id: &str, indexes: Arc<WritePathIndexes>) {
    let Ok(mut entries) = CACHE.entries.write() else {
        return;
    };
    entries.insert(
        table_id.to_owned(),
        Entry {
            generation: token.generation,
            filled_at: Instant::now(),
            indexes,
        },
    );
}

/// Held across an operation that changes what the write path reads: index or
/// vector-index rows, their status or definitions, or a table's attribute
/// definitions. See the module docs for why it must span the commit.
#[must_use = "the guard must be held until the catalog change has committed"]
pub(crate) struct CatalogChange(());

/// Raise a [`CatalogChange`] guard for the rest of the caller's scope.
pub(crate) fn catalog_change() -> CatalogChange {
    CACHE.in_flight.fetch_add(1, Ordering::SeqCst);
    CatalogChange(())
}

impl Drop for CatalogChange {
    fn drop(&mut self) {
        CACHE.generation.fetch_add(1, Ordering::SeqCst);
        clear();
        CACHE.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // One test, because the cache is process-wide and tests run in parallel.
    #[test]
    fn serves_until_ttl_or_a_catalog_change() {
        let id = "index-cache-unit-test";
        let set = || Arc::new((Vec::new(), Vec::new()));

        set_ttl(None);
        assert!(begin_fill().is_none(), "off: nothing to fill");
        assert!(lookup(id).is_none(), "off: never a hit");

        set_ttl(Some(Duration::from_secs(60)));
        let token = begin_fill().expect("on");
        fill(token, id, set());
        assert!(lookup(id).is_some(), "a fresh fill is a hit");

        // A change in flight refuses every entry, and its end invalidates them.
        let guard = catalog_change();
        assert!(lookup(id).is_none(), "refused while a change is in flight");
        drop(guard);
        assert!(lookup(id).is_none(), "gone after the change");

        // A read that began before a change and filled after it is stale.
        let token = begin_fill().expect("on");
        drop(catalog_change());
        fill(token, id, set());
        assert!(
            lookup(id).is_none(),
            "a fill that straddles a change is refused"
        );

        let token = begin_fill().expect("on");
        fill(token, id, set());
        assert!(lookup(id).is_some());
        set_ttl(Some(Duration::from_millis(1)));
        std::thread::sleep(Duration::from_millis(5));
        assert!(lookup(id).is_none(), "expired");

        set_ttl(None);
    }
}
