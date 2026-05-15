// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Background workers spawned by `extenddb serve`.
//!
//! Each function runs as a `tokio::spawn`-ed task for the lifetime of the
//! server process. Workers handle log-level polling, control-plane transitions,
//! TTL cleanup, table size refresh, stream record expiry, idempotency token
//! cleanup, capacity warning, and metrics pruning.
//!
//! Workers are generic over storage traits so they are decoupled from the
//! concrete `PostgresEngine` / `PostgresCatalogStore` types.

use std::sync::Arc;

use extenddb_core::throttle::ThrottleManager;
use extenddb_storage::management_store::{MetricsStore, RateLimitStore, SettingsStore};
use extenddb_storage::{DataEngine, MetadataEngine, StreamEngine, WorkerStore};
use tracing_subscriber::{EnvFilter, reload};

/// Poll the `log_level` and `sqlx_log_level` settings from the database
/// and reload the tracing filter when either changes (D-22, D-3).
/// The combined filter is `{log_level},sqlx={sqlx_log_level}`.
/// Falls back to `config_level` when `log_level` is absent from the DB.
/// Runs until the process exits.
pub(crate) async fn poll_log_level<S: SettingsStore>(
    store: Arc<S>,
    handle: reload::Handle<EnvFilter, tracing_subscriber::Registry>,
    config_level: String,
) {
    use std::time::Duration;

    const POLL_INTERVAL: Duration = Duration::from_secs(30);
    let mut current_level = config_level;
    let mut current_sqlx_level = String::from("warn");

    loop {
        tokio::time::sleep(POLL_INTERVAL).await;

        let (log_result, sqlx_result) = tokio::join!(
            store.get_setting("log_level"),
            store.get_setting("sqlx_log_level"),
        );

        let new_level = match log_result {
            Ok(Some(v)) => v,
            Ok(None) => current_level.clone(),
            Err(_) => {
                tracing::debug!("Failed to query log_level setting");
                continue;
            }
        };

        let new_sqlx_level = match sqlx_result {
            Ok(Some(v)) => v,
            Ok(None) => current_sqlx_level.clone(),
            Err(_) => {
                tracing::debug!("Failed to query sqlx_log_level setting");
                continue;
            }
        };

        if new_level == current_level && new_sqlx_level == current_sqlx_level {
            continue;
        }

        // D-3: Combined filter encodes both levels.
        let filter_str = format!("{new_level},sqlx={new_sqlx_level}");

        match EnvFilter::try_new(&filter_str) {
            Ok(new_filter) => {
                // H-4: Log at warn so the message is visible even when
                // switching to a more restrictive level (e.g. debug → error).
                if new_level != current_level {
                    tracing::warn!("Log level changing to '{new_level}' (from settings table)");
                }
                if new_sqlx_level != current_sqlx_level {
                    tracing::warn!(
                        "sqlx log level changing to '{new_sqlx_level}' (from settings table)"
                    );
                }
                if let Err(e) = handle.reload(new_filter) {
                    tracing::warn!("Failed to reload log filter: {e}");
                } else {
                    current_level = new_level;
                    current_sqlx_level = new_sqlx_level;
                }
            }
            Err(e) => {
                tracing::warn!("Invalid log filter '{filter_str}': {e}");
            }
        }
    }
}

/// Poll the `throttling_enabled` runtime setting and update the
/// `ThrottleManager` when it changes. This allows enabling/disabling
/// throttling at runtime via `extenddb settings set throttling_enabled true`.
pub(crate) async fn poll_throttling_enabled<S: SettingsStore>(
    store: Arc<S>,
    throttle: Arc<ThrottleManager>,
    config_enabled: bool,
) {
    use std::time::Duration;

    const POLL_INTERVAL: Duration = Duration::from_secs(30);
    let mut current = config_enabled;

    loop {
        tokio::time::sleep(POLL_INTERVAL).await;

        let new_enabled = match store.get_setting("throttling_enabled").await {
            Ok(Some(v)) => v == "true",
            Ok(None) => config_enabled,
            Err(_) => {
                tracing::debug!("Failed to query throttling_enabled setting");
                continue;
            }
        };

        if new_enabled != current {
            tracing::warn!(
                "Throttling {} (from settings table)",
                if new_enabled { "enabled" } else { "disabled" }
            );
            throttle.set_enabled(new_enabled);
            current = new_enabled;
        }
    }
}

/// F-3: Event-driven control plane transition poller.
///
/// Blocks on a `Notify` until a `CreateTable` or `DeleteTable` wakes it.
/// Once woken, polls every second until no work remains, then returns
/// to idle. A 60-second timeout provides a defensive sweep even if a
/// notification is missed.
pub(crate) async fn poll_control_plane_transitions<W: WorkerStore, S: SettingsStore>(
    storage: Arc<W>,
    notify: Arc<tokio::sync::Notify>,
    settings: Arc<S>,
) {
    use std::time::Duration;

    const ACTIVE_POLL: Duration = Duration::from_secs(1);
    const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
    const MARGIN_SECS: f64 = 5.0;

    loop {
        // Idle: wait for a wake signal or timeout (defensive sweep).
        let _ = tokio::time::timeout(IDLE_TIMEOUT, notify.notified()).await;

        // Read control_plane_delay_seconds from settings to compute active window.
        let delay_secs = read_control_plane_delay(&*settings).await;
        let active_window = Duration::from_secs_f64(delay_secs + MARGIN_SECS);

        // Active: poll every second for active_window. The notification fires
        // at commit time, but the transition is scheduled delay-seconds in the
        // future. We must keep polling until the transition actually fires.
        let deadline = tokio::time::Instant::now() + active_window;
        loop {
            match storage.process_control_plane_transitions().await {
                Ok(ref t) if t.is_empty() => {}
                Ok(transitions) => {
                    // D-4: Log meaningful state changes, not poll ticks.
                    for (name, transition) in &transitions {
                        tracing::info!("Table '{name}': {transition}");
                    }
                }
                Err(e) => {
                    tracing::warn!("Control plane transition poll failed: {e}");
                    break;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(ACTIVE_POLL).await;
        }
    }
}

/// Read `control_plane_delay_seconds` from the settings store.
/// Returns 0.25 on any error (store unreachable, missing key, parse failure).
async fn read_control_plane_delay<S: SettingsStore + ?Sized>(store: &S) -> f64 {
    store
        .get_setting("control_plane_delay_seconds")
        .await
        .ok()
        .flatten()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|&v| v >= 0.0)
        .unwrap_or(0.25)
}

/// REQ-CTRL-004: Background worker that periodically recomputes
/// `TableSizeBytes` and `ItemCount` for all active tables across all accounts.
pub(crate) async fn table_size_refresh_worker<E: MetadataEngine>(storage: Arc<E>) {
    use std::time::Duration;

    // DynamoDB updates these approximately every 6 hours. We use 5 minutes
    // for faster feedback in local development.
    const REFRESH_INTERVAL: Duration = Duration::from_secs(300);

    loop {
        tokio::time::sleep(REFRESH_INTERVAL).await;

        let tables = match storage.all_active_tables().await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!("Size refresh worker: failed to list tables: {e}");
                continue;
            }
        };

        for (account_id, table_name) in &tables {
            if let Err(e) = storage.refresh_table_size(account_id, table_name).await {
                tracing::warn!("Size refresh worker: failed for {table_name}: {e}");
            }
        }
    }
}

/// Background worker that periodically deletes expired stream records.
/// `DynamoDB` retains stream records for 24 hours.
pub(crate) async fn stream_record_cleanup_worker<E: StreamEngine>(
    storage: Arc<E>,
    metrics: Arc<extenddb_core::metrics::MetricsCollector>,
) {
    use extenddb_core::metrics::QuerySource;
    use std::time::Duration;

    const CLEANUP_INTERVAL: Duration = Duration::from_secs(3600);
    const RETENTION_HOURS: i64 = 24;

    loop {
        tokio::time::sleep(CLEANUP_INTERVAL).await;
        let cycle_start = std::time::Instant::now();
        match storage
            .cleanup_expired_stream_records(RETENTION_HOURS)
            .await
        {
            Ok(0) => {
                #[allow(clippy::cast_precision_loss)]
                let cycle_us = cycle_start.elapsed().as_micros() as f64;
                metrics.record_worker_success(QuerySource::StreamCleanup, cycle_us);
            }
            Ok(n) => {
                tracing::info!("Stream cleanup worker: deleted {n} expired record(s)");
                #[allow(clippy::cast_precision_loss)]
                let cycle_us = cycle_start.elapsed().as_micros() as f64;
                metrics.record_worker_success(QuerySource::StreamCleanup, cycle_us);
            }
            Err(e) => {
                tracing::warn!("Stream cleanup worker: {e}");
                metrics.record_worker_error(QuerySource::StreamCleanup);
            }
        }
    }
}

/// Background worker that deletes expired idempotency tokens (>10 min old).
pub(crate) async fn idempotency_token_cleanup_worker<E: DataEngine>(
    storage: Arc<E>,
    metrics: Arc<extenddb_core::metrics::MetricsCollector>,
) {
    use extenddb_core::metrics::QuerySource;
    use std::time::Duration;

    const CLEANUP_INTERVAL: Duration = Duration::from_secs(600);
    const MAX_AGE_SECONDS: i64 = 600;

    loop {
        tokio::time::sleep(CLEANUP_INTERVAL).await;
        let cycle_start = std::time::Instant::now();
        match storage
            .cleanup_expired_idempotency_tokens(MAX_AGE_SECONDS)
            .await
        {
            Ok(0) => {
                #[allow(clippy::cast_precision_loss)]
                let cycle_us = cycle_start.elapsed().as_micros() as f64;
                metrics.record_worker_success(QuerySource::IdempotencyCleanup, cycle_us);
            }
            Ok(n) => {
                tracing::info!("Idempotency cleanup worker: deleted {n} expired token(s)");
                #[allow(clippy::cast_precision_loss)]
                let cycle_us = cycle_start.elapsed().as_micros() as f64;
                metrics.record_worker_success(QuerySource::IdempotencyCleanup, cycle_us);
            }
            Err(e) => {
                tracing::warn!("Idempotency cleanup worker: {e}");
                metrics.record_worker_error(QuerySource::IdempotencyCleanup);
            }
        }
    }
}

/// Background worker that periodically logs a warning when requests use
/// approximate consumed capacity information.
///
/// Phase 11a: `ConsumedCapacity` returns plausible stubs, not real values.
/// This worker reads and resets the counter on a fixed interval and emits
/// a single log line summarizing usage since the last tick.
pub(crate) async fn capacity_warning_worker() {
    use extenddb_engine::capacity_helpers::CAPACITY_REQUEST_COUNT;
    use std::time::Duration;

    const WARNING_INTERVAL: Duration = Duration::from_secs(3600);

    loop {
        tokio::time::sleep(WARNING_INTERVAL).await;

        let count = CAPACITY_REQUEST_COUNT.swap(0, std::sync::atomic::Ordering::Relaxed);
        if count > 0 {
            tracing::warn!(
                "{count} request(s) used approximate consumed capacity information in the last {} seconds",
                WARNING_INTERVAL.as_secs(),
            );
        }
    }
}

/// Periodically prune metrics data points older than 1 day.
pub(crate) async fn metrics_prune_worker(metrics: Arc<extenddb_core::metrics::MetricsCollector>) {
    use extenddb_core::metrics::QuerySource;

    const PRUNE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);
    loop {
        tokio::time::sleep(PRUNE_INTERVAL).await;
        let cycle_start = std::time::Instant::now();
        metrics.prune();
        #[allow(clippy::cast_precision_loss)]
        let cycle_us = cycle_start.elapsed().as_micros() as f64;
        metrics.record_worker_success(QuerySource::MetricsPrune, cycle_us);
    }
}

/// Periodically flush in-memory metrics to the database.
///
/// Drains data points older than 60 seconds, aggregates them into 1-minute
/// buckets, and upserts via the `MetricsStore` trait. Also prunes DB rows
/// older than 24 hours.
pub(crate) async fn metrics_flush_worker<M: MetricsStore>(
    metrics: Arc<extenddb_core::metrics::MetricsCollector>,
    store: Arc<M>,
) {
    use extenddb_core::metrics::QuerySource;
    use extenddb_storage::management_store::MetricsRow;

    const FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
    const RETENTION: std::time::Duration = std::time::Duration::from_secs(86400);
    loop {
        tokio::time::sleep(FLUSH_INTERVAL).await;
        let cycle_start = std::time::Instant::now();
        let buckets = metrics.drain(FLUSH_INTERVAL);
        if !buckets.is_empty() {
            let rows: Vec<MetricsRow> = buckets
                .iter()
                .map(|b| {
                    let secs = b
                        .bucket
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    #[allow(clippy::cast_possible_wrap)]
                    let bucket_ts = time::OffsetDateTime::from_unix_timestamp(secs as i64)
                        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
                    MetricsRow {
                        bucket: bucket_ts,
                        metric: b.metric.to_string(),
                        table_name: if b.table_name.is_empty() {
                            None
                        } else {
                            Some(b.table_name.clone())
                        },
                        index_name: if b.index_name.is_empty() {
                            None
                        } else {
                            Some(b.index_name.clone())
                        },
                        operation: if b.operation.is_empty() {
                            None
                        } else {
                            Some(b.operation.clone())
                        },
                        sum: b.sum,
                        count: i64::try_from(b.count).unwrap_or(i64::MAX),
                        min: b.min,
                        max: b.max,
                    }
                })
                .collect();
            // insert_metrics logs per-row failures internally and always returns Ok.
            let _ = store.insert_metrics(&rows).await;
        }
        // Prune old DB rows.
        let mut errored = false;
        if let Err(e) = store.prune_metrics(RETENTION).await {
            tracing::warn!("Failed to prune old metrics from DB: {e:?}");
            metrics.record_worker_error(QuerySource::MetricsFlush);
            errored = true;
        }
        if !errored {
            #[allow(clippy::cast_precision_loss)]
            let cycle_us = cycle_start.elapsed().as_micros() as f64;
            metrics.record_worker_success(QuerySource::MetricsFlush, cycle_us);
        }
    }
}

/// Background worker that deletes old login attempt records.
pub(crate) async fn login_attempt_cleanup_worker<R: RateLimitStore>(store: Arc<R>) {
    use std::time::Duration;

    const CLEANUP_INTERVAL: Duration = Duration::from_secs(3600);
    // Keep records for 24 hours for audit purposes.
    const MAX_AGE_SECONDS: i64 = 86400;

    loop {
        tokio::time::sleep(CLEANUP_INTERVAL).await;
        store.cleanup_old_attempts(MAX_AGE_SECONDS).await;
    }
}

/// P119: Poll `gsi_propagation_delay_ms` from the settings table and update
/// the cached `AtomicU64` in `PostgresEngine`. Runs every 30 seconds.
/// On failure, retains the last known good value and logs a debug message.
pub(crate) async fn poll_gsi_delay<S: SettingsStore>(
    store: Arc<S>,
    cached: Arc<std::sync::atomic::AtomicU64>,
) {
    use std::time::Duration;

    const POLL_INTERVAL: Duration = Duration::from_secs(30);

    loop {
        tokio::time::sleep(POLL_INTERVAL).await;

        match store.get_setting("gsi_propagation_delay_ms").await {
            Ok(Some(v)) => {
                if let Ok(ms) = v.parse::<u64>() {
                    cached.store(ms, std::sync::atomic::Ordering::Relaxed);
                }
            }
            Ok(None) => {
                // Setting removed — revert to default.
                cached.store(10, std::sync::atomic::Ordering::Relaxed);
            }
            Err(_) => {
                tracing::debug!("Failed to query gsi_propagation_delay_ms setting");
            }
        }
    }
}

/// P120d: Sample connection pool utilization every 5 seconds and record
/// gauge metrics for active/idle connections.
pub(crate) async fn pool_metrics_worker(
    catalog_pool: sqlx::PgPool,
    data_pool: sqlx::PgPool,
    metrics: Arc<extenddb_core::metrics::MetricsCollector>,
) {
    use std::time::Duration;

    const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

    loop {
        tokio::time::sleep(SAMPLE_INTERVAL).await;

        let catalog_size = catalog_pool.size() as usize;
        let catalog_idle = catalog_pool.num_idle();
        let data_size = data_pool.size() as usize;
        let data_idle = data_pool.num_idle();

        // Combined pool stats (catalog + data).
        let total_active =
            (catalog_size.saturating_sub(catalog_idle)) + (data_size.saturating_sub(data_idle));
        let total_idle = catalog_idle + data_idle;

        #[allow(clippy::cast_possible_truncation)]
        metrics.record_pool_state(total_active as u32, total_idle as u32);
    }
}
