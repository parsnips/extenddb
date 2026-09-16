// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Add routing keys without changing stream payloads or queue ordering.

use std::collections::HashMap;

use extenddb_core::types::{Item, KeySchemaElement};
use extenddb_storage::management_store::{OpError, OpResult};
use extenddb_storage::util::composite_pk_to_text;
use sqlx::{Executor, PgPool, Postgres, Transaction};

use crate::gsi_queue::PendingApplyContext;

pub(super) const NAME: &str = "005_base_pk_routing";
const BATCH_SIZE: i64 = 500;

/// The DDL, backfill, constraints, and ledger entry commit together. Any failure
/// leaves the old schema intact and the migration retryable. Requires stopped
/// writers: the ALTERs hold exclusive locks until the entire backfill commits.
pub(super) async fn migrate(catalog: &PgPool, data: &PgPool) -> OpResult<()> {
    migrate_inner(catalog, data)
        .await
        .map_err(|e| OpError::Internal(format!("Data migration {NAME} failed: {e}")))
}

async fn migrate_inner(catalog: &PgPool, data: &PgPool) -> anyhow::Result<()> {
    let mut tx = data.begin().await?;
    (&mut *tx)
        .execute(include_str!("base_pk/prepare.sql"))
        .await?;
    backfill_streams(catalog, &mut tx).await?;
    backfill_pending(&mut tx).await?;
    (&mut *tx)
        .execute(include_str!("base_pk/finish.sql"))
        .await?;
    sqlx::query("INSERT INTO schema_history (filename) VALUES ($1)")
        .bind(NAME)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

async fn backfill_streams(
    catalog: &PgPool,
    tx: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<()> {
    let mut schemas: HashMap<String, Vec<KeySchemaElement>> = HashMap::new();
    let mut after: Option<(String, String)> = None;
    loop {
        // Page by the existing primary key, not OFFSET or repeated NULL scans.
        // Only Keys are needed, even for KEYS_ONLY and deleted items.
        let rows: Vec<(String, String, String, serde_json::Value)> = sqlx::query_as(
            "SELECT shard_id, sequence_number, table_id, record_data #> '{dynamodb,Keys}' \
             FROM stream_records \
             WHERE $1::text IS NULL OR (shard_id, sequence_number) > ($1, $2) \
             ORDER BY shard_id, sequence_number LIMIT $3",
        )
        .bind(after.as_ref().map(|a| &a.0))
        .bind(after.as_ref().map(|a| &a.1))
        .bind(BATCH_SIZE)
        .fetch_all(&mut **tx)
        .await?;
        if rows.is_empty() {
            break;
        }
        let mut shards = Vec::with_capacity(rows.len());
        let mut sequences = Vec::with_capacity(rows.len());
        let mut base_pks = Vec::with_capacity(rows.len());
        for (shard, sequence, table, keys) in rows {
            if !schemas.contains_key(&table) {
                let schema: Option<serde_json::Value> =
                    sqlx::query_scalar("SELECT key_schema FROM tables WHERE table_id = $1")
                        .bind(&table)
                        .fetch_optional(catalog)
                        .await?;
                let schema = schema.ok_or_else(|| {
                    anyhow::anyhow!(
                        "Stream table {table} has no catalog key schema; restore its schema \
                         or let its retained records expire before migrating"
                    )
                })?;
                schemas.insert(table.clone(), serde_json::from_value(schema)?);
            }
            let keys: Item = serde_json::from_value(keys)?;
            base_pks.push(composite_pk_to_text(&keys, &schemas[&table])?);
            after = Some((shard.clone(), sequence.clone()));
            shards.push(shard);
            sequences.push(sequence);
        }
        sqlx::query(
            "UPDATE stream_records r SET base_pk = b.base_pk \
             FROM UNNEST($1::text[], $2::text[], $3::text[]) AS b(shard_id, sequence_number, base_pk) \
             WHERE r.shard_id = b.shard_id AND r.sequence_number = b.sequence_number",
        )
        .bind(shards)
        .bind(sequences)
        .bind(base_pks)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

async fn backfill_pending(tx: &mut Transaction<'_, Postgres>) -> anyhow::Result<()> {
    let mut after: Option<i64> = None;
    loop {
        let rows: Vec<(i64, Option<serde_json::Value>, serde_json::Value)> = sqlx::query_as(
            "SELECT id, COALESCE(new_item, old_item), index_context FROM gsi_pending \
             WHERE $1::bigint IS NULL OR id > $1 ORDER BY id LIMIT $2",
        )
        .bind(after)
        .bind(BATCH_SIZE)
        .fetch_all(&mut **tx)
        .await?;
        if rows.is_empty() {
            break;
        }
        let mut ids = Vec::with_capacity(rows.len());
        let mut base_pks = Vec::with_capacity(rows.len());
        for (id, item, context) in rows {
            let item: Item = serde_json::from_value(
                item.ok_or_else(|| anyhow::anyhow!("Pending index row {id} has no base item"))?,
            )?;
            let context: PendingApplyContext = serde_json::from_value(context)?;
            base_pks.push(composite_pk_to_text(&item, context.base_key_schema())?);
            ids.push(id);
            after = Some(id);
        }
        sqlx::query(
            "UPDATE gsi_pending p SET base_pk = b.base_pk \
             FROM UNNEST($1::bigint[], $2::text[]) AS b(id, base_pk) WHERE p.id = b.id",
        )
        .bind(ids)
        .bind(base_pks)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
