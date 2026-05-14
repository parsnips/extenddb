// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `MetadataEngine` trait implementation for `PostgresEngine`.

use extenddb_core::types::{Item, Tag, TimeToLiveDescription, TimeToLiveStatus};
use extenddb_storage::MetadataEngine;
use extenddb_storage::error::StorageError;

use crate::PostgresEngine;
use crate::data;

impl MetadataEngine for PostgresEngine {
    async fn describe_ttl(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<TimeToLiveDescription, StorageError> {
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT ttl_attribute FROM tables WHERE account_id = $1 AND table_name = $2",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (ttl_attr,) = row.ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;

        Ok(match ttl_attr {
            Some(attr) => TimeToLiveDescription {
                time_to_live_status: TimeToLiveStatus::Enabled,
                attribute_name: Some(attr),
            },
            None => TimeToLiveDescription {
                time_to_live_status: TimeToLiveStatus::Disabled,
                attribute_name: None,
            },
        })
    }

    async fn update_ttl(
        &self,
        account_id: &str,
        table_name: &str,
        attribute_name: &str,
        enabled: bool,
    ) -> Result<(), StorageError> {
        let ttl_val: Option<&str> = if enabled { Some(attribute_name) } else { None };
        // When disabling, also clear ttl_index_ready (index will be dropped separately).
        let index_ready = false;

        let result = sqlx::query(
            "UPDATE tables SET ttl_attribute = $1, ttl_index_ready = $4 \
             WHERE account_id = $2 AND table_name = $3 AND table_status = 'ACTIVE'",
        )
        .bind(ttl_val)
        .bind(account_id)
        .bind(table_name)
        .bind(index_ready)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        if result.rows_affected() == 0 {
            // Check if table exists at all.
            let exists: Option<(String,)> = sqlx::query_as(
                "SELECT table_status FROM tables WHERE account_id = $1 AND table_name = $2",
            )
            .bind(account_id)
            .bind(table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            return match exists {
                None => Err(StorageError::TableNotFound(table_name.to_owned())),
                Some(_) => Err(StorageError::TableNotActive(table_name.to_owned())),
            };
        }

        Ok(())
    }

    async fn tag_resource(&self, arn: &str, tags: &[Tag]) -> Result<(), StorageError> {
        for tag in tags {
            sqlx::query(
                "INSERT INTO tags (resource_arn, tag_key, tag_value) VALUES ($1, $2, $3) \
                 ON CONFLICT (resource_arn, tag_key) DO UPDATE SET tag_value = EXCLUDED.tag_value",
            )
            .bind(arn)
            .bind(&tag.key)
            .bind(&tag.value)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        }
        Ok(())
    }

    async fn untag_resource(&self, arn: &str, tag_keys: &[String]) -> Result<(), StorageError> {
        for key in tag_keys {
            sqlx::query("DELETE FROM tags WHERE resource_arn = $1 AND tag_key = $2")
                .bind(arn)
                .bind(key)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }
        Ok(())
    }

    async fn list_tags(&self, arn: &str) -> Result<Vec<Tag>, StorageError> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT tag_key, tag_value FROM tags WHERE resource_arn = $1 ORDER BY tag_key",
        )
        .bind(arn)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|(key, value)| Tag { key, value })
            .collect())
    }

    async fn tables_with_ttl(
        &self,
        account_id: &str,
    ) -> Result<Vec<(String, String)>, StorageError> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT table_name, ttl_attribute FROM tables \
             WHERE account_id = $1 AND ttl_attribute IS NOT NULL AND table_status = 'ACTIVE'",
        )
        .bind(account_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(rows)
    }

    async fn refresh_table_size(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<(), StorageError> {
        Self::validate_account_id(account_id)?;
        let (table_id,): (String,) = sqlx::query_as(
            "SELECT table_id FROM tables WHERE account_id = $1 AND table_name = $2",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        let data_table = data::data_table_name(&table_id);

        // P54 Bug 1: Data tables live in the data database. Read size/count
        // from the data pool, then update the catalog on the catalog pool.
        // Note: count and size are read in separate queries, so they may be
        // slightly inconsistent if the table changes between reads. This is
        // acceptable — DynamoDB's own TableSizeBytes/ItemCount are approximate.
        let count_sql = format!("SELECT COUNT(*) FROM {data_table}");
        let (item_count,): (i64,) = sqlx::query_as(&count_sql)
            .fetch_one(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let size_sql = format!("SELECT COALESCE(pg_total_relation_size('{data_table}'), 0)");
        let (table_size,): (i64,) = sqlx::query_as(&size_sql)
            .fetch_one(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        sqlx::query(
            "UPDATE tables SET item_count = $1, table_size_bytes = $2 \
             WHERE account_id = $3 AND table_name = $4 AND table_status = 'ACTIVE'",
        )
        .bind(item_count)
        .bind(table_size)
        .bind(account_id)
        .bind(table_name)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(())
    }

    async fn list_active_table_names(&self, account_id: &str) -> Result<Vec<String>, StorageError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT table_name FROM tables WHERE account_id = $1 AND table_status = 'ACTIVE' ORDER BY table_name",
        )
        .bind(account_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(rows.into_iter().map(|(n,)| n).collect())
    }

    async fn all_tables_with_ttl(&self) -> Result<Vec<(String, String, String)>, StorageError> {
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT account_id, table_name, ttl_attribute FROM tables \
             WHERE ttl_attribute IS NOT NULL AND table_status = 'ACTIVE'",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(rows)
    }

    async fn all_tables_with_ttl_index_ready(
        &self,
    ) -> Result<Vec<(String, String, String)>, StorageError> {
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT account_id, table_name, ttl_attribute FROM tables \
             WHERE ttl_attribute IS NOT NULL AND ttl_index_ready = TRUE AND table_status = 'ACTIVE'",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(rows)
    }

    async fn create_ttl_index(
        &self,
        account_id: &str,
        table_name: &str,
        ttl_attribute: &str,
    ) -> Result<(), StorageError> {
        Self::validate_account_id(account_id)?;
        let (table_id,): (String,) = sqlx::query_as(
            "SELECT table_id FROM tables WHERE account_id = $1 AND table_name = $2",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        let data_table = data::data_table_name(&table_id);
        // Strip quotes for index name (data_table_name returns quoted identifier).
        let bare_table = data_table.trim_matches('"');
        let index_name = format!("idx_ttl_{bare_table}");

        // CREATE INDEX CONCURRENTLY cannot run inside a transaction.
        // Use a raw connection from the data pool.
        let sql = format!(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS \"{index_name}\" \
             ON {data_table} (((item_data->'{ttl_attribute}'->>'N')::BIGINT)) \
             WHERE (item_data->'{ttl_attribute}'->>'N') IS NOT NULL"
        );
        sqlx::query(&sql)
            .execute(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(format!("TTL index creation failed: {e}")))?;

        // Mark index as ready in catalog.
        sqlx::query(
            "UPDATE tables SET ttl_index_ready = TRUE \
             WHERE account_id = $1 AND table_name = $2",
        )
        .bind(account_id)
        .bind(table_name)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(())
    }

    async fn drop_ttl_index(&self, account_id: &str, table_name: &str) -> Result<(), StorageError> {
        Self::validate_account_id(account_id)?;
        let (table_id,): (String,) = sqlx::query_as(
            "SELECT table_id FROM tables WHERE account_id = $1 AND table_name = $2",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        let data_table = data::data_table_name(&table_id);
        let bare_table = data_table.trim_matches('"');
        let index_name = format!("idx_ttl_{bare_table}");

        // Mark index as not ready first (sweeper stops using it).
        sqlx::query(
            "UPDATE tables SET ttl_index_ready = FALSE \
             WHERE account_id = $1 AND table_name = $2",
        )
        .bind(account_id)
        .bind(table_name)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let sql = format!("DROP INDEX IF EXISTS \"{index_name}\"");
        sqlx::query(&sql)
            .execute(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(format!("TTL index drop failed: {e}")))?;

        Ok(())
    }

    async fn find_expired_items_indexed(
        &self,
        account_id: &str,
        table_name: &str,
        ttl_attribute: &str,
        limit: usize,
    ) -> Result<Vec<Item>, StorageError> {
        Self::validate_account_id(account_id)?;
        let (table_id,): (String,) = sqlx::query_as(
            "SELECT table_id FROM tables WHERE account_id = $1 AND table_name = $2",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        let data_table = data::data_table_name(&table_id);

        let now_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
        let now_i64 = i64::try_from(now_epoch).unwrap_or(i64::MAX);

        // Index-ordered scan: stops after LIMIT rows (efficient with B-tree).
        let sql = format!(
            "SELECT item_data FROM {data_table} \
             WHERE (item_data->$1->>'N')::BIGINT BETWEEN 1 AND $2 \
             ORDER BY (item_data->$1->>'N')::BIGINT \
             LIMIT $3"
        );
        let rows: Vec<(serde_json::Value,)> = sqlx::query_as(&sql)
            .bind(ttl_attribute)
            .bind(now_i64)
            .bind(limit_i64)
            .fetch_all(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        rows.into_iter().map(|(v,)| data::json_to_item(v)).collect()
    }

    async fn all_active_tables(&self) -> Result<Vec<(String, String)>, StorageError> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT account_id, table_name FROM tables \
             WHERE table_status = 'ACTIVE' ORDER BY account_id, table_name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(rows)
    }
}
