// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use extenddb_storage::management_store::{OpError, OpResult};
use sqlx::PgPool;

pub(super) const NAME: &str = "006_stream_shard_routing";

pub(super) async fn migrate(data: &PgPool) -> OpResult<()> {
    async {
        let mut tx = data.begin().await?;
        // NULL preserves pre-configuration streams without moving any records.
        sqlx::query("ALTER TABLE stream_shards ADD COLUMN routing JSONB")
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO schema_history (filename) VALUES ($1)")
            .bind(NAME)
            .execute(&mut *tx)
            .await?;
        tx.commit().await
    }
    .await
    .map_err(|e: sqlx::Error| OpError::Internal(format!("Data migration {NAME} failed: {e}")))
}
