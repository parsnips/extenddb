// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Real PostgreSQL tests. Set EXTENDDB_TEST_PG_CONNECTION_STRING to a server
//! URL without a database; the role must be able to create/drop databases.
//! Catalog and data use separate scratch databases, cleaned up even on panic.

use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::*;
use extenddb_storage::{DataEngine, StreamCapture, StreamEngine, TableEngine};
use futures::FutureExt;
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, PgPool};

use crate::gsi_queue::{GsiQueue, PendingApplyContext, enqueue_gsi_pending};
use crate::migrations::{run_catalog_migrations, run_data_code_migrations, run_data_migrations};
use crate::{PostgresConfig, PostgresEngine};

const ACCOUNT: &str = "123456789012";

struct Scratch {
    catalog: PgPool,
    data: PgPool,
    engine: PostgresEngine,
}

async fn with_databases<F, Fut>(test: F)
where
    F: FnOnce(Scratch) -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    let Ok(base) = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING") else {
        eprintln!("SKIP base_pk PostgreSQL test: EXTENDDB_TEST_PG_CONNECTION_STRING is unset");
        return;
    };
    let base = base.trim_end_matches('/');
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .unwrap();
    let prefix = format!("eddb_routing_{}", uuid::Uuid::new_v4().simple());
    let names = [format!("{prefix}_catalog"), format!("{prefix}_data")];
    let outcome = AssertUnwindSafe(async {
        for name in &names {
            sqlx::query(&format!("CREATE DATABASE \"{name}\""))
                .execute(&admin).await?;
        }
        let catalog_url = format!("{base}/{}", names[0]);
        let data_url = format!("{base}/{}", names[1]);
        let catalog = PgPoolOptions::new().max_connections(2).connect(&catalog_url).await?;
        let data = PgPoolOptions::new().max_connections(2).connect(&data_url).await?;
        run_catalog_migrations(&catalog).await.unwrap();
        run_data_migrations(&data).await.unwrap();
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, 'routing-test')")
            .bind(ACCOUNT).execute(&catalog).await?;
        sqlx::query("UPDATE settings SET value = '0' WHERE key IN ('control_plane_delay_seconds', 'index_propagation_delay_ms')")
            .execute(&catalog).await?;
        sqlx::query("INSERT INTO settings (key, value) VALUES ('data_database_connection_string', $1) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
            .bind(data_url).execute(&catalog).await?;
        let engine = PostgresEngine::new(&PostgresConfig {
            connection_string: catalog_url, pool_size: 10, max_item_size_bytes: 400_000, stream_sharding: None,
        }, "us-east-1").await?;
        test(Scratch { catalog, data, engine }).await
    }).catch_unwind().await;
    for name in &names {
        sqlx::query(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
            .execute(&admin)
            .await
            .unwrap();
    }
    admin.close().await;
    match outcome {
        Ok(result) => result.unwrap(),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

fn schema() -> Value {
    json!([{"AttributeName":"pk","KeyType":"HASH"}, {"AttributeName":"sk","KeyType":"RANGE"}])
}

async fn create_table(
    s: &Scratch,
    name: &str,
    key_schema: Value,
    pk_type: &str,
    gsi: bool,
) -> anyhow::Result<TableKeyInfo> {
    let mut input = json!({
        "TableName": name, "KeySchema": key_schema,
        "AttributeDefinitions": [
            {"AttributeName":"pk","AttributeType":pk_type},
            {"AttributeName":"sk","AttributeType":"S"}
        ],
        "BillingMode":"PAY_PER_REQUEST",
        "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"KEYS_ONLY"}
    });
    if gsi {
        input["AttributeDefinitions"]
            .as_array_mut()
            .unwrap()
            .push(json!({"AttributeName":"gpk","AttributeType":"S"}));
        input["GlobalSecondaryIndexes"] = json!([{
            "IndexName":"by_gpk", "KeySchema":[{"AttributeName":"gpk","KeyType":"HASH"}],
            "Projection":{"ProjectionType":"ALL"}
        }]);
    }
    s.engine
        .create_table(ACCOUNT, serde_json::from_value(input)?)
        .await?;
    Ok(s.engine.table_key_info(ACCOUNT, name).await?)
}

fn context(key_schema: Value) -> Value {
    json!({
        "base_key_schema":key_schema, "attribute_definitions":[],
        "index":{"index_id":"unused","key_schema":[{"AttributeName":"gpk","KeyType":"HASH"}],
                 "projection":{"ProjectionType":"ALL"}}
    })
}

fn vector_context(key_schema: Value) -> Value {
    json!({
        "base_key_schema": key_schema, "attribute_definitions": [], "table_id": "unused",
        "vector": {"index_id": "unused", "dimensions": 3, "vector_attribute_name": "emb",
                   "projection": {"ProjectionType": "ALL"}, "hash_attribute_name": null,
                   "search_schema_attribute_names": []}
    })
}

async fn snapshot(pool: &PgPool, table: &str, order: &str) -> anyhow::Result<Value> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT jsonb_agg(to_jsonb(r) - 'base_pk' ORDER BY {order}) FROM {table} r"
    ))
    .fetch_one(pool)
    .await?)
}

#[tokio::test]
async fn base_pk_migration_backfills_encoded_keys_atomically_and_is_repeatable() {
    with_databases(|s| async move {
        let composite_schema = json!([
            {"AttributeName":"pk","KeyType":"HASH"},
            {"AttributeName":"sk","KeyType":"HASH"}
        ]);
        let cases = [
            (schema(), "S", json!({"pk":{"S":"é:tenant,"},"sk":{"S":"sort"}}), "é:tenant,"),
            (schema(), "N", json!({"pk":{"N":"123.50"},"sk":{"S":"sort"}}), "123.5"),
            (schema(), "B", json!({"pk":{"B":"AP8="},"sk":{"S":"sort"}}), "AP8="),
            (composite_schema, "S", json!({"pk":{"S":"é:"},"sk":{"S":"x,y"}}), "3:é:,3:x,y,"),
        ];
        let mut expected = Vec::new();
        for (i, (ks, kind, keys, encoded)) in cases.into_iter().enumerate() {
            let info = create_table(&s, &format!("legacy_{i}"), ks.clone(), kind, false).await?;
            let shard: String = sqlx::query_scalar("SELECT shard_id FROM stream_shards WHERE table_id = $1 ORDER BY shard_id LIMIT 1")
                .bind(&info.table_id).fetch_one(&s.data).await?;
            // More than one migration batch, and no corresponding live base rows.
            let count = if i == 0 { 501 } else { 1 };
            sqlx::query("INSERT INTO stream_records (shard_id, sequence_number, table_id, event_name, record_data) SELECT $1, n::text, $2, 'Remove', $3 FROM generate_series(1, $4) n")
                .bind(shard).bind(&info.table_id).bind(json!({"dynamodb":{"Keys":keys}})).bind(count)
                .execute(&s.data).await?;
            sqlx::query("INSERT INTO gsi_pending (table_id, worker_partition, old_item, new_item, index_context) SELECT $1, 3, CASE WHEN n % 2 = 1 THEN $2 END, CASE WHEN n % 2 = 0 THEN $2 END, $3 FROM generate_series(1, $4) n")
                .bind(&info.table_id).bind(keys).bind(if i == 2 { vector_context(ks) } else { context(ks) }).bind(count).execute(&s.data).await?;
            expected.push((info.table_id, encoded, i64::from(count)));
        }
        let streams = snapshot(&s.data, "stream_records", "shard_id, sequence_number").await?;
        let queue = snapshot(&s.data, "gsi_pending", "id").await?;

        // Missing metadata must fail closed, without leaving nullable columns or
        // a success ledger entry behind. Repair and retry the same migration.
        sqlx::query("UPDATE stream_records SET table_id = 'missing' WHERE table_id = $1")
            .bind(&expected[0].0).execute(&s.data).await?;
        let err = run_data_code_migrations(&s.catalog, &s.data).await.unwrap_err();
        assert!(format!("{err:?}").contains("no catalog key schema"), "{err:?}");
        let columns: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM information_schema.columns WHERE table_name IN ('stream_records', 'gsi_pending') AND column_name = 'base_pk'")
            .fetch_one(&s.data).await?;
        assert_eq!(columns, 0);
        let applied: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM schema_history WHERE filename = $1)")
            .bind(super::NAME).fetch_one(&s.data).await?;
        assert!(!applied);
        sqlx::query("UPDATE stream_records SET table_id = $1 WHERE table_id = 'missing'")
            .bind(&expected[0].0).execute(&s.data).await?;
        run_data_code_migrations(&s.catalog, &s.data).await.unwrap();
        run_data_code_migrations(&s.catalog, &s.data).await.unwrap();
        for (id, encoded, count) in expected {
            for table in ["stream_records", "gsi_pending"] {
                let actual: Vec<(String, i64)> = sqlx::query_as(&format!("SELECT base_pk, COUNT(*) FROM {table} WHERE table_id = $1 GROUP BY base_pk"))
                    .bind(&id).fetch_all(&s.data).await?;
                assert_eq!(actual, vec![(encoded.to_owned(), count)]);
            }
        }
        assert_eq!(streams, snapshot(&s.data, "stream_records", "shard_id, sequence_number").await?);
        assert_eq!(queue, snapshot(&s.data, "gsi_pending", "id").await?);
        for table in ["stream_records", "gsi_pending"] {
            assert!(sqlx::query(&format!("UPDATE {table} SET base_pk = NULL")).execute(&s.data).await.is_err());
        }
        Ok(())
    }).await;
}

#[tokio::test]
async fn stream_buckets_match_base_keys_and_survive_config_changes() {
    with_databases(|mut s| async move {
        run_data_code_migrations(&s.catalog, &s.data).await.unwrap();
        let routing = crate::StreamSharding { hash: crate::StreamHash::Xxh3_64, mapping: crate::BucketMapping::HashRange, bucket_count: 16, seed: 0 };
        s.engine.stream_sharding = Some(routing.clone());
        let capture = StreamCapture { view_type: StreamViewType::KeysOnly, user_identity: None, region: "us-east-1".into() };
        let maps = ExpressionMaps::new(HashMap::new(), HashMap::new());
        for (i, (ks, kind, pk, sk)) in [
            (schema(), "S", json!({"S":"é:tenant,"}), json!({"S":"sort"})),
            (schema(), "N", json!({"N":"123.50"}), json!({"S":"sort"})),
            (schema(), "B", json!({"B":"AP8="}), json!({"S":"sort"})),
            (json!([{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"HASH"}]),
             "S", json!({"S":"é:"}), json!({"S":"x,y"})),
        ].into_iter().enumerate() {
            s.engine.stream_sharding = Some(routing.clone());
            let name = format!("bucket_{i}");
            let info = create_table(&s, &name, ks, kind, false).await?;
            let shard_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM stream_shards WHERE table_id = $1")
                .bind(&info.table_id).fetch_one(&s.data).await?;
            assert_eq!(shard_count, 16);
            // A restart/config change affects new streams only.
            s.engine.stream_sharding = Some(crate::StreamSharding { hash: crate::StreamHash::Crc32, mapping: crate::BucketMapping::Modulo, bucket_count: 3, seed: 0 });
            let item: Item = serde_json::from_value(json!({"pk":pk,"sk":sk}))?;
            s.engine.put_item(&info, item.clone(), false, None, &maps, Some(&capture)).await?;
            let data_table = crate::data::data_table_name(&info.table_id);
            let base: String = sqlx::query_scalar(&format!("SELECT pk FROM {data_table}"))
                .fetch_one(&s.data).await?;
            let ids = routing.shard_ids(&info.table_id)?;
            let expected = ids[routing.bucket(&base)? as usize].clone();
            assert_eq!(s.engine.assign_shard(ACCOUNT, &name, &base).await?, expected);
            let (shard, stream_base, payload): (String, String, Value) = sqlx::query_as(
                "SELECT shard_id, base_pk, record_data FROM stream_records WHERE table_id = $1")
                .bind(&info.table_id).fetch_one(&s.data).await?;
            assert_eq!(stream_base, base);
            assert_eq!(shard, expected);
            let mut record: StreamRecord = serde_json::from_value(payload)?;
            record.dynamodb.sequence_number = s.engine.next_sequence_number(&shard).await?;
            let wrong = ids[((routing.bucket(&base)? + 1) % 16) as usize].clone();
            assert!(s.engine.write_stream_record(ACCOUNT, &record, &wrong, &name).await.is_err());
            s.engine.write_stream_record(ACCOUNT, &record, &shard, &name).await?;
            s.engine.delete_item(&info, &item, false, None, &maps, Some(&capture)).await?;
            let (records, _) = s.engine.get_stream_records(ACCOUNT, &shard, None, 100).await?;
            assert_eq!(records.len(), 3);
            assert!(records.iter().all(|r| r.dynamodb.keys == record.dynamodb.keys));

            // Transactional puts/updates/deletes must use the same encoded key,
            // especially on a composite-HASH table.
            s.engine.transact_write_items(&[extenddb_storage::TransactWriteOp::Put {
                key_info: &info, item: &item, condition: None, maps: &maps,
                return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                stream: Some(capture.clone()),
            }], None).await?;
            let stored: String = sqlx::query_scalar(&format!("SELECT pk FROM {data_table}"))
                .fetch_one(&s.data).await?;
            assert_eq!(stored, base);
            let tokens = extenddb_core::expression::tokenize("SET value = :v")?;
            let actions = extenddb_core::expression::parse_update(&tokens)?;
            let update_maps = ExpressionMaps::new(HashMap::new(), HashMap::from([("v".to_owned(), AttributeValue::S("changed".to_owned()))]));
            s.engine.update_item(&info, &item, &actions, false, false, None, &update_maps, Some(&capture)).await?;
            s.engine.transact_write_items(&[extenddb_storage::TransactWriteOp::Delete {
                key_info: &info, key: &item, condition: None, maps: &maps,
                return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                stream: Some(capture.clone()),
            }], None).await?;
            let (records, _) = s.engine.get_stream_records(ACCOUNT, &shard, None, 100).await?;
            assert_eq!(records.len(), 6);

            // Incomplete metadata must roll the base mutation back.
            sqlx::query("DELETE FROM stream_shards WHERE shard_id = $1")
                .bind(&wrong).execute(&s.data).await?;
            assert!(s.engine.put_item(&info, item, false, None, &maps, Some(&capture)).await.is_err());
            let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {data_table}"))
                .fetch_one(&s.data).await?;
            assert_eq!(count, 0);
        }
        Ok(())
    }).await;
}

#[tokio::test]
async fn base_pk_writes_match_base_and_gsi_and_stream_failures_roll_back() {
    with_databases(|s| async move {
        run_data_code_migrations(&s.catalog, &s.data).await.unwrap();
        let capture = StreamCapture { view_type: StreamViewType::KeysOnly, user_identity: None, region: "us-east-1".into() };
        let maps = ExpressionMaps::new(HashMap::new(), HashMap::new());
        for (i, (kind, pk, encoded)) in [
            ("S", json!({"S":"é:tenant,"}), "é:tenant,"),
            ("N", json!({"N":"123.50"}), "123.5"),
            ("B", json!({"B":"AP8="}), "AP8="),
        ].into_iter().enumerate() {
            sqlx::query("UPDATE settings SET value = '0' WHERE key = 'index_propagation_delay_ms'")
                .execute(&s.catalog).await?;
            let name = format!("live_{i}");
            let info = create_table(&s, &name, schema(), kind, true).await?;
            let item: Item = serde_json::from_value(json!({"pk":pk,"sk":{"S":"sort"},"gpk":{"S":"index-key"}}))?;
            s.engine.put_item(&info, item.clone(), false, None, &maps, Some(&capture)).await?;
            let data_table = crate::data::data_table_name(&info.table_id);
            let base: String = sqlx::query_scalar(&format!("SELECT pk FROM {data_table}"))
                .fetch_one(&s.data).await?;
            assert_eq!(base, encoded);
            let index_id: String = sqlx::query_scalar("SELECT index_id FROM indexes WHERE table_id = $1")
                .bind(&info.table_id).fetch_one(&s.catalog).await?;
            let index_table = crate::data::index_table_name(&index_id);
            let index_pk: String = sqlx::query_scalar(&format!("SELECT base_pk FROM {index_table}"))
                .fetch_one(&s.data).await?;
            assert_eq!(index_pk, base);

            // The standalone StreamEngine path must also populate the routing
            // column; KEYS_ONLY carries no old/new image to fall back to.
            let (shard, payload): (String, Value) = sqlx::query_as("SELECT shard_id, record_data FROM stream_records WHERE table_id = $1")
                .bind(&info.table_id).fetch_one(&s.data).await?;
            let mut record: StreamRecord = serde_json::from_value(payload)?;
            record.dynamodb.sequence_number = "manual".to_owned();
            s.engine.write_stream_record(ACCOUNT, &record, &shard, &name).await?;

            sqlx::query("UPDATE settings SET value = '10000' WHERE key = 'index_propagation_delay_ms'")
                .execute(&s.catalog).await?;
            let tokens = extenddb_core::expression::tokenize("SET gpk = :v")?;
            let actions = extenddb_core::expression::parse_update(&tokens)?;
            let update_maps = ExpressionMaps::new(HashMap::new(), HashMap::from([("v".to_owned(), AttributeValue::S("changed".to_owned()))]));
            s.engine.update_item(&info, &item, &actions, false, false, None, &update_maps, Some(&capture)).await?;
            s.engine.delete_item(&info, &item, false, None, &maps, Some(&capture)).await?;
            let pending: Vec<String> = sqlx::query_scalar("SELECT base_pk FROM gsi_pending WHERE table_id = $1 ORDER BY id")
                .bind(&info.table_id).fetch_all(&s.data).await?;
            assert_eq!(pending, vec![base.clone(), base.clone()]);
            let streams: Vec<String> = sqlx::query_scalar("SELECT base_pk FROM stream_records WHERE table_id = $1")
                .bind(&info.table_id).fetch_all(&s.data).await?;
            assert_eq!(streams, vec![base.clone(); 4]);

            // A failed stream insert must roll back the base write and its
            // queued index work, not merely report an error after committing.
            s.data.execute("CREATE FUNCTION reject_stream_test() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected stream failure'; END $$; CREATE TRIGGER reject_stream_test BEFORE INSERT ON stream_records FOR EACH ROW EXECUTE FUNCTION reject_stream_test();").await?;
            assert!(s.engine.put_item(&info, item, false, None, &maps, Some(&capture)).await.is_err());
            let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {data_table}"))
                .fetch_one(&s.data).await?;
            assert_eq!(count, 0);
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM gsi_pending WHERE table_id = $1")
                .bind(&info.table_id).fetch_one(&s.data).await?;
            assert_eq!(count, 2);
            s.data.execute("DROP TRIGGER reject_stream_test ON stream_records; DROP FUNCTION reject_stream_test();").await?;
        }
        // Exercise the changed claim tuple and routed DELETE with real index
        // updates. Each queued update/delete pair must converge to no GSI row.
        sqlx::query("UPDATE gsi_pending SET ready_at = NOW()").execute(&s.data).await?;
        let _queue = GsiQueue::spawn(s.data.clone());
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM gsi_pending")
                    .fetch_one(&s.data).await?;
                if count == 0 { break; }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            Ok::<_, sqlx::Error>(())
        }).await??;
        let index_ids: Vec<String> = sqlx::query_scalar("SELECT index_id FROM indexes")
            .fetch_all(&s.catalog).await?;
        for id in index_ids {
            let table = crate::data::index_table_name(&id);
            let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(&s.data).await?;
            assert_eq!(count, 0);
        }
        Ok(())
    }).await;
}

#[tokio::test]
async fn base_pk_queue_clamps_only_the_same_table_and_base_key() {
    with_databases(|s| async move {
        run_data_code_migrations(&s.catalog, &s.data).await.unwrap();
        let ctx: PendingApplyContext = serde_json::from_value(context(schema()))?;
        let item: Item = serde_json::from_value(json!({"pk":{"S":"tenant"},"sk":{"S":"sort"}}))?;
        let mut tx = s.data.begin().await?;
        enqueue_gsi_pending(&mut tx, "table-a", None, Some(&item), 1, &ctx).await?;
        tx.commit().await?;
        sqlx::query("UPDATE gsi_pending SET ready_at = NOW() + INTERVAL '1 day'").execute(&s.data).await?;
        let mut tx = s.data.begin().await?;
        // Delete uses old_item and must inherit the earlier row's ready_at.
        enqueue_gsi_pending(&mut tx, "table-a", Some(&item), None, 1, &ctx).await?;
        // Same encoded key in another table must not inherit that delay.
        enqueue_gsi_pending(&mut tx, "table-b", None, Some(&item), 1, &ctx).await?;
        assert!(enqueue_gsi_pending(&mut tx, "table-a", None, None, 1, &ctx).await.is_err());
        tx.commit().await?;
        let rows: Vec<(String, String, bool)> = sqlx::query_as("SELECT table_id, base_pk, ready_at > NOW() + INTERVAL '1 hour' FROM gsi_pending ORDER BY id")
            .fetch_all(&s.data).await?;
        assert_eq!(rows, vec![
            ("table-a".to_owned(), "tenant".to_owned(), true),
            ("table-a".to_owned(), "tenant".to_owned(), true),
            ("table-b".to_owned(), "tenant".to_owned(), false),
        ]);
        let vector_ctx: PendingApplyContext = serde_json::from_value(vector_context(schema()))?;
        let mut tx = s.data.begin().await?;
        enqueue_gsi_pending(&mut tx, "table-vector", Some(&item), None, 1, &vector_ctx).await?;
        let base_pk: String = sqlx::query_scalar("SELECT base_pk FROM gsi_pending WHERE table_id = 'table-vector'")
            .fetch_one(&mut *tx).await?;
        assert_eq!(base_pk, "tenant");
        tx.rollback().await?;
        Ok(())
    }).await;
}
