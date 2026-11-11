// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The write path with its index cache on (`write_path_index_cache_ttl_ms`).
//!
//! A cache hit stands for a catalog read at the moment of the hit, so these
//! tests rerun the online-build races of `gsi_online_build.rs` with a TTL far
//! longer than the build: an index the cache did not know about must still end
//! equal to the base table, and a dropped index must not be maintained.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component (for example `postgresql://postgres@127.0.0.1:5432`), pointing at a
//! server whose role may create and drop databases. Without it every test here
//! reports a skip and passes, the same convention the wire suites use.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, Item, KeySchemaElement,
    KeyType, ScalarAttributeType, TableKeyInfo, UpdateTableInput,
};
use extenddb_storage::{DataEngine, TableEngine};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
const TABLE: &str = "t_index_cache";

struct Scratch {
    engine: PostgresEngine,
    db: PgPool,
    admin: PgPool,
    db_name: String,
}

impl Scratch {
    async fn cleanup(self) {
        let Scratch {
            engine,
            db,
            admin,
            db_name,
        } = self;
        drop(engine);
        db.close().await;
        sqlx::query(&format!(
            "DROP DATABASE IF EXISTS \"{db_name}\" WITH (FORCE)"
        ))
        .execute(&admin)
        .await
        .expect("drop the scratch database");
        admin.close().await;
    }
}

/// The server's base URL, or `None` to skip. With `EXTENDDB_TEST_PG_REQUIRED`
/// set to any value but `0`, a job that must reach a server fails here
/// instead.
fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").unwrap_or_default();
    if conn.trim().is_empty() {
        let required =
            std::env::var("EXTENDDB_TEST_PG_REQUIRED").is_ok_and(|v| !v.is_empty() && v != "0");
        assert!(
            !required,
            "EXTENDDB_TEST_PG_CONNECTION_STRING is not set, and EXTENDDB_TEST_PG_REQUIRED says \
             this job must reach a PostgreSQL server"
        );
        return None;
    }
    Some(conn.trim_end_matches('/').to_owned())
}

fn shipped_migrations() -> Vec<String> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut out = Vec::new();
    for dir in ["migrations", "data_migrations"] {
        let mut files: Vec<_> = std::fs::read_dir(root.join(dir))
            .expect("read the migrations directory")
            .map(|e| e.expect("directory entry").path())
            .filter(|p| p.extension().is_some_and(|e| e == "sql"))
            .collect();
        files.sort();
        for f in files {
            out.push(std::fs::read_to_string(&f).expect("read a migration file"));
        }
    }
    out
}

fn skip(test: &str) {
    eprintln!(
        "SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set, so there is no PostgreSQL \
         to build a scratch catalog in."
    );
}

async fn scratch() -> Scratch {
    // Process-wide, so every test in this binary runs with the cache on.
    extenddb_storage_postgres::set_write_path_index_cache_ttl(Some(CACHE_TTL));
    let base = base_conn().expect("caller checks base_conn() first");
    let db_name = format!("eddb_idxc_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to the postgres maintenance database");
    sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
        .execute(&admin)
        .await
        .expect("create the scratch database");
    let url = format!("{base}/{db_name}");
    let db = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect to the scratch database");
    // One scratch database stands in for both the catalog and the data database.
    for sql in shipped_migrations() {
        sqlx::raw_sql(&sql)
            .execute(&db)
            .await
            .expect("apply a shipped migration");
    }
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0') \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .execute(&db)
    .await
    .expect("pin the control-plane delay to zero");
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
        .bind(ACCOUNT)
        .bind(format!("acct-{db_name}"))
        .execute(&db)
        .await
        .expect("seed the account row");
    let engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: url,
            pool_size: 10,
            max_item_size_bytes: 400_000,
        },
        REGION,
    )
    .await
    .expect("open a PostgresEngine on the scratch database")
    .start_gsi_workers();
    Scratch {
        engine,
        db,
        admin,
        db_name,
    }
}

async fn create_table(s: &Scratch) -> TableKeyInfo {
    s.engine
        .create_table(
            ACCOUNT,
            CreateTableInput {
                table_name: TABLE.to_owned(),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "pk".to_owned(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "sk".to_owned(),
                        key_type: KeyType::Range,
                    },
                ],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".to_owned(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "sk".to_owned(),
                        attribute_type: ScalarAttributeType::N,
                    },
                ],
                billing_mode: Some(BillingMode::PayPerRequest),
                ..Default::default()
            },
        )
        .await
        .expect("create the table");
    s.engine
        .table_key_info(ACCOUNT, TABLE)
        .await
        .expect("read the key info")
}

/// Add GSI `gidx` keyed on `g` (S) and, when `range`, `r` (N).
async fn add_gsi(s: &Scratch, range: bool) {
    let mut defs = vec![serde_json::json!({"AttributeName": "g", "AttributeType": "S"})];
    let mut schema = vec![serde_json::json!({"AttributeName": "g", "KeyType": "HASH"})];
    if range {
        defs.push(serde_json::json!({"AttributeName": "r", "AttributeType": "N"}));
        schema.push(serde_json::json!({"AttributeName": "r", "KeyType": "RANGE"}));
    }
    let input: UpdateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": TABLE,
        "AttributeDefinitions": defs,
        "GlobalSecondaryIndexUpdates": [{"Create": {
            "IndexName": "gidx",
            "KeySchema": schema,
            "Projection": {"ProjectionType": "ALL"},
        }}],
    }))
    .expect("build the UpdateTable input");
    s.engine
        .update_table(ACCOUNT, input)
        .await
        .expect("add the GSI");
}

fn item(n: u32, g: Option<&str>, r: Option<u32>) -> Item {
    let mut it = BTreeMap::from([
        ("pk".to_owned(), AttributeValue::S(format!("p{}", n % 5))),
        ("sk".to_owned(), AttributeValue::N(n.to_string())),
    ]);
    if let Some(g) = g {
        it.insert("g".to_owned(), AttributeValue::S(g.to_owned()));
    }
    if let Some(r) = r {
        it.insert("r".to_owned(), AttributeValue::N(r.to_string()));
    }
    it
}

async fn put(s: &Scratch, key_info: &TableKeyInfo, it: Item) {
    s.engine
        .put_item(key_info, it, false, None, &ExpressionMaps::default(), None)
        .await
        .expect("put");
}

async fn ids(db: &PgPool) -> (String, String, String) {
    sqlx::query_as(
        "SELECT t.table_id, i.index_id, i.index_status FROM tables t \
         JOIN indexes i ON i.table_id = t.table_id \
         WHERE t.table_name = $1 AND i.index_name = 'gidx'",
    )
    .bind(TABLE)
    .fetch_one(db)
    .await
    .expect("look up the ids")
}

/// `(base_pk, base_sk, g, item_data)` for every row of the index: the key
/// columns and the projected item, so a row with the right key but a stale
/// projection shows up as a difference.
async fn index_rows(db: &PgPool, index_id: &str) -> BTreeSet<(String, String, String, String)> {
    let rows: Vec<(String, String, String, String)> = sqlx::query_as(&format!(
        "SELECT base_pk, base_sk_n::text, pk, item_data::text FROM \"_ddb_{index_id}\""
    ))
    .fetch_all(db)
    .await
    .expect("read the index table");
    rows.into_iter().collect()
}

/// `(pk, sk, g, item_data)` for every base item carrying `g`; the GSIs in this
/// file project ALL, so the index row's item is the base item.
async fn expected_rows(db: &PgPool, table_id: &str) -> BTreeSet<(String, String, String, String)> {
    let rows: Vec<(String, String, String, String)> = sqlx::query_as(&format!(
        "SELECT pk, sk_n::text, item_data->'g'->>'S', item_data::text FROM \"_ddb_{table_id}\" \
         WHERE item_data ? 'g'"
    ))
    .fetch_all(db)
    .await
    .expect("read the base table");
    rows.into_iter().collect()
}

/// Wait for the GSI to become ACTIVE. UpdateTable publishes it before returning
/// unless the build hit a transient failure, in which case the recovery worker
/// (not running in these tests) would; a short wait tells the two apart.
async fn wait_active(db: &PgPool) -> String {
    let mut status = String::new();
    for _ in 0..100 {
        status = ids(db).await.2;
        if status == "ACTIVE" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    status
}

async fn wait_queue_empty(db: &PgPool) {
    for _ in 0..500 {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM gsi_pending")
            .fetch_one(db)
            .await
            .expect("count pending rows");
        if n == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the propagation queue did not drain");
}

/// Writers race an UpdateTable that adds `gidx`, then the index must equal the
/// base table. Copied from `gsi_online_build.rs`.
async fn concurrent_build(delay_ms: u32) {
    let s = scratch().await;
    let key_info = create_table(&s).await;
    // Several backfill pages' worth, so pages run while writers hold their rows.
    for n in 0..1200 {
        put(
            &s,
            &key_info,
            item(n, Some(&format!("g{}", n % 4)), Some(n)),
        )
        .await;
    }
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('index_propagation_delay_ms', $1) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(delay_ms.to_string())
    .execute(&s.db)
    .await
    .expect("set the propagation delay");

    let stop = std::sync::atomic::AtomicBool::new(false);
    let maps = ExpressionMaps::default();
    let exists = extenddb_core::expression::parse_condition(
        &extenddb_core::expression::tokenize("attribute_exists(pk)").expect("tokenize"),
    )
    .expect("parse");
    let writer = |seed: u32| {
        let (s, key_info, stop, maps, exists) = (&s, &key_info, &stop, &maps, &exists);
        async move {
            let mut i = 0u32;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                i += 1;
                let n = (seed * 7919 + i * 31) % 1300;
                let result = match i % 4 {
                    0 => s
                        .engine
                        .delete_item(key_info, &item(n, None, None), false, None, maps, None)
                        .await
                        .map(|_| ()),
                    1 => s
                        .engine
                        .put_item(key_info, item(n, None, Some(i)), false, None, maps, None)
                        .await
                        .map(|_| ()),
                    2 => match s
                        .engine
                        .put_item(
                            key_info,
                            item(n, Some(&format!("c{}", i % 3)), Some(i)),
                            false,
                            Some(exists),
                            maps,
                            None,
                        )
                        .await
                    {
                        Err(extenddb_storage::error::StorageError::ConditionFailed(_)) => Ok(()),
                        other => other.map(|_| ()),
                    },
                    _ => s
                        .engine
                        .put_item(
                            key_info,
                            item(n, Some(&format!("w{}", i % 5)), Some(i)),
                            false,
                            None,
                            maps,
                            None,
                        )
                        .await
                        .map(|_| ()),
                };
                result.expect("a write during the build");
            }
        }
    };
    let build = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        add_gsi(&s, false).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
    };
    tokio::join!(writer(1), writer(2), writer(3), writer(4), build);

    let (table_id, index_id, _) = ids(&s.db).await;
    assert_eq!(wait_active(&s.db).await, "ACTIVE");
    wait_queue_empty(&s.db).await;
    assert_eq!(
        index_rows(&s.db, &index_id).await,
        expected_rows(&s.db, &table_id).await,
        "delay {delay_ms} ms"
    );
    s.cleanup().await;
}

/// Longer than any test here, so only a catalog change can end an entry.
const CACHE_TTL: Duration = Duration::from_secs(60);

/// The cache is process-wide and any catalog change empties it, so one test's
/// UpdateTable would empty the cache under another. The tests take turns.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn concurrent_writes_during_a_build_with_a_warm_cache_leave_the_index_equal_to_the_table() {
    let test =
        "concurrent_writes_during_a_build_with_a_warm_cache_leave_the_index_equal_to_the_table";
    if base_conn().is_none() {
        return skip(test);
    }
    let _serial = SERIAL.lock().await;
    concurrent_build(0).await;
    concurrent_build(10).await;
}

/// Writes after a GSI is deleted neither fail nor maintain the dropped index,
/// although the cache held the index when the delete ran.
#[tokio::test]
async fn writes_after_a_gsi_delete_with_a_warm_cache_succeed() {
    let test = "writes_after_a_gsi_delete_with_a_warm_cache_succeed";
    if base_conn().is_none() {
        return skip(test);
    }
    let _serial = SERIAL.lock().await;
    let s = scratch().await;
    let key_info = create_table(&s).await;
    add_gsi(&s, false).await;
    assert_eq!(wait_active(&s.db).await, "ACTIVE");
    for n in 0..50 {
        put(&s, &key_info, item(n, Some("g"), None)).await;
    }
    let input: UpdateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": TABLE,
        "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": "gidx"}}],
    }))
    .expect("build the UpdateTable input");
    s.engine
        .update_table(ACCOUNT, input)
        .await
        .expect("delete the GSI");
    for n in 50..100 {
        put(&s, &key_info, item(n, Some("g"), None)).await;
    }
    s.cleanup().await;
}

/// With the cache warm, writes do not read the index catalog. The catalog rows
/// are changed behind the engine's back (no guard), so a write that read them
/// would fail validation on the new index's key type.
#[tokio::test]
async fn a_warm_cache_serves_writes_without_reading_the_catalog() {
    let test = "a_warm_cache_serves_writes_without_reading_the_catalog";
    if base_conn().is_none() {
        return skip(test);
    }
    let _serial = SERIAL.lock().await;
    let s = scratch().await;
    let key_info = create_table(&s).await;
    add_gsi(&s, false).await;
    assert_eq!(wait_active(&s.db).await, "ACTIVE");
    put(&s, &key_info, item(1, Some("g"), None)).await;
    // Retype the GSI key to N in the catalog only. A fresh read would now reject
    // an S value for `g`; the cached definitions still accept it.
    sqlx::query(
        "UPDATE tables SET attribute_definitions = \
         '[{\"AttributeName\":\"pk\",\"AttributeType\":\"S\"},\
           {\"AttributeName\":\"sk\",\"AttributeType\":\"N\"},\
           {\"AttributeName\":\"g\",\"AttributeType\":\"N\"}]'::jsonb \
         WHERE table_name = $1",
    )
    .bind(TABLE)
    .execute(&s.db)
    .await
    .expect("retype g in the catalog");
    put(&s, &key_info, item(2, Some("g"), None)).await;
    s.cleanup().await;
}
