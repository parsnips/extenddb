// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Create-only puts: a PutItem whose condition is `attribute_not_exists` on a
//! key attribute. With nothing else to do in a transaction, the write is one
//! autocommit `INSERT ... ON CONFLICT DO NOTHING`; these tests hold it to the
//! answers of the transaction path.
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
const TABLE: &str = "t_create_only";

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
    let base = base_conn().expect("caller checks base_conn() first");
    let db_name = format!("eddb_cput_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
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

fn cond(expr: &str) -> extenddb_core::expression::Expr {
    extenddb_core::expression::parse_condition(
        &extenddb_core::expression::tokenize(expr).expect("tokenize"),
    )
    .expect("parse")
}

async fn put_if(
    s: &Scratch,
    key_info: &TableKeyInfo,
    it: Item,
    expr: &str,
    maps: &ExpressionMaps,
) -> Result<Option<Item>, extenddb_storage::error::StorageError> {
    let c = cond(expr);
    s.engine
        .put_item(key_info, it, false, Some(&c), maps, None)
        .await
}

async fn stored(s: &Scratch, key_info: &TableKeyInfo, n: u32) -> Option<Item> {
    s.engine
        .get_item(key_info, &item(n, None, None))
        .await
        .expect("get")
}

#[tokio::test]
async fn create_only_put_inserts_when_absent_and_reports_the_item_when_present() {
    let test = "create_only_put_inserts_when_absent_and_reports_the_item_when_present";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = create_table(&s).await;
    let maps = ExpressionMaps::default();
    let mut named = ExpressionMaps::default();
    named.names.insert("k".to_owned(), "pk".to_owned());

    for (n, expr, maps) in [
        (1, "attribute_not_exists(pk)", &maps),
        (2, "attribute_not_exists(sk)", &maps),
        (
            3,
            "attribute_not_exists(pk) AND attribute_not_exists(sk)",
            &maps,
        ),
        (4, "attribute_not_exists(#k)", &named),
    ] {
        let first = item(n, Some("first"), None);
        put_if(&s, &key_info, first.clone(), expr, maps)
            .await
            .unwrap_or_else(|e| panic!("{expr}: first put: {e:?}"));
        assert_eq!(
            stored(&s, &key_info, n).await,
            Some(first.clone()),
            "{expr}"
        );

        match put_if(&s, &key_info, item(n, Some("second"), None), expr, maps).await {
            Err(extenddb_storage::error::StorageError::ConditionFailed(Some(old))) => {
                assert_eq!(old, first, "{expr}: the failure names the stored item");
            }
            other => panic!("{expr}: second put: expected ConditionFailed(Some), got {other:?}"),
        }
        assert_eq!(
            stored(&s, &key_info, n).await,
            Some(first),
            "{expr}: unchanged"
        );
    }
    s.cleanup().await;
}

/// `attribute_not_exists` on a non-key attribute is not create-only: an
/// existing item without the attribute passes and is overwritten.
#[tokio::test]
async fn not_exists_on_a_non_key_attribute_still_overwrites() {
    let test = "not_exists_on_a_non_key_attribute_still_overwrites";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = create_table(&s).await;
    let maps = ExpressionMaps::default();
    put(&s, &key_info, item(1, None, None)).await;
    let next = item(1, Some("g"), None);
    put_if(
        &s,
        &key_info,
        next.clone(),
        "attribute_not_exists(g)",
        &maps,
    )
    .await
    .expect("passes: the stored item has no g");
    assert_eq!(stored(&s, &key_info, 1).await, Some(next));
    s.cleanup().await;
}

/// Concurrent create-only puts of one key: exactly one wins, and every loser
/// reports the winner's item.
#[tokio::test]
async fn concurrent_create_only_puts_have_one_winner() {
    let test = "concurrent_create_only_puts_have_one_winner";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = create_table(&s).await;
    let maps = ExpressionMaps::default();
    for round in 0..20u32 {
        let n = 100 + round;
        let attempts = (0..8u32).map(|w| {
            let it = item(n, Some(&format!("w{w}")), None);
            put_if(&s, &key_info, it, "attribute_not_exists(pk)", &maps)
        });
        let results = futures::future::join_all(attempts).await;
        let winners = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(winners, 1, "round {round}: {results:?}");
        let winner = stored(&s, &key_info, n).await.expect("the winner's item");
        for r in &results {
            match r {
                Ok(_) => {}
                Err(extenddb_storage::error::StorageError::ConditionFailed(Some(old))) => {
                    assert_eq!(old, &winner, "round {round}");
                }
                Err(e) => panic!("round {round}: {e:?}"),
            }
        }
    }
    s.cleanup().await;
}

/// On a table with a GSI the put keeps the transaction path, and the index
/// gets its row.
#[tokio::test]
async fn create_only_put_on_an_indexed_table_maintains_the_index() {
    let test = "create_only_put_on_an_indexed_table_maintains_the_index";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = create_table(&s).await;
    add_gsi(&s, false).await;
    assert_eq!(wait_active(&s.db).await, "ACTIVE");
    let maps = ExpressionMaps::default();
    for n in 0..10 {
        put_if(
            &s,
            &key_info,
            item(n, Some("g"), None),
            "attribute_not_exists(pk)",
            &maps,
        )
        .await
        .expect("create");
    }
    wait_queue_empty(&s.db).await;
    let (table_id, index_id, _) = ids(&s.db).await;
    assert_eq!(
        index_rows(&s.db, &index_id).await,
        expected_rows(&s.db, &table_id).await
    );
    s.cleanup().await;
}
