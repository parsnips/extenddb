# PostgreSQL stream buckets on Neki

Configure ExtendDB and Neki to use **XXH3-64, seed zero, and the same hash
ranges**. ExtendDB generates a stream shard ID that hashes into the same bucket
as every `base_pk` assigned to it. Base rows, derived rows, and stream metadata
therefore land on the same physical shard when their Neki layouts agree.

This guide uses four equal buckets on four machines. Replace `shard-a` through
`shard-d` with your physical Neki shard UIDs. Neki's native `xxhash` index is
XXH3-64, not the older XXH64 algorithm. See the
[Neki topology reference](https://planetscale.com/docs/neki/data-topology).

## ExtendDB configuration

Keep your existing connection and pool settings. Add:

```toml
[storage.postgres.stream_sharding]
hash = "xxh3_64"
mapping = "hash_range"
bucket_count = 4
seed = 0
```

For this configuration, the bucket is the top two bits of the 64-bit hash.
All keys are hashed as the UTF-8 bytes of their stored `pk`/`base_pk` TEXT value.
Do not cast numeric-looking partition keys to SQL numbers in the Neki index.
Composite keys use ExtendDB's existing encoding of all HASH attributes; the
sort key does not enter the hash.

All stream reads use the ordinary data pool. Neki owns the physical routing;
ExtendDB has no node list, dedicated shard pools, or session routing overrides.

## Neki topology

The following is a placement template for the relevant table bindings. Merge it into
your complete existing topology, preserving its authoritative shard and other
bindings. The three data groups use identical physical UIDs and ranges; their
default indexes differ only in column name.

The deployment also needs its existing Neki routing for `stream_records` queries
with a `shard_id` predicate. The placement bindings below do not by themselves
specify that additional read route. Verify the ordinary queries below use it.

```json
{
  "shard_indexes": {
    "extenddb_pk": { "type": "xxhash", "columns": ["pk"] },
    "extenddb_base_pk": { "type": "xxhash", "columns": ["base_pk"] },
    "extenddb_stream_id": { "type": "xxhash", "columns": ["shard_id"] }
  },
  "shard_groups": [
    {
      "uid": "extenddb_base",
      "default_shard_index": "extenddb_pk",
      "key_ranges": [
        { "shard_uid": "shard-a", "end": "40" },
        { "shard_uid": "shard-b", "start": "40", "end": "80" },
        { "shard_uid": "shard-c", "start": "80", "end": "c0" },
        { "shard_uid": "shard-d", "start": "c0" }
      ]
    },
    {
      "uid": "extenddb_derived",
      "default_shard_index": "extenddb_base_pk",
      "key_ranges": [
        { "shard_uid": "shard-a", "end": "40" },
        { "shard_uid": "shard-b", "start": "40", "end": "80" },
        { "shard_uid": "shard-c", "start": "80", "end": "c0" },
        { "shard_uid": "shard-d", "start": "c0" }
      ]
    },
    {
      "uid": "extenddb_stream_metadata",
      "default_shard_index": "extenddb_stream_id",
      "key_ranges": [
        { "shard_uid": "shard-a", "end": "40" },
        { "shard_uid": "shard-b", "start": "40", "end": "80" },
        { "shard_uid": "shard-c", "start": "80", "end": "c0" },
        { "shard_uid": "shard-d", "start": "c0" }
      ]
    }
  ],
  "databases": {
    "extenddb_data": {
      "schemas": {
        "public": {
          "tables": {
            "_ddb_<BASE_TABLE_ID>": { "shard_group": "extenddb_base" },
            "_ddb_<INDEX_ID>": { "shard_group": "extenddb_derived" },
            "_ddb_vec_<VECTOR_INDEX_ID>": { "shard_group": "extenddb_derived" },
            "gsi_pending": { "shard_group": "extenddb_derived" },
            "stream_records": { "shard_group": "extenddb_derived" },
            "stream_shards": { "shard_group": "extenddb_stream_metadata" }
          }
        }
      }
    }
  }
}
```

Use the actual data database name. Replace the `_ddb_*` placeholders with each
base table, secondary index, and vector index's physical SQL table name; these
are UUID-based identifiers, not DynamoDB table names. Maintain those bindings
as tables and indexes are created. Keep catalog and other unlisted metadata on
their existing groups. This change does not automate Neki topology maintenance.

For four buckets the ownership map is:

| Bucket | Hash prefix range | Physical UID |
| --- | --- | --- |
| 0 | `00` through `3f` | `shard-a` |
| 1 | `40` through `7f` | `shard-b` |
| 2 | `80` through `bf` | `shard-c` |
| 3 | `c0` through `ff` | `shard-d` |

A bucket must fit entirely within one physical range. For finer future split
points, choose more buckets initially. Multiple buckets may share a machine.
Keep the hash/seed/bucket count fixed; Neki manages changes in physical ownership
through a coordinated data migration. Never split through a bucket. Neki topology replacement alone does not redistribute rows.

## Migration and verification

With writers stopped, apply the schema migration using the new binary:

```bash
extenddb migrate --config extenddb.toml --yes
```

Existing stream mappings remain intact. The new configuration applies when
stream shards are first created; disable/re-enable does not recreate them.
Legacy streams are not converted by changing configuration. Provision new
streams before writing data under this mapping,
or coordinate a separate migration of existing records and consumer cursors.
Do not mix frontends using different stream configurations.

In a fresh Neki session, verify base-row and metadata routing using actual keys
and IDs from a captured record:

```sql
EXPLAIN (NEKI_PLAN, COSTS OFF, FORMAT TEXT)
SELECT item_data FROM "_ddb_<BASE_TABLE_ID>" WHERE pk = '<encoded base_pk>';

EXPLAIN (NEKI_PLAN, COSTS OFF, FORMAT TEXT)
SELECT shard_id FROM stream_shards WHERE shard_id = '<stream shard ID>';

EXPLAIN (NEKI_PLAN, COSTS OFF, FORMAT TEXT)
SELECT record_data FROM stream_records
WHERE shard_id = '<stream shard ID>' ORDER BY sequence_number LIMIT 100;
```

Verify that these ordinary queries resolve to the same single owner, without
scatter. For a custom consumer that locks/checkpoints in SQL, keep its
checkpoint/lock rows colocated and use predicates Neki can route to that owner.
ExtendDB's public stream API remains stateless and does not acquire consumer leases.

The PostgreSQL tests verify hash vectors, generated-ID placement, key encoding,
transaction rollback, and stream reads through the ordinary data pool. A live Neki query-plan
check is still required to validate your deployed topology. This change does
not resolve global sequence commit ordering, asynchronous GSI worker claiming,
multi-shard control-plane operations, or arbitrary multi-key transactions. Use
zero effective index propagation delay when actual GSI rows must commit with
the base item; otherwise only their queue entries do.

Copyright 2026 ExtendDB contributors. Licensed under Apache-2.0.
