# RFC: PostgreSQL base partition keys on stream and pending-index rows

- Status: Draft (fork implementation available for review)
- Created: 2026-09-16

## Summary

Add a non-null `base_pk TEXT` column to PostgreSQL `stream_records` and
`gsi_pending`. Its value uses the same `composite_pk_to_text` encoding as the
base table's `pk` and a secondary index's `base_pk`. A sharding layer can use
these columns to place an item's data, indexes, and events on the same physical
shard. This patch supplies routing keys and optional stream routing; it does not
provision Neki topology or redistribute existing data.

## Motivation

Without distributed atomic commit, a base mutation and its stream event must
stay in one shard transaction. The stream's logical `shard_id` groups many base
partition keys and cannot identify that physical placement. Similarly, the
pending-index queue's `worker_partition` identifies a consumer bucket, not the
base item's shard.

## Detailed design

Both stream insertion paths bind `base_pk`: transactional item capture and the
standalone `StreamEngine::write_stream_record` implementation. The latter reads
the table key schema from the catalog and encodes the record's `Keys`. Capturing
a deletion uses the old item; capturing an insert/update uses the new item.
`KEYS_ONLY` streams need no item images to determine the routing key.

Pending GSI and vector updates use their snapshotted base key schema and the new
item, or old item for deletes. An update without either item is rejected.
Enqueue-time `ready_at` ordering is scoped to `(table_id, base_pk)`, with an index
on `(table_id, base_pk, ready_at)`, instead of reading every key in a worker
bucket. Updates to unrelated keys no longer inherit one another's delay.
Claimed-row deletion includes both `id` and `base_pk`.

The key is encoded text, not a precomputed xxhash. Neki must apply the same
routing function and shard mapping to base `pk`, index `base_pk`, stream
`base_pk`, and queue `base_pk`. Hashing these values again in ExtendDB could
produce a different placement. Single HASH attributes use their ordinary
storage encoding; multiple HASH attributes use the existing netstring encoding
in key-schema order. Sort-key attributes do not enter the routing value.

By default, logical stream shard IDs, sequence numbers, stream API payloads,
queue IDs, and worker assignments retain their existing meanings. Configured
stream bucket assignment is described below. Ordinary PostgreSQL tables and
their primary keys remain unchanged apart from the added columns/index.

### Configured stream buckets

`[storage.postgres.stream_sharding]` enables immutable, configured buckets for
new streams. Supported hashes are `crc32`, `xxhash64` (XXH64), and `xxh3_64`
(XXH3-64). `mapping = "modulo"` uses the hash remainder; `mapping = "hash_range"`
uses the high bits of a 64-bit hash and requires a power-of-two bucket count.
Neki's native `xxhash` index uses the latter with `xxh3_64` and seed zero.

Assignment hashes the UTF-8 bytes of the exact encoded `base_pk`, including
all HASH attributes for composite keys. Shard creation searches for one public
shard ID whose hash falls in each bucket. Consequently, **the shard ID and every
base key assigned to it hash into the same bucket**. The ID includes the table
UUID and a search nonce, fits the SDK length limits, and does not expose a key.
Its numeric suffix is not the bucket number.

Migration `006_stream_shard_routing` adds nullable JSON `routing` metadata to
`stream_shards`. New configured streams persist the hash, mapping, seed, and
count with their shard rows. Existing streams retain their previous mapping
(NULL means legacy CRC32/4), including through disable/re-enable. Configuration
changes apply only to newly created streams. Converting legacy streams requires
a separate migration of retained records and consumer cursors.

Both transactional capture and standalone insertion use the same assignment.
Standalone insertion rejects the wrong supplied shard ID. Missing, incomplete,
or inconsistent shard metadata fails capture instead of silently omitting an
event. Metadata enumeration happens before opening the item transaction so it
does not enlist all bucket owners in that transaction. Composite-key mutations
also use the same full key encoding for their base-row access.

Stream reads, latest-sequence reads, and shard validation use the ordinary data
pool. Neki owns physical routing, including routing stream-record queries by
`shard_id`. ExtendDB does not store node UIDs or set session routing overrides.
Bucket ownership may change through Neki while the bucket hash mapping stays
fixed.

See [Neki stream sharding](../neki-stream-sharding.md) for matching configuration,
a topology example, and query-plan verification. Matching physical routing is
an operator-supplied contract, not something this patch installs or discovers.

### Upgrade

Stop all ExtendDB writers and background workers. Using the new binary, run:

```bash
extenddb migrate --config extenddb.toml --yes
```

Then restart with the new binary. Initialization also applies the migration.
`005_base_pk_routing` is tracked in the data database's `schema_history`; it does
not require a catalog version bump. Do not mix old and new writers: old writers
omit a now-required column. Reverting just the binary is not a supported rollback.

The code migration adds nullable columns, backfills existing rows, enforces
`NOT NULL`, creates the queue lookup index, and records its completion in one
data transaction. It reads records in bounded batches using the existing primary
keys and reuses the Rust key encoder. A failure rolls back this entire migration,
including DDL and its ledger entry, so it can be retried after repair.

Stream backfill uses saved `dynamodb.Keys` plus the catalog schema, without
looking up the live item. Queue backfill uses its saved item and index context,
including vector contexts. Existing payloads and ordering fields are preserved.
If a deleted table has retained stream rows but no catalog schema, the migration
fails instead of guessing which attributes are HASH keys. Restore the schema or
allow the old deployment's stream retention cleanup to expire those records
before retrying. Malformed keys or queue contexts also fail the migration.

## Drawbacks

The migration holds exclusive locks and rewrites retained stream and queue rows;
plan downtime and disk/WAL headroom proportional to their size. Each new row
stores another copy of its partition key. The queue adds an index to support
its new ordering lookup.

## Remaining Neki integration

This change is necessary but insufficient for single-shard atomicity:

- Configure matching column types, routing functions, and shard placement for
  all four table families. Existing data must be redistributed before relying
  on co-location.
- For actual GSI contents to commit with the base item, use an effective index
  propagation delay of zero. With a delay, only the durable queue entry commits
  with the base item; applying it is a later transaction.
- Stream capture still reads `stream_shards`, uses a foreign key to it, and
  allocates from `stream_seq`. Their placement/sequence semantics need a Neki
  design. The existing empty-shard capture path also silently skips a record;
  strict capture requires a separate lifecycle/fail-closed change.
- Queue claiming still scans worker buckets and checks `vector_index_holds`.
  Workers must claim on physical shards for local claim/apply/delete transactions.
- Stream consumers query a logical shard across base keys. Configured buckets
  make these records colocated under the matching topology contract;
  legacy shards do not. Sequence allocation and commit visibility still need
  their own ordering design; this change does not fix the pre-commit sequence
  cursor race or introduce consumer locks.
- Transaction idempotency, metadata placement, and multi-item writes need an
  explicit policy. Configure Neki to reject transactions spanning physical
  shards when atomicity cannot be provided; co-location of a single key does not
  make arbitrary `TransactWriteItems` requests atomic.

## Alternatives

Using the base key as `shard_id` would change stream enumeration and iterator
semantics, potentially creating a logical shard per key. A separate column
preserves the current stream API and lets physical placement evolve separately.

## Validation

PostgreSQL tests use separate catalog and data databases. They cover fresh
migration, populated upgrade spanning multiple batches, failed migration and
retry, repeated migration, required columns, string/numeric/binary/composite
keys, GSI/vector queue contexts, deleted-item backfill, both stream write paths,
update/delete capture, per-key queue ordering, and rollback on a rejected stream
insert. These tests establish PostgreSQL behavior, not Neki routing correctness.

Stream-routing tests cover fixed hash vectors, config deserialization and
validation, encoded string/numeric/binary/composite keys, both insertion paths,
deletion and consumption, mapping persistence after configuration changes,
rejection of wrong shard IDs, and rollback with incomplete shard metadata.

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.
