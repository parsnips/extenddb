// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Stable stream buckets derived from the physical routing key.

use extenddb_core::types::{Item, KeySchemaElement};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::pk_to_text;
use serde::{Deserialize, Serialize};

/// Mapping used by newly created PostgreSQL streams. Neki must use the same
/// hash, seed, and bucket mapping for `pk`/`base_pk` and `shard_id`. A bucket must never be split between physical shards.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StreamSharding {
    pub hash: StreamHash,
    #[serde(default)]
    pub mapping: BucketMapping,
    #[serde(deserialize_with = "extenddb_storage::config::string_coerce::u32")]
    pub bucket_count: u32,
    #[serde(
        default,
        deserialize_with = "extenddb_storage::config::string_coerce::u64"
    )]
    pub seed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamHash {
    Crc32,
    Xxhash64,
    Xxh3_64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BucketMapping {
    #[default]
    Modulo,
    /// Equal-width ranges of a 64-bit hash, using its high bits.
    HashRange,
}

impl StreamSharding {
    /// Validate before connecting or creating any stream metadata.
    pub fn validate(&self) -> Result<(), StorageError> {
        if !(1..=65_536).contains(&self.bucket_count) {
            return Err(StorageError::Validation(
                "stream_sharding.bucket_count must be between 1 and 65536".to_owned(),
            ));
        }
        if self.hash == StreamHash::Crc32 && self.seed != 0 {
            return Err(StorageError::Validation(
                "stream_sharding.seed must be zero for crc32".to_owned(),
            ));
        }
        if self.mapping == BucketMapping::HashRange
            && (self.hash == StreamHash::Crc32 || !self.bucket_count.is_power_of_two())
        {
            return Err(StorageError::Validation(
                "hash_range requires a 64-bit hash and a power-of-two bucket_count".to_owned(),
            ));
        }
        Ok(())
    }

    /// Hash the UTF-8 bytes of the exact encoded base-table partition key.
    pub fn bucket(&self, base_pk: &str) -> Result<u32, StorageError> {
        self.validate()?;
        let hash = match self.hash {
            StreamHash::Crc32 => u64::from(crc32fast::hash(base_pk.as_bytes())),
            StreamHash::Xxhash64 => xxhash_rust::xxh64::xxh64(base_pk.as_bytes(), self.seed),
            StreamHash::Xxh3_64 => {
                xxhash_rust::xxh3::xxh3_64_with_seed(base_pk.as_bytes(), self.seed)
            }
        };
        Ok(match self.mapping {
            BucketMapping::Modulo => (hash % u64::from(self.bucket_count)) as u32,
            BucketMapping::HashRange => {
                ((u128::from(hash) * u128::from(self.bucket_count)) >> 64) as u32
            }
        })
    }

    /// One public shard ID hashing to each bucket. The suffix is a search nonce,
    /// not a bucket number. Filling all buckets together avoids quadratic work.
    pub(crate) fn shard_ids(&self, table_id: &str) -> Result<Vec<String>, StorageError> {
        self.validate()?;
        let mut ids = vec![String::new(); self.bucket_count as usize];
        let mut remaining = self.bucket_count;
        for nonce in 0..u64::from(self.bucket_count) * 128 {
            let candidate = format!("shardId-{table_id}-{nonce:016}");
            let bucket = self.bucket(&candidate)? as usize;
            if ids[bucket].is_empty() {
                ids[bucket] = candidate;
                remaining -= 1;
                if remaining == 0 {
                    return Ok(ids);
                }
            }
        }
        Err(StorageError::Internal(
            "Could not generate stream IDs for every routing bucket".to_owned(),
        ))
    }
}

/// NULL routing identifies pre-configuration streams; retain their exact
/// assignment, including use of only the first attribute for composite keys.
#[derive(sqlx::FromRow)]
pub(crate) struct StreamShard {
    pub shard_id: String,
    pub routing: Option<sqlx::types::Json<StreamSharding>>,
}

/// Resolve immutable stream metadata before opening the item transaction.
/// Enumerating buckets may fan out; it must not enlist their owners in the
/// transaction that writes the base item and its one stream record.
pub(crate) async fn load(
    pool: &sqlx::PgPool,
    table_id: &str,
) -> Result<Vec<StreamShard>, StorageError> {
    sqlx::query_as(
        "SELECT shard_id, routing FROM stream_shards WHERE table_id = $1 ORDER BY shard_id",
    )
    .bind(table_id)
    .fetch_all(pool)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))
}

pub(crate) fn legacy_partition_key(
    item: &Item,
    schema: &[KeySchemaElement],
) -> Result<String, StorageError> {
    let value = schema
        .first()
        .and_then(|key| item.get(&key.attribute_name))
        .ok_or_else(|| StorageError::Internal("missing stream partition key".to_owned()))?;
    Ok(pk_to_text(value)?.into_owned())
}

/// Legacy rows are ordered by shard_id. Configured IDs hash to their bucket.
/// Check the persisted mapping and complete bucket set before assigning a row;
/// partial/mixed metadata must fail the base-item transaction, not lose an event.
pub(crate) fn assign<'a>(
    shards: &'a [StreamShard],
    base_pk: &str,
    legacy_pk: &str,
) -> Result<&'a str, StorageError> {
    let first = shards.first().ok_or_else(|| {
        StorageError::Internal("No stream shards available for stream capture".to_owned())
    })?;
    if shards.iter().any(|shard| shard.routing != first.routing) {
        return Err(StorageError::Internal(
            "Inconsistent stream routing metadata".to_owned(),
        ));
    }
    if let Some(routing) = &first.routing {
        routing.validate()?;
        if shards.len() != routing.bucket_count as usize {
            return Err(StorageError::Internal(
                "Incomplete stream routing buckets".to_owned(),
            ));
        }
        let wanted = routing.bucket(base_pk)?;
        let mut seen = vec![false; shards.len()];
        let mut selected = None;
        for shard in shards {
            let bucket = routing.bucket(&shard.shard_id)?;
            if std::mem::replace(&mut seen[bucket as usize], true) {
                return Err(StorageError::Internal(
                    "Duplicate stream routing bucket".to_owned(),
                ));
            }
            if bucket == wanted {
                selected = Some(shard.shard_id.as_str());
            }
        }
        selected.ok_or_else(|| StorageError::Internal("Missing stream routing bucket".to_owned()))
    } else {
        let bucket =
            (u64::from(crc32fast::hash(legacy_pk.as_bytes())) % shards.len() as u64) as usize;
        Ok(&shards[bucket].shard_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn neki_hash_ranges_and_generated_ids_agree() {
        let mut routing = StreamSharding {
            hash: StreamHash::Xxh3_64,
            mapping: BucketMapping::HashRange,
            bucket_count: 256,
            seed: 0,
        };
        // XXH3-64 vectors: empty=2d06800538d394c2, abc=78af5f94892f3950.
        assert_eq!(routing.bucket("").unwrap(), 0x2d);
        assert_eq!(routing.bucket("abc").unwrap(), 0x78);
        let ids = routing
            .shard_ids("00000000-0000-0000-0000-000000000000")
            .unwrap();
        for (bucket, id) in ids.iter().enumerate() {
            assert!(id.len() >= 28 && id.len() <= 65);
            assert_eq!(
                (xxhash_rust::xxh3::xxh3_64(id.as_bytes()) >> 56) as usize,
                bucket
            );
        }
        let mut shards: Vec<_> = ids
            .into_iter()
            .map(|shard_id| StreamShard {
                shard_id,
                routing: Some(sqlx::types::Json(routing.clone())),
            })
            .collect();
        shards.reverse(); // Database ORDER BY shard_id is not bucket order.
        for i in 0..1000 {
            let key = format!("tenant-{i}");
            let shard = assign(&shards, &key, "ignored").unwrap();
            assert_eq!(
                xxhash_rust::xxh3::xxh3_64(key.as_bytes()) >> 56,
                xxhash_rust::xxh3::xxh3_64(shard.as_bytes()) >> 56
            );
        }
        shards[0].shard_id = shards[1].shard_id.clone();
        assert!(assign(&shards, "tenant", "ignored").is_err());
        routing.bucket_count = 3;
        assert!(routing.validate().is_err());
        routing.bucket_count = 1;
        assert_eq!(routing.bucket("anything").unwrap(), 0);
    }

    #[test]
    fn routing_vectors_and_validation() {
        let mut routing = StreamSharding {
            hash: StreamHash::Xxhash64,
            mapping: BucketMapping::Modulo,
            bucket_count: 256,
            seed: 0,
        };
        // Published XXH64 seed-zero vectors: empty=ef46db3751d8e999,
        // a=d24ec4f1a98c6e5b, abc=44bc2cf5ad770999.
        assert_eq!(routing.bucket("").unwrap(), 0x99);
        assert_eq!(routing.bucket("a").unwrap(), 0x5b);
        assert_eq!(routing.bucket("abc").unwrap(), 0x99);
        routing.hash = StreamHash::Crc32;
        assert_eq!(routing.bucket("123456789").unwrap(), 0x26); // CRC32 cbf43926
        routing.seed = 1;
        assert!(routing.validate().is_err());
        routing.seed = 0;
        for count in [0, 65_537, u32::MAX] {
            routing.bucket_count = count;
            assert!(routing.bucket("key").is_err());
        }
    }

    #[test]
    fn assignment_uses_base_key_and_rejects_missing_or_mixed_buckets() {
        let routing = StreamSharding {
            hash: StreamHash::Xxhash64,
            mapping: BucketMapping::Modulo,
            bucket_count: 16,
            seed: 7,
        };
        let mut shards: Vec<_> = routing
            .shard_ids("table")
            .unwrap()
            .into_iter()
            .map(|shard_id| StreamShard {
                shard_id,
                routing: Some(sqlx::types::Json(routing.clone())),
            })
            .collect();
        for key in ["tenant", "AP8=", "123.5", "3:é:,3:x,y,"] {
            assert_eq!(
                assign(&shards, key, "ignored").unwrap(),
                shards[routing.bucket(key).unwrap() as usize].shard_id
            );
        }
        shards[3].routing = None;
        assert!(assign(&shards, "tenant", "ignored").is_err());
        shards[3].routing = Some(sqlx::types::Json(routing));
        shards.remove(3);
        assert!(assign(&shards, "tenant", "ignored").is_err());
        assert!(assign(&[], "tenant", "ignored").is_err());
    }

    #[test]
    fn legacy_assignment_does_not_move_existing_composite_keys() {
        let shards: Vec<_> = (0..4)
            .map(|bucket| StreamShard {
                shard_id: format!("shardId-table-{bucket:016}"),
                routing: None,
            })
            .collect();
        let bucket = crc32fast::hash(b"first") as usize % 4;
        assert_eq!(
            assign(&shards, "5:first,6:second,", "first").unwrap(),
            shards[bucket].shard_id
        );
    }
}
