// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared post-read processing for Query and Scan operations.
//!
//! Both operations apply the same filter → project → 1 MB limit pipeline
//! after fetching raw items from storage. This module extracts that shared
//! logic to avoid duplication.

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    Expr, ExpressionMaps, PathElement, apply_projection, evaluate_condition,
};
use extenddb_core::types::{
    IndexInfo, Item, KeySchemaElement, Select, extract_key, item_size_bytes,
};

use crate::index_helpers::apply_index_projection;

/// Result of the post-read processing pipeline.
pub struct PostReadResult {
    pub items: Option<Vec<Item>>,
    pub count: i64,
    pub scanned_count: i64,
    pub last_evaluated_key: Option<Item>,
}

/// Apply FilterExpression, ProjectionExpression, and 1 MB response size limit
/// to raw items returned from storage.
///
/// This is the shared pipeline used by both `Query` and `Scan`.
#[allow(clippy::cast_possible_wrap)] // item counts won't exceed i64::MAX
#[allow(clippy::too_many_arguments)] // all parameters are distinct concerns from the caller
pub fn apply_post_read(
    raw_items: &[Item],
    storage_last_key: Option<Item>,
    filter: &Option<Expr>,
    projection: &Option<Vec<Vec<PathElement>>>,
    maps: &ExpressionMaps,
    lek_key_schema: &[KeySchemaElement],
    select: Option<&Select>,
    index_proj: Option<&IndexInfo>,
    base_key_schema: &[KeySchemaElement],
) -> Result<PostReadResult, DynamoDbError> {
    let mut scanned_count: i64 = 0;
    let mut result_items = Vec::new();
    let mut filtered_count: i64 = 0;
    let mut response_bytes: usize = 0;
    let mut last_evaluated_key = storage_last_key;
    let mut last_processed_key: Option<Item> = None;
    let is_count = matches!(select, Some(Select::Count));

    for item in raw_items {
        let item_bytes = item_size_bytes(item);
        if response_bytes + item_bytes > 1_048_576
            && (filtered_count > 0 || !result_items.is_empty())
        {
            // Hit the 1 MB limit — LastEvaluatedKey is the last *processed* item's key,
            // not the current item, so the next paginated request doesn't skip it.
            last_evaluated_key = last_processed_key;
            break;
        }
        response_bytes += item_bytes;
        scanned_count += 1;
        last_processed_key = Some(extract_key(item, lek_key_schema));

        if let Some(filter_expr) = filter {
            let passed = evaluate_condition(filter_expr, item, maps).map_err(|e| {
                let msg = e.to_string();
                if msg.starts_with("Invalid ") {
                    DynamoDbError::ValidationException(msg)
                } else {
                    DynamoDbError::ValidationException(format!("Invalid FilterExpression: {msg}"))
                }
            })?;
            if !passed {
                continue;
            }
        }

        filtered_count += 1;

        if !is_count {
            let projected = if let Some(paths) = projection {
                apply_projection(item, paths, maps)?
            } else if let Some(idx) = index_proj {
                apply_index_projection(item, idx, base_key_schema)
            } else {
                item.clone()
            };
            result_items.push(projected);
        }
    }

    let count = if is_count {
        filtered_count
    } else {
        i64::try_from(result_items.len()).unwrap_or(i64::MAX)
    };

    Ok(PostReadResult {
        items: if is_count { None } else { Some(result_items) },
        count,
        scanned_count,
        last_evaluated_key,
    })
}
