// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `Query` operation handler.

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::PathElement;
use extenddb_core::expression::{parse_key_condition, parse_projection, tokenize_with_limit};
use extenddb_core::types::{
    IndexType, QueryInput, QueryOutput, Select, TableKeyInfo, item_size_bytes,
};
use extenddb_storage::DataEngine;
use extenddb_storage::TableEngine;

use crate::OperationContext;
use crate::capacity_helpers;
use crate::create_table::storage_err_to_dynamo;
use crate::expression_helpers::{build_expression_maps, parse_optional_filter};
use crate::index_helpers::combined_lek_key_schema;
use crate::read_helpers::apply_post_read;
use crate::serialize_output;
use crate::{DispatchMetrics, DispatchResult};

/// Handle a `Query` request.
///
/// Parses `KeyConditionExpression`, queries the storage layer, applies
/// `FilterExpression` post-read, applies `ProjectionExpression`, and
/// enforces the 1 MB response size limit.
///
/// # Errors
///
/// Returns `DynamoDbError` for validation failures, missing tables, or storage errors.
#[allow(clippy::cast_possible_wrap)] // item counts won't exceed i64::MAX
pub async fn handle_query<S: TableEngine + DataEngine>(
    body: Value,
    ctx: &OperationContext<S>,
) -> Result<DispatchResult, DynamoDbError> {
    let input: QueryInput = serde_json::from_value(body).map_err(crate::deserialize_error)?;

    // P118: Fetch key_info first so we can use table_id for index lookup.
    let key_info = ctx
        .table_key_info(&input.table_name)
        .await
        .map_err(storage_err_to_dynamo)?;

    // GSI/LSI: resolve index metadata if querying a secondary index.
    // Uses table_id from pre-fetched key_info to skip redundant table lookup (P118 #4).
    let index_info = if let Some(ref idx_name) = input.index_name {
        Some(
            ctx.storage
                .index_info_by_table_id(&key_info.table_id, idx_name)
                .await
                .map_err(storage_err_to_dynamo)?,
        )
    } else {
        None
    };

    // ConsistentRead is not supported on GSI queries (tenet 1: fidelity).
    if input.consistent_read == Some(true) {
        if let Some(ref idx) = index_info {
            if idx.index_type == IndexType::Gsi {
                return Err(DynamoDbError::ValidationException(
                    "Consistent reads are not supported on global secondary indexes".to_owned(),
                ));
            }
        }
    }

    // Validate Limit >= 1 (REQ-QUERY-001)
    if let Some(limit) = input.limit {
        if limit < 1 {
            return Err(DynamoDbError::ValidationException(format!(
                "1 validation error detected: Value '{limit}' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"
            )));
        }
    }

    // For index queries, build a key_info that reflects the index's key schema
    // so the storage layer uses the correct SK column for the index table.
    let query_key_info = if let Some(ref idx) = index_info {
        TableKeyInfo {
            table_name: key_info.table_name.clone(),
            account_id: key_info.account_id.clone(),
            table_id: key_info.table_id.clone(),
            key_schema: idx.key_schema.clone(),
            attribute_definitions: key_info.attribute_definitions.clone(),
            has_lsi: key_info.has_lsi,
            stream_specification: None, // Queries don't capture stream records
        }
    } else {
        key_info.clone()
    };

    let maps = build_expression_maps(
        input.expression_attribute_names.as_ref(),
        input.expression_attribute_values.as_ref(),
    );

    // Parse KeyConditionExpression (required)
    let kce_str = input.key_condition_expression.as_deref().ok_or_else(|| {
        DynamoDbError::ValidationException(
            "Either the KeyConditions or KeyConditionExpression parameter must be specified in the request."
                .to_owned(),
        )
    })?;
    let tokens = tokenize_with_limit(kce_str, ctx.limits.max_expression_tokens)?;
    let mut key_condition = parse_key_condition(&tokens)?;

    // Correct PK/SK assignment when both clauses are equality comparisons.
    // The parser can't distinguish PK from SK without the key schema.
    let pk_attr = &query_key_info.key_schema[0].attribute_name;
    key_condition.resolve_pk_sk(pk_attr, &maps.names)?;

    // For multi-part key schemas (GSIs with >1 HASH attribute), reclassify
    // the parsed conditions so all HASH attributes go to pk_path/extra_pk_conditions
    // and the RANGE condition stays as sk_condition.
    if extenddb_core::types::is_multipart_key_schema(&query_key_info.key_schema) {
        let hash_elements = extenddb_core::types::hash_key_elements(&query_key_info.key_schema);
        let hash_attrs: Vec<&str> = hash_elements
            .iter()
            .map(|ks| ks.attribute_name.as_str())
            .collect();
        key_condition.resolve_multipart(&hash_attrs, &maps.names)?;

        // Validate all HASH attributes are present in the KeyConditionExpression.
        let provided_count = 1 + key_condition.extra_pk_conditions.len();
        if provided_count != hash_attrs.len() {
            // Find the first missing HASH attribute for the error message.
            let missing = hash_attrs
                .iter()
                .find(|attr| {
                    let pk_name = resolve_path_attr_name(&key_condition.pk_path, &maps.names);
                    if pk_name.as_deref() == Some(*attr) {
                        return false;
                    }
                    !key_condition.extra_pk_conditions.iter().any(|(path, _)| {
                        resolve_path_attr_name(path, &maps.names).as_deref() == Some(*attr)
                    })
                })
                .unwrap_or(&hash_attrs[0]);
            return Err(DynamoDbError::ValidationException(format!(
                "Query condition missed key schema element: {missing}"
            )));
        }
    }

    // Parse FilterExpression
    let filter = parse_optional_filter(input.filter_expression.as_deref(), &ctx.limits)?;

    // Parse ProjectionExpression
    let projection = if let Some(ref proj_str) = input.projection_expression {
        let proj_tokens = tokenize_with_limit(proj_str, ctx.limits.max_expression_tokens)?;
        Some(parse_projection(&proj_tokens)?)
    } else {
        None
    };

    // Validate unused expression attributes
    {
        let exprs: Vec<&extenddb_core::expression::Expr> = filter.iter().collect();
        let (mut kc_names, kc_values) = extenddb_core::expression::collect_key_condition_refs(&key_condition);
        // Collect #name refs from projection paths
        if let Some(ref proj) = projection {
            for path in proj {
                for el in path {
                    if let PathElement::Attribute(name) = el {
                        if let Some(ref_name) = name.strip_prefix('#') {
                            kc_names.insert(ref_name.to_owned());
                        }
                    }
                }
            }
        }
        extenddb_core::expression::validate_unused_attributes(
            &maps.names, &maps.values, &exprs, &[],
            &kc_names, &kc_values,
        )?;
    }

    // Validate Select vs ProjectionExpression and index requirements
    if let Some(Select::AllProjectedAttributes) = input.select {
        if index_info.is_none() {
            return Err(DynamoDbError::ValidationException(
                "ALL_PROJECTED_ATTRIBUTES can be used only when querying an index".to_owned(),
            ));
        }
    }
    if let Some(Select::Count) = input.select {
        if input.projection_expression.is_some() {
            return Err(DynamoDbError::ValidationException(
                "Cannot specify the ProjectionExpression when Select is COUNT".to_owned(),
            ));
        }
    }

    // When Select=ALL_PROJECTED_ATTRIBUTES, capture the index info for post-read filtering.
    let index_proj = if matches!(input.select, Some(Select::AllProjectedAttributes)) {
        index_info.as_ref()
    } else {
        None
    };

    // Query storage
    let (raw_items, storage_last_key) = ctx
        .storage
        .query(
            &query_key_info,
            &key_condition,
            &maps,
            input.scan_index_forward,
            input.limit,
            input.exclusive_start_key.as_ref(),
            input.index_name.as_deref(),
        )
        .await
        .map_err(storage_err_to_dynamo)?;

    // Capacity metering: RCU based on total pre-projection size of all scanned items.
    let pre_projection_bytes: usize = raw_items.iter().map(item_size_bytes).sum();
    let strongly_consistent = input.consistent_read == Some(true);
    let rcu = capacity_helpers::read_capacity_units(pre_projection_bytes, strongly_consistent);

    // Determine which key schema to use for LastEvaluatedKey extraction.
    // For index queries, the LEK includes both the index key and the base table key.
    let lek_key_schema = combined_lek_key_schema(&key_info.key_schema, index_info.as_ref());

    // Apply FilterExpression, ProjectionExpression, and 1 MB limit
    let result = apply_post_read(
        &raw_items,
        storage_last_key,
        &filter,
        &projection,
        &maps,
        &lek_key_schema,
        input.select.as_ref(),
        index_proj,
        &key_info.key_schema,
    )?;

    let output = QueryOutput {
        items: result.items,
        count: result.count,
        scanned_count: result.scanned_count,
        last_evaluated_key: result.last_evaluated_key,
        consumed_capacity: capacity_helpers::read_capacity(
            input.return_consumed_capacity,
            &input.table_name,
            rcu,
        ),
    };

    let body = serialize_output(&output)?;
    #[allow(clippy::cast_sign_loss)] // count is non-negative
    Ok(DispatchResult {
        body,
        metrics: DispatchMetrics {
            read_capacity_units: rcu,
            returned_item_count: result.count as u64,
            returned_bytes: pre_projection_bytes as u64,
            index_name: input.index_name,
            ..Default::default()
        },
    })
}

/// Resolve a path's top-level attribute name, handling `#name` references.
/// Returns `None` if the path is empty or the name reference is unresolved.
fn resolve_path_attr_name(
    path: &[PathElement],
    names: &std::collections::HashMap<String, String>,
) -> Option<String> {
    match path.first() {
        Some(PathElement::Attribute(name)) => {
            if let Some(ref_name) = name.strip_prefix('#') {
                names.get(ref_name).cloned()
            } else {
                Some(name.clone())
            }
        }
        _ => None,
    }
}
