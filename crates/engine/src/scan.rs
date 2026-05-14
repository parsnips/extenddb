// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `Scan` operation handler.

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::parse_projection;
use extenddb_core::types::{
    IndexType, ScanInput, ScanOutput, Select, TableKeyInfo, item_size_bytes,
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

/// Handle a `Scan` request.
///
/// Reads all items from the table (or segment), applies `FilterExpression`
/// post-read, applies `ProjectionExpression`, and enforces the 1 MB limit.
///
/// # Errors
///
/// Returns `DynamoDbError` for validation failures, missing tables, or storage errors.
#[allow(clippy::cast_possible_wrap)] // item counts won't exceed i64::MAX
pub async fn handle_scan<S: TableEngine + DataEngine>(
    body: Value,
    ctx: &OperationContext<S>,
) -> Result<DispatchResult, DynamoDbError> {
    let input: ScanInput = serde_json::from_value(body).map_err(|e| {
        DynamoDbError::SerializationException(format!(
            "Start of structure or map found where not expected: {e}"
        ))
    })?;

    // P118: Fetch key_info first so we can use table_id for index lookup.
    let key_info = ctx
        .table_key_info(&input.table_name)
        .await
        .map_err(storage_err_to_dynamo)?;

    // GSI/LSI: resolve index metadata if scanning a secondary index.
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

    // ConsistentRead is not supported on GSI scans (tenet 1: fidelity).
    if input.consistent_read == Some(true) {
        if let Some(ref idx) = index_info {
            if idx.index_type == IndexType::Gsi {
                return Err(DynamoDbError::ValidationException(
                    "Consistent reads are not supported on global secondary indexes".to_owned(),
                ));
            }
        }
    }

    // Validate Segment/TotalSegments — DynamoDB returns different messages per direction
    match (input.segment, input.total_segments) {
        (Some(_), None) => {
            return Err(DynamoDbError::ValidationException(
                "The TotalSegments parameter is required but was not present in the request when Segment parameter is present"
                    .to_owned(),
            ));
        }
        (None, Some(_)) => {
            return Err(DynamoDbError::ValidationException(
                "The Segment parameter is required but was not present in the request when TotalSegments parameter is present"
                    .to_owned(),
            ));
        }
        (Some(seg), Some(total)) => {
            if total < 1 {
                return Err(DynamoDbError::ValidationException(
                    "The parameter TotalSegments should be greater than or equal to 1".to_owned(),
                ));
            }
            if seg < 0 || seg >= total {
                return Err(DynamoDbError::ValidationException(
                    "The Segment parameter is zero-based and must be less than parameter TotalSegments"
                        .to_owned(),
                ));
            }
        }
        (None, None) => {}
    }

    // Validate Limit >= 1
    if let Some(limit) = input.limit {
        if limit < 1 {
            return Err(DynamoDbError::ValidationException(format!(
                "1 validation error detected: Value '{limit}' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"
            )));
        }
    }

    // For index scans, build a key_info that reflects the index's key schema.
    let scan_key_info = if let Some(ref idx) = index_info {
        TableKeyInfo {
            table_name: key_info.table_name.clone(),
            account_id: key_info.account_id.clone(),
            table_id: key_info.table_id.clone(),
            key_schema: idx.key_schema.clone(),
            attribute_definitions: key_info.attribute_definitions.clone(),
            has_lsi: key_info.has_lsi,
            stream_specification: None, // Scans don't capture stream records
        }
    } else {
        key_info.clone()
    };

    let maps = build_expression_maps(
        input.expression_attribute_names.as_ref(),
        input.expression_attribute_values.as_ref(),
    );

    // Parse FilterExpression
    let filter = parse_optional_filter(input.filter_expression.as_deref(), &ctx.limits)?;

    // Parse ProjectionExpression
    let projection = if let Some(ref proj_str) = input.projection_expression {
        let proj_tokens = crate::expression_helpers::tokenize_expression(proj_str, &ctx.limits)?;
        Some(parse_projection(&proj_tokens)?)
    } else {
        None
    };

    // Validate Select vs ProjectionExpression and index requirements
    if let Some(Select::AllProjectedAttributes) = input.select {
        if index_info.is_none() {
            return Err(DynamoDbError::ValidationException(
                "ALL_PROJECTED_ATTRIBUTES can be used only when scanning an index".to_owned(),
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

    let index_proj = if matches!(input.select, Some(Select::AllProjectedAttributes)) {
        index_info.as_ref()
    } else {
        None
    };

    // Scan storage
    let (raw_items, storage_last_key) = ctx
        .storage
        .scan(
            &scan_key_info,
            input.limit,
            input.exclusive_start_key.as_ref(),
            input.segment,
            input.total_segments,
            input.index_name.as_deref(),
        )
        .await
        .map_err(storage_err_to_dynamo)?;

    // Capacity metering: RCU based on total pre-projection size of all scanned items.
    let pre_projection_bytes: usize = raw_items.iter().map(item_size_bytes).sum();
    let strongly_consistent = input.consistent_read == Some(true);
    let rcu = capacity_helpers::read_capacity_units(pre_projection_bytes, strongly_consistent);

    // Determine which key schema to use for LastEvaluatedKey extraction.
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

    let output = ScanOutput {
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
