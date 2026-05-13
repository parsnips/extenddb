// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::ListTablesInput;
use extenddb_core::validation::validate_table_name_chars;
use extenddb_storage::TableEngine;

use crate::OperationContext;
use crate::create_table::storage_err_to_dynamo;
use crate::serialize_output;

pub async fn handle_list_tables<S: TableEngine>(
    body: Value,
    ctx: &OperationContext<S>,
) -> Result<Value, DynamoDbError> {
    let input: ListTablesInput = serde_json::from_value(body).map_err(crate::deserialize_error)?;

    // Defense-in-depth: validate ExclusiveStartTableName characters before reaching storage.
    // Real DynamoDB does not enforce min-length on pagination tokens, so we only check
    // for safe characters ([a-zA-Z0-9_.-]) — not the full table name rules.
    if let Some(ref start) = input.exclusive_start_table_name {
        validate_table_name_chars(start)?;
    }

    // Validate limit before clamping — use original value in error message
    let raw_limit = input.limit.unwrap_or(ctx.limits.list_tables_max_per_page);
    if raw_limit < 1 {
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value '{raw_limit}' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"
        )));
    }
    if raw_limit > ctx.limits.list_tables_max_per_page {
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value '{raw_limit}' at 'limit' failed to satisfy constraint: Member must have value less than or equal to {}",
            ctx.limits.list_tables_max_per_page
        )));
    }

    let validated_input = ListTablesInput {
        limit: Some(raw_limit),
        exclusive_start_table_name: input.exclusive_start_table_name,
    };

    let output = ctx
        .storage
        .list_tables(&ctx.account_id, validated_input)
        .await
        .map_err(storage_err_to_dynamo)?;

    serialize_output(&output)
}
