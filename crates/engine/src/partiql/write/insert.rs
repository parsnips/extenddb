// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The INSERT statement: a create-only PutItem.

use std::collections::{BTreeMap, HashMap};

use serde_json::{Map, Value, json};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::ast::InsertStatement;
use extenddb_core::types::{
    AttributeValue, Item, PutItemInput, ReturnConsumedCapacity, ReturnItemCollectionMetrics,
    ReturnValues, ReturnValuesOnConditionCheckFailure, TableKeyInfo,
};
use extenddb_core::validation::validate_index_key_types;

use super::expr_builder::ExprBuilder;
use super::{
    from_wire, index_keys, internal, key_names, reshape, resolve_av, to_json, validate_insert_keys,
};
use crate::create_table::storage_err_to_dynamo;
use crate::partiql::authorize::Gate;
use crate::partiql::{SingleRequest, common, item_depth};
use crate::{DispatchResult, OperationContext, handle_put_item};

// --- INSERT ---------------------------------------------------------------

pub(super) async fn run_insert(
    ctx: &OperationContext,
    insert: &InsertStatement,
    req: &SingleRequest,
    gate: &mut Gate<'_>,
) -> Result<DispatchResult, DynamoDbError> {
    let typed = build_insert_typed(ctx, insert, &req.parameters).await?;
    gate.pass().await?;
    // PutItem gets the typed input of the INSERT, without a JSON body to
    // serialize and deserialize again. A member that would not deserialize to
    // the same value goes in a JSON body, so that PutItem reports it as it does
    // for a body (see `direct_put_input`).
    let result = match direct_put_input(typed, req) {
        Ok(input) => crate::put_item::put_item_input(input, ctx).await,
        Err(typed) => {
            let parts = typed.into_parts()?;
            let mut body = json!({
                "TableName": parts.table,
                "Item": parts.item,
                "ConditionExpression": parts.condition,
                "ExpressionAttributeNames": Value::Object(parts.names),
            });
            if let Some(rcc) = &req.return_consumed_capacity {
                body["ReturnConsumedCapacity"] = json!(rcc);
            }
            handle_put_item(body, ctx).await
        }
    }
    .map_err(|e| {
        crate::partiql::bare_expression_error(item_depth::item_nesting_error(map_insert_error(e)))
    });
    Ok(reshape(result?))
}

/// The translated classic `PutItem` pieces for an `INSERT`: the item, the
/// create-only condition, and the names map it references. Shared by the
/// single-statement executor and the transaction executor (which wraps them in
/// a `TransactWriteItems` `Put` member).
pub(crate) struct PutParts {
    pub table: String,
    pub item: Value,
    pub condition: String,
    pub names: Map<String, Value>,
}

/// [`PutParts`] with the item still typed.
pub(crate) struct TypedPutParts {
    table: String,
    item: BTreeMap<String, AttributeValue>,
    condition: String,
    names: Map<String, Value>,
}

impl TypedPutParts {
    fn into_parts(self) -> Result<PutParts, DynamoDbError> {
        Ok(PutParts {
            table: self.table,
            item: to_json(&self.item)?,
            condition: self.condition,
            names: self.names,
        })
    }
}

/// Normalize the numbers of `value` as deserializing its JSON form does: each
/// `N`, and each member of an `NS`, at any depth. A number written as a
/// literal in the statement (`.7325e1`) reaches the item as written, and the
/// JSON path stores it normalized (`7.325`). `false` when an `NS` would lose a
/// member to a duplicate, which deserializing refuses.
fn normalize_numbers(value: &mut AttributeValue) -> bool {
    use extenddb_core::validation::number::validate_and_normalize_number;
    match value {
        AttributeValue::N(n) => {
            if let Ok(normalized) = validate_and_normalize_number(n) {
                *n = normalized;
            }
            true
        }
        AttributeValue::NS(set) => {
            let normalized: std::collections::BTreeSet<String> = set
                .iter()
                .map(|n| validate_and_normalize_number(n).unwrap_or_else(|_| n.clone()))
                .collect();
            if normalized.len() != set.len() {
                return false;
            }
            *set = normalized;
            true
        }
        AttributeValue::L(list) => list.iter_mut().all(normalize_numbers),
        AttributeValue::M(map) => map.values_mut().all(normalize_numbers),
        AttributeValue::S(_)
        | AttributeValue::B(_)
        | AttributeValue::SS(_)
        | AttributeValue::BS(_)
        | AttributeValue::Bool(_)
        | AttributeValue::Null => true,
    }
}

/// The typed `PutItem` input of `parts`, as `PutItem` would deserialize it
/// from the JSON body that `run_insert` otherwise builds. `parts` back when a
/// member would not deserialize to the same value (an unknown
/// `ReturnConsumedCapacity`, or a number set that normalizing would merge), so
/// that the JSON path handles it.
fn direct_put_input(
    mut parts: TypedPutParts,
    req: &SingleRequest,
) -> Result<PutItemInput, TypedPutParts> {
    // Normalizing is idempotent and leaves a merging set as it was, so the
    // JSON path gives the same result from an item normalized in part.
    if !parts.item.values_mut().all(normalize_numbers) {
        return Err(parts);
    }
    let return_consumed_capacity = match req.return_consumed_capacity.as_deref() {
        None | Some("NONE") => ReturnConsumedCapacity::None,
        Some("TOTAL") => ReturnConsumedCapacity::Total,
        Some("INDEXES") => ReturnConsumedCapacity::Indexes,
        Some(_) => return Err(parts),
    };
    let Some(names) = parts
        .names
        .iter()
        .map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_owned())))
        .collect::<Option<HashMap<_, _>>>()
    else {
        return Err(parts);
    };
    Ok(PutItemInput {
        table_name: parts.table,
        item: parts.item.into_iter().collect(),
        return_values: ReturnValues::default(),
        condition_expression: Some(parts.condition),
        expression_attribute_names: Some(names),
        expression_attribute_values: None,
        expected: None,
        conditional_operator: None,
        return_values_on_condition_check_failure: ReturnValuesOnConditionCheckFailure::default(),
        return_consumed_capacity,
        return_item_collection_metrics: ReturnItemCollectionMetrics::default(),
    })
}

/// Translate an `INSERT` into its `PutItem` pieces (create-only condition,
/// item, names), resolving `?` parameters. Does not dispatch.
///
/// # Errors
///
/// Returns the service's key-validation message when a key attribute is missing
/// or mistyped, or `ResourceNotFoundException` for a missing table.
pub(crate) async fn build_insert_parts(
    ctx: &OperationContext,
    insert: &InsertStatement,
    params: &[AttributeValue],
) -> Result<PutParts, DynamoDbError> {
    build_insert_typed(ctx, insert, params).await?.into_parts()
}

/// [`build_insert_parts`] with the item still typed.
async fn build_insert_typed(
    ctx: &OperationContext,
    insert: &InsertStatement,
    params: &[AttributeValue],
) -> Result<TypedPutParts, DynamoDbError> {
    let key_info = ctx
        .table_key_info(&insert.table)
        .await
        .map_err(storage_err_to_dynamo)?;

    // Build the item from the VALUE map, resolving `?` parameters to their typed
    // AttributeValues. The values are the ones authorization read the key
    // from (see `common::insert_values`).
    let mut item: BTreeMap<String, AttributeValue> = BTreeMap::new();
    for (name, operand) in common::insert_values(insert) {
        item.insert(name.to_owned(), resolve_av(operand, params)?);
    }

    // PartiQL names the missing or mistyped key attribute with its own message,
    // which differs from the classic PutItem key-validation message, so the
    // check runs here before the put.
    validate_insert_keys(&item, &key_info)?;

    // Create-only: a duplicate full key must fail. `attribute_not_exists` on the
    // partition key is the create-only condition; the target slot is addressed
    // by its full key, so an item with the same partition key but a different
    // sort key does not trip it.
    let (pk_name, _sk_name) = key_names(&key_info)?;
    let mut builder = ExprBuilder::default();
    let pk_ref = builder.name(pk_name);
    let condition = format!("attribute_not_exists({pk_ref})");

    Ok(TypedPutParts {
        table: insert.table.clone(),
        item,
        condition,
        names: builder.names,
    })
}

/// The refusal of the `PutItem` request of `parts` on the table `key_info`
/// when its item gives a secondary index key a value of the wrong type.
///
/// # Errors
///
/// The error a classic request gets for a value of the item that does not
/// read back (see `from_wire`), which the statement checks refuse first, or
/// a fault when the translated item is not a map.
pub(crate) fn insert_refusal(
    parts: &PutParts,
    key_info: &TableKeyInfo,
) -> Result<Option<DynamoDbError>, DynamoDbError> {
    let Value::Object(item) = &parts.item else {
        return Err(internal());
    };
    let item: Item = from_wire(item)?;
    Ok(validate_index_key_types(
        &item,
        &index_keys(key_info),
        &key_info.attribute_definitions,
    )
    .err())
}

/// Map a create-only condition failure to the PartiQL duplicate-key error.
///
/// The only condition the INSERT path sets is `attribute_not_exists(pk)`, so a
/// `ConditionalCheckFailedException` from it means the full key already exists.
fn map_insert_error(e: DynamoDbError) -> DynamoDbError {
    match e {
        DynamoDbError::ConditionalCheckFailedException(..) => {
            DynamoDbError::DuplicateItemException(
                "Duplicate primary key exists in table".to_owned(),
            )
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use extenddb_core::types::AttributeValue;

    use super::normalize_numbers;

    /// The value `PutItem` deserializes from the JSON form of `value`: the
    /// value the JSON path of an INSERT stores.
    fn through_json(value: &AttributeValue) -> Result<AttributeValue, serde_json::Error> {
        serde_json::from_value(serde_json::to_value(value)?)
    }

    fn n(s: &str) -> AttributeValue {
        AttributeValue::N(s.to_owned())
    }

    fn ns(members: &[&str]) -> AttributeValue {
        AttributeValue::NS(
            members
                .iter()
                .map(|&m| m.to_owned())
                .collect::<BTreeSet<_>>(),
        )
    }

    #[test]
    fn normalizing_gives_the_value_of_the_json_path() {
        let values = [
            n(".7325e1"),
            n("-.73e1"),
            n("007"),
            n("1E2"),
            n("-0.50"),
            n("0.000"),
            n("not a number"),
            ns(&["1.50", "2", ".3e1"]),
            AttributeValue::L(vec![
                n("1.0"),
                AttributeValue::S("1.0".to_owned()),
                ns(&["10e-1"]),
            ]),
            AttributeValue::M(BTreeMap::from([
                ("a".to_owned(), n("+5")),
                (
                    "b".to_owned(),
                    AttributeValue::M(BTreeMap::from([("c".to_owned(), n("2.500"))])),
                ),
            ])),
            AttributeValue::S("text".to_owned()),
            AttributeValue::SS(BTreeSet::from(["x".to_owned()])),
            AttributeValue::B(vec![1, 2]),
            AttributeValue::Bool(true),
            AttributeValue::Null,
        ];
        for value in values {
            let mut direct = value.clone();
            assert!(normalize_numbers(&mut direct), "{value:?}");
            let json = through_json(&value).expect("the JSON path accepts the value");
            assert_eq!(direct, json, "{value:?}");
        }
    }

    #[test]
    fn a_number_set_that_normalizing_merges_takes_the_json_path() {
        // 1 and 1.0 are one number, so the JSON path refuses the set as a
        // duplicate. The direct path reports false and leaves the set as it
        // was, so that the INSERT takes the JSON path.
        let value = ns(&["1", "1.0"]);
        let mut direct = value.clone();
        assert!(!normalize_numbers(&mut direct));
        assert_eq!(direct, value);
        assert!(through_json(&value).is_err());

        let mut nested = AttributeValue::L(vec![n("2.0"), value]);
        assert!(!normalize_numbers(&mut nested));
    }
}
