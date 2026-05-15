// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Expression attribute name and value resolution.
//!
//! Resolves `#name` references and `:value` placeholders against the maps
//! provided in `ExpressionAttributeNames` and `ExpressionAttributeValues`.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};

use crate::error::DynamoDbError;
use crate::types::AttributeValue;

use super::ast::PathElement;

/// Resolved expression attribute names and values.
///
/// Constructed from the request's `ExpressionAttributeNames` and
/// `ExpressionAttributeValues` maps. Used by the parser and evaluator.
///
/// Numeric placeholder values are pre-parsed into `BigDecimal` at construction
/// time so that filter expressions comparing a placeholder against many items
/// parse the placeholder only once per request.
#[derive(Debug, Clone, Default)]
pub struct ExpressionMaps {
    pub names: HashMap<String, String>,
    pub values: HashMap<String, AttributeValue>,
    /// Pre-parsed `BigDecimal` values for numeric placeholders.
    /// Populated eagerly by `new()` and refreshed by `pre_parse_numerics()`.
    parsed_numerics: HashMap<String, bigdecimal::BigDecimal>,
}

impl ExpressionMaps {
    /// Create a new `ExpressionMaps` with the given names and values.
    ///
    /// Pre-parses all numeric placeholder values into `BigDecimal`.
    #[must_use]
    pub fn new(names: HashMap<String, String>, values: HashMap<String, AttributeValue>) -> Self {
        let mut maps = Self {
            names,
            values,
            parsed_numerics: HashMap::new(),
        };
        maps.pre_parse_numerics();
        maps
    }

    /// Resolve a `#name` reference to an attribute name.
    ///
    /// # Errors
    ///
    /// Returns `ValidationException` if the name is not in the map.
    pub fn resolve_name(&self, name_ref: &str) -> Result<&str, DynamoDbError> {
        self.names.get(name_ref).map(String::as_str).ok_or_else(|| {
            DynamoDbError::ValidationException(format!(
                "An expression attribute name used in the document path is not defined; attribute name: #{name_ref}"
            ))
        })
    }

    /// Resolve a `:value` placeholder to an `AttributeValue`.
    ///
    /// # Errors
    ///
    /// Returns `ValidationException` if the placeholder is not in the map.
    pub fn resolve_value(&self, placeholder: &str) -> Result<&AttributeValue, DynamoDbError> {
        self.values.get(placeholder).ok_or_else(|| {
            DynamoDbError::ValidationException(format!(
                "An expression attribute value used in expression is not defined; attribute value: :{placeholder}"
            ))
        })
    }

    /// Like [`resolve_value`](Self::resolve_value) but prefixes the error with the expression type.
    pub fn resolve_value_for(
        &self,
        placeholder: &str,
        expr_type: &str,
    ) -> Result<&AttributeValue, DynamoDbError> {
        self.values.get(placeholder).ok_or_else(|| {
            DynamoDbError::ValidationException(format!(
                "Invalid {expr_type}: An expression attribute value used in expression is not defined; attribute value: :{placeholder}"
            ))
        })
    }

    /// Pre-parse all numeric placeholder values into `BigDecimal`.
    ///
    /// Call once per request before evaluating filter expressions against
    /// multiple items. Invalid numeric strings are silently skipped — they
    /// will produce `false` comparisons at evaluation time, matching the
    /// existing behavior.
    pub fn pre_parse_numerics(&mut self) {
        for (key, value) in &self.values {
            if let AttributeValue::N(n) = value {
                if let Ok(d) = n.parse::<bigdecimal::BigDecimal>() {
                    self.parsed_numerics.insert(key.clone(), d);
                }
            }
        }
    }

    /// Look up a pre-parsed `BigDecimal` for a numeric placeholder.
    ///
    /// Returns `None` if the placeholder was not numeric or failed to parse.
    #[must_use]
    pub fn get_parsed_numeric(&self, placeholder: &str) -> Option<&bigdecimal::BigDecimal> {
        self.parsed_numerics.get(placeholder)
    }
}

/// Resolve a `#name` reference or return the bare name.
///
/// Shared by the condition evaluator and update evaluator for resolving
/// attribute names in document paths.
///
/// # Errors
///
/// Returns `ValidationException` if a `#name` reference is not in the map.
pub fn resolve_name_ref<'a>(
    name: &'a str,
    maps: &'a ExpressionMaps,
) -> Result<Cow<'a, str>, DynamoDbError> {
    if let Some(ref_name) = name.strip_prefix('#') {
        Ok(Cow::Owned(maps.resolve_name(ref_name)?.to_owned()))
    } else {
        Ok(Cow::Borrowed(name))
    }
}

/// Walk a document path to find the target attribute value.
///
/// Resolves `#name` references along the way. Returns `None` for missing paths.
/// Shared by the condition evaluator and update evaluator.
///
/// # Errors
///
/// Returns `ValidationException` for unresolvable `#name` references.
pub fn resolve_path<'a>(
    elements: &[PathElement],
    item: &'a BTreeMap<String, AttributeValue>,
    maps: &ExpressionMaps,
) -> Result<Option<&'a AttributeValue>, DynamoDbError> {
    if elements.is_empty() {
        return Ok(None);
    }

    let first_name = resolve_element_name(&elements[0], maps)?;
    let Some(mut current) = item.get(first_name.as_ref()) else {
        return Ok(None);
    };

    for element in &elements[1..] {
        match element {
            PathElement::Attribute(name) => {
                let resolved = resolve_name_ref(name, maps)?;
                match current {
                    AttributeValue::M(map) => {
                        current = match map.get(resolved.as_ref()) {
                            Some(v) => v,
                            None => return Ok(None),
                        };
                    }
                    _ => return Ok(None),
                }
            }
            PathElement::Index(idx) => match current {
                AttributeValue::L(list) => {
                    current = match list.get(*idx) {
                        Some(v) => v,
                        None => return Ok(None),
                    };
                }
                _ => return Ok(None),
            },
        }
    }

    Ok(Some(current))
}

/// Resolve the attribute name from a path element, handling `#name` references.
///
/// Returns an error if the path starts with an index.
///
/// # Errors
///
/// Returns `ValidationException` for unresolvable references or index-start paths.
pub fn resolve_element_name<'a>(
    element: &'a PathElement,
    maps: &'a ExpressionMaps,
) -> Result<Cow<'a, str>, DynamoDbError> {
    match element {
        PathElement::Attribute(name) => resolve_name_ref(name, maps),
        PathElement::Index(_) => Err(DynamoDbError::ValidationException(
            "Invalid expression: path cannot start with an index".to_owned(),
        )),
    }
}

/// Validate that all provided ExpressionAttributeNames/Values were used.
///
/// Collects `#name` and `:value` references from the given expressions,
/// then checks that every key in `names`/`values` maps was referenced.
pub fn validate_unused_attributes(
    names: &HashMap<String, String>,
    values: &HashMap<String, AttributeValue>,
    exprs: &[&super::ast::Expr],
    update_actions: &[&super::ast::UpdateAction],
    key_condition_names: &std::collections::HashSet<String>,
    key_condition_values: &std::collections::HashSet<String>,
) -> Result<(), DynamoDbError> {
    let mut used_names = key_condition_names.clone();
    let mut used_values = key_condition_values.clone();

    for expr in exprs {
        collect_expr_refs(expr, &mut used_names, &mut used_values);
    }
    for action in update_actions {
        collect_action_refs(action, &mut used_names, &mut used_values);
    }

    for key in names.keys() {
        if !used_names.contains(key.strip_prefix('#').unwrap_or(key)) {
            let display_key = if key.starts_with('#') {
                key.clone()
            } else {
                format!("#{key}")
            };
            return Err(DynamoDbError::ValidationException(format!(
                "Value provided in ExpressionAttributeNames unused in expressions: keys: {{{display_key}}}"
            )));
        }
    }
    for key in values.keys() {
        if !used_values.contains(key.strip_prefix(':').unwrap_or(key)) {
            let display_key = if key.starts_with(':') {
                key.clone()
            } else {
                format!(":{key}")
            };
            return Err(DynamoDbError::ValidationException(format!(
                "Value provided in ExpressionAttributeValues unused in expressions: keys: {{{display_key}}}"
            )));
        }
    }
    Ok(())
}

fn collect_expr_refs(
    expr: &super::ast::Expr,
    names: &mut std::collections::HashSet<String>,
    values: &mut std::collections::HashSet<String>,
) {
    use super::ast::Expr;
    match expr {
        Expr::Path(elements) => collect_path_refs(elements, names),
        Expr::Placeholder(name) => {
            values.insert(name.clone());
        }
        Expr::Compare { left, right, .. } | Expr::Arithmetic { left, right, .. } => {
            collect_expr_refs(left, names, values);
            collect_expr_refs(right, names, values);
        }
        Expr::And(l, r) | Expr::Or(l, r) => {
            collect_expr_refs(l, names, values);
            collect_expr_refs(r, names, values);
        }
        Expr::Not(inner) => collect_expr_refs(inner, names, values),
        Expr::Function { args, .. } => {
            for arg in args {
                collect_expr_refs(arg, names, values);
            }
        }
        Expr::Between { operand, low, high } => {
            collect_expr_refs(operand, names, values);
            collect_expr_refs(low, names, values);
            collect_expr_refs(high, names, values);
        }
        Expr::In { operand, list } => {
            collect_expr_refs(operand, names, values);
            for item in list {
                collect_expr_refs(item, names, values);
            }
        }
    }
}

fn collect_action_refs(
    action: &super::ast::UpdateAction,
    names: &mut std::collections::HashSet<String>,
    values: &mut std::collections::HashSet<String>,
) {
    use super::ast::UpdateAction;
    match action {
        UpdateAction::Set { path, value }
        | UpdateAction::Add { path, value }
        | UpdateAction::Delete { path, value } => {
            collect_path_refs(path, names);
            collect_expr_refs(value, names, values);
        }
        UpdateAction::Remove { path } => collect_path_refs(path, names),
    }
}

fn collect_path_refs(elements: &[PathElement], names: &mut std::collections::HashSet<String>) {
    for el in elements {
        if let PathElement::Attribute(name) = el {
            if let Some(ref_name) = name.strip_prefix('#') {
                names.insert(ref_name.to_owned());
            }
        }
    }
}

/// Collect `#name` and `:value` references from a `KeyCondition`.
///
/// Returns `(used_names, used_values)` sets suitable for passing to
/// `validate_unused_attributes`.
pub fn collect_key_condition_refs(
    kc: &super::key_condition::KeyCondition,
) -> (
    std::collections::HashSet<String>,
    std::collections::HashSet<String>,
) {
    let mut names = std::collections::HashSet::new();
    let mut values = std::collections::HashSet::new();

    collect_path_refs(&kc.pk_path, &mut names);
    collect_expr_refs(&kc.pk_value, &mut names, &mut values);

    if let Some(ref sk) = kc.sk_condition {
        match sk {
            super::key_condition::SortKeyCondition::Compare { path, value, .. } => {
                collect_path_refs(path, &mut names);
                collect_expr_refs(value, &mut names, &mut values);
            }
            super::key_condition::SortKeyCondition::Between { path, low, high } => {
                collect_path_refs(path, &mut names);
                collect_expr_refs(low, &mut names, &mut values);
                collect_expr_refs(high, &mut names, &mut values);
            }
            super::key_condition::SortKeyCondition::BeginsWith { path, prefix } => {
                collect_path_refs(path, &mut names);
                collect_expr_refs(prefix, &mut names, &mut values);
            }
        }
    }

    for (path, expr) in &kc.extra_pk_conditions {
        collect_path_refs(path, &mut names);
        collect_expr_refs(expr, &mut names, &mut values);
    }
    for (path, expr) in &kc.extra_sk_conditions {
        collect_path_refs(path, &mut names);
        collect_expr_refs(expr, &mut names, &mut values);
    }

    (names, values)
}
