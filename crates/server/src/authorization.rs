// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Authorization layer for DynamoDB requests.
//!
//! After authentication resolves an `AuthIdentity`, this module fetches the
//! applicable IAM policies, permissions boundary, and session policy via the
//! [`AuthorizationStore`] trait, builds a `RequestContext`, and evaluates
//! authorization using the policy engine from `extenddb-auth`.

use std::collections::HashMap;

use extenddb_auth::AuthIdentity;
use extenddb_auth::policy::context::{RequestContext, RequestParams};
use extenddb_auth::policy::document::PolicyDocument;
use extenddb_auth::policy::evaluator::{AuthzDecision, evaluate_policies};
use extenddb_core::error::DynamoDbError;
use extenddb_storage::authorization_store::AuthorizationStore;

/// Evaluate whether the authenticated identity is authorized for the given
/// DynamoDB operation on the given resource.
///
/// For `AuthIdentity::User` and `AuthIdentity::RoleSession`, the full IAM
/// evaluation algorithm runs: explicit deny → permissions boundary → session
/// policy → identity allow → implicit deny.
pub async fn check_authorization(
    store: &dyn AuthorizationStore,
    identity: &AuthIdentity,
    operation: &str,
    resource_arn: &str,
    is_scan: bool,
    params: RequestParams,
) -> Result<(), DynamoDbError> {
    match identity {
        AuthIdentity::User {
            account_id,
            user_name,
        } => {
            check_user_authorization(
                store,
                account_id,
                user_name,
                operation,
                resource_arn,
                is_scan,
                params,
            )
            .await
        }
        AuthIdentity::RoleSession {
            account_id,
            role_name,
            session_name,
        } => {
            check_role_authorization(
                store,
                account_id,
                role_name,
                session_name,
                operation,
                resource_arn,
                is_scan,
                params,
            )
            .await
        }
    }
}

async fn check_user_authorization(
    store: &dyn AuthorizationStore,
    account_id: &str,
    user_name: &str,
    operation: &str,
    resource_arn: &str,
    is_scan: bool,
    params: RequestParams,
) -> Result<(), DynamoDbError> {
    let action = format!("dynamodb:{operation}");

    // Fetch all 5 authz inputs concurrently — they are independent queries.
    let (user_policies, group_policies, boundary, principal_tags, resource_tags) = tokio::try_join!(
        fetch_policies(store.fetch_user_policies(account_id, user_name)),
        fetch_policies(store.fetch_user_group_policies(account_id, user_name)),
        fetch_boundary(store.fetch_user_boundary(account_id, user_name)),
        fetch_tags(store.fetch_user_tags(account_id, user_name)),
        fetch_resource_tags(store, resource_arn),
    )?;

    // Combine identity policies.
    let mut identity_policies = user_policies;
    identity_policies.extend(group_policies);

    // Build request context.
    let context = RequestContext::build(principal_tags, resource_tags, is_scan, params);

    let decision = evaluate_policies(
        &identity_policies,
        boundary.as_ref(),
        None,
        &action,
        resource_arn,
        &context,
    );

    if decision == AuthzDecision::Allow {
        Ok(())
    } else {
        tracing::warn!(
            principal = format!("arn:aws:iam::{account_id}:user/{user_name}"),
            action = action,
            resource = resource_arn,
            "Authorization denied"
        );
        Err(DynamoDbError::AccessDeniedException(format!(
            "User: arn:aws:iam::{account_id}:user/{user_name} is not authorized \
             to perform: {action} on resource: {resource_arn}"
        )))
    }
}

#[allow(clippy::too_many_arguments)]
async fn check_role_authorization(
    store: &dyn AuthorizationStore,
    account_id: &str,
    role_name: &str,
    session_name: &str,
    operation: &str,
    resource_arn: &str,
    is_scan: bool,
    params: RequestParams,
) -> Result<(), DynamoDbError> {
    let action = format!("dynamodb:{operation}");

    // Fetch all 4 authz inputs concurrently — they are independent queries.
    let (identity_policies, boundary, (session_policy, principal_tags), resource_tags) = tokio::try_join!(
        fetch_policies(store.fetch_role_policies(account_id, role_name)),
        fetch_boundary(store.fetch_role_boundary(account_id, role_name)),
        fetch_session_data_and_tags(store, account_id, role_name, session_name),
        fetch_resource_tags(store, resource_arn),
    )?;

    // Build request context.
    let context = RequestContext::build(principal_tags, resource_tags, is_scan, params);

    let decision = evaluate_policies(
        &identity_policies,
        boundary.as_ref(),
        session_policy.as_ref(),
        &action,
        resource_arn,
        &context,
    );

    if decision == AuthzDecision::Allow {
        Ok(())
    } else {
        tracing::warn!(
            principal =
                format!("arn:aws:iam::{account_id}:assumed-role/{role_name}/{session_name}"),
            action = action,
            resource = resource_arn,
            "Authorization denied"
        );
        Err(DynamoDbError::AccessDeniedException(format!(
            "User: arn:aws:iam::{account_id}:assumed-role/{role_name}/{session_name} \
             is not authorized to perform: {action} on resource: {resource_arn}"
        )))
    }
}

// ---------------------------------------------------------------------------
// Helpers — convert store results to authorization types
// ---------------------------------------------------------------------------

/// Parse policy JSON strings into `PolicyDocument`s. Fail closed on parse errors.
async fn fetch_policies(
    fut: impl std::future::Future<
        Output = Result<Vec<String>, extenddb_storage::management_store::OpError>,
    >,
) -> Result<Vec<PolicyDocument>, DynamoDbError> {
    let jsons = fut.await.map_err(|e| {
        tracing::error!("Authorization: fetch policies failed: {e:?}");
        DynamoDbError::InternalServerError("Internal error during authorization".to_owned())
    })?;

    let mut docs = Vec::with_capacity(jsons.len());
    for json_str in &jsons {
        match PolicyDocument::from_json(json_str) {
            Ok(doc) => docs.push(doc),
            Err(e) => {
                // Fail closed: an unparseable stored policy denies access rather
                // than being silently skipped.
                tracing::error!("Authorization: unparseable policy: {e}");
                return Err(DynamoDbError::AccessDeniedException(
                    "Not authorized to perform this action (policy evaluation error)".to_owned(),
                ));
            }
        }
    }
    Ok(docs)
}

/// Parse a boundary policy JSON string into a `PolicyDocument`. Fail closed on parse errors.
async fn fetch_boundary(
    fut: impl std::future::Future<
        Output = Result<Option<String>, extenddb_storage::management_store::OpError>,
    >,
) -> Result<Option<PolicyDocument>, DynamoDbError> {
    let json = fut.await.map_err(|e| {
        tracing::error!("Authorization: fetch boundary failed: {e:?}");
        DynamoDbError::InternalServerError("Internal error during authorization".to_owned())
    })?;

    match json {
        Some(json_str) => match PolicyDocument::from_json(&json_str) {
            Ok(doc) => Ok(Some(doc)),
            Err(e) => {
                tracing::error!("Authorization: unparseable permissions boundary: {e}");
                Err(DynamoDbError::AccessDeniedException(
                    "Not authorized to perform this action (policy evaluation error)".to_owned(),
                ))
            }
        },
        None => Ok(None),
    }
}

/// Convert tag tuples to a `HashMap`.
async fn fetch_tags(
    fut: impl std::future::Future<
        Output = Result<Vec<(String, String)>, extenddb_storage::management_store::OpError>,
    >,
) -> Result<HashMap<String, String>, DynamoDbError> {
    let tags = fut.await.map_err(|e| {
        tracing::error!("Authorization: fetch tags failed: {e:?}");
        DynamoDbError::InternalServerError("Internal error during authorization".to_owned())
    })?;
    Ok(tags.into_iter().collect())
}

/// Fetch resource tags, returning empty map for wildcard ARNs.
async fn fetch_resource_tags(
    store: &dyn AuthorizationStore,
    resource_arn: &str,
) -> Result<HashMap<String, String>, DynamoDbError> {
    // Wildcard ARNs (e.g. table/*) have no specific resource to tag.
    if resource_arn.ends_with("/*") {
        return Ok(HashMap::new());
    }
    fetch_tags(store.fetch_resource_tags(resource_arn)).await
}

/// Fetch session data and merge role tags with session tags (session wins on conflict).
async fn fetch_session_data_and_tags(
    store: &dyn AuthorizationStore,
    account_id: &str,
    role_name: &str,
    session_name: &str,
) -> Result<(Option<PolicyDocument>, HashMap<String, String>), DynamoDbError> {
    // Fetch role tags and session data concurrently (independent queries).
    let (role_tags, session_data) = tokio::try_join!(
        async {
            store
                .fetch_role_tags(account_id, role_name)
                .await
                .map_err(|e| {
                    tracing::error!("Authorization: fetch role tags failed: {e:?}");
                    DynamoDbError::InternalServerError(
                        "Internal error during authorization".to_owned(),
                    )
                })
        },
        async {
            store
                .fetch_session_data(account_id, role_name, session_name)
                .await
                .map_err(|e| {
                    tracing::error!("Authorization: fetch session data failed: {e:?}");
                    DynamoDbError::InternalServerError(
                        "Internal error during authorization".to_owned(),
                    )
                })
        },
    )?;
    let mut tags: HashMap<String, String> = role_tags.into_iter().collect();

    let mut session_policy = None;
    if let Some(data) = session_data {
        // Parse session policy.
        if let Some(json_str) = data.session_policy {
            match PolicyDocument::from_json(&json_str) {
                Ok(doc) => session_policy = Some(doc),
                Err(e) => {
                    tracing::error!("Authorization: unparseable session policy: {e}");
                    return Err(DynamoDbError::AccessDeniedException(
                        "Not authorized to perform this action (policy evaluation error)"
                            .to_owned(),
                    ));
                }
            }
        }
        // Merge session tags (session wins on conflict).
        for (k, v) in data.session_tags {
            tags.insert(k, v);
        }
    }

    Ok((session_policy, tags))
}
