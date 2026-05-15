// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! IAM policy management endpoints (admin only).

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use serde_json::Value;

use super::ManagementState;
use super::auth::authenticate_admin;
use super::is_valid_iam_name;
use super::ops::{OpError, op_err_to_response};

#[derive(Serialize)]
struct PolicyEntry {
    policy_name: String,
    policy_document: Value,
    created_at: String,
}

pub async fn put_user_policy(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, user_name, policy_name)): Path<(String, String, String)>,
    axum::Json(document): axum::Json<Value>,
) -> Response {
    put_policy(
        &state,
        &headers,
        &account_id,
        "user",
        &user_name,
        &policy_name,
        &document,
    )
    .await
}

pub async fn list_user_policies(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, user_name)): Path<(String, String)>,
) -> Response {
    list_policies(&state, &headers, &account_id, "user", &user_name).await
}

pub async fn delete_user_policy(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, user_name, policy_name)): Path<(String, String, String)>,
) -> Response {
    delete_policy(
        &state,
        &headers,
        &account_id,
        "user",
        &user_name,
        &policy_name,
    )
    .await
}

pub async fn put_group_policy(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, group_name, policy_name)): Path<(String, String, String)>,
    axum::Json(document): axum::Json<Value>,
) -> Response {
    put_policy(
        &state,
        &headers,
        &account_id,
        "group",
        &group_name,
        &policy_name,
        &document,
    )
    .await
}

pub async fn list_group_policies(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, group_name)): Path<(String, String)>,
) -> Response {
    list_policies(&state, &headers, &account_id, "group", &group_name).await
}

pub async fn delete_group_policy(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, group_name, policy_name)): Path<(String, String, String)>,
) -> Response {
    delete_policy(
        &state,
        &headers,
        &account_id,
        "group",
        &group_name,
        &policy_name,
    )
    .await
}

pub async fn put_role_policy(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, role_name, policy_name)): Path<(String, String, String)>,
    axum::Json(document): axum::Json<Value>,
) -> Response {
    put_policy(
        &state,
        &headers,
        &account_id,
        "role",
        &role_name,
        &policy_name,
        &document,
    )
    .await
}

pub async fn list_role_policies(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, role_name)): Path<(String, String)>,
) -> Response {
    list_policies(&state, &headers, &account_id, "role", &role_name).await
}

pub async fn delete_role_policy(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, role_name, policy_name)): Path<(String, String, String)>,
) -> Response {
    delete_policy(
        &state,
        &headers,
        &account_id,
        "role",
        &role_name,
        &policy_name,
    )
    .await
}

// ---------------------------------------------------------------------------
// Shared implementation
// ---------------------------------------------------------------------------

async fn put_policy(
    state: &ManagementState,
    headers: &HeaderMap,
    account_id: &str,
    principal_type: &str,
    principal_name: &str,
    policy_name: &str,
    document: &Value,
) -> Response {
    if let Err(e) =
        authenticate_admin(headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    if !is_valid_iam_name(policy_name) {
        return op_err_to_response(OpError::Validation(
            "policy_name must be 1-128 characters: alphanumeric, hyphens, underscores, dots, plus, equals, at".to_owned(),
        ));
    }

    if document.get("Version").is_none() || document.get("Statement").is_none() {
        return op_err_to_response(OpError::Validation(
            "Policy document must contain Version and Statement".to_owned(),
        ));
    }

    // Strict parse-on-write: reject documents the policy evaluator cannot parse.
    {
        let json_str = document.to_string();
        if let Err(e) = extenddb_auth::policy::document::PolicyDocument::from_json(&json_str) {
            return (
                StatusCode::BAD_REQUEST,
                format!("Invalid policy document: {e}"),
            )
                .into_response();
        }
    }

    match state
        .catalog_store
        .put_policy(
            account_id,
            principal_type,
            principal_name,
            policy_name,
            document,
        )
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => op_err_to_response(OpError::from_storage(e)),
    }
}

async fn list_policies(
    state: &ManagementState,
    headers: &HeaderMap,
    account_id: &str,
    principal_type: &str,
    principal_name: &str,
) -> Response {
    if let Err(e) =
        authenticate_admin(headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    match state
        .catalog_store
        .list_policies(account_id, principal_type, principal_name)
        .await
    {
        Ok(rows) => {
            let entries: Vec<PolicyEntry> = rows
                .into_iter()
                .map(|(policy_name, policy_document, created_at)| PolicyEntry {
                    policy_name,
                    policy_document,
                    created_at: created_at
                        .format(&time::format_description::well_known::Rfc3339)
                        .unwrap_or_default(),
                })
                .collect();
            axum::Json(entries).into_response()
        }
        Err(e) => {
            tracing::error!("Management API: list {principal_type} policies failed: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn delete_policy(
    state: &ManagementState,
    headers: &HeaderMap,
    account_id: &str,
    principal_type: &str,
    principal_name: &str,
    policy_name: &str,
) -> Response {
    if let Err(e) =
        authenticate_admin(headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    match state
        .catalog_store
        .delete_policy(account_id, principal_type, principal_name, policy_name)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => op_err_to_response(OpError::from_storage(e)),
    }
}
