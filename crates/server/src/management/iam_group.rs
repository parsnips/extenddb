// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! IAM group management endpoints (admin only).

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use super::ManagementState;
use super::auth::authenticate_admin;
use super::ops::{OpError, op_err_to_response};

#[derive(Deserialize)]
pub struct CreateGroupRequest {
    group_name: String,
}

#[derive(Serialize)]
struct GroupEntry {
    account_id: String,
    group_name: String,
    group_arn: String,
    created_at: String,
}

#[derive(Deserialize)]
pub struct AddMemberRequest {
    user_name: String,
}

/// `POST /management/accounts/{id}/groups` — create an IAM group.
pub async fn create_group(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
    axum::Json(body): axum::Json<CreateGroupRequest>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    if !super::is_valid_iam_name(&body.group_name) {
        return op_err_to_response(OpError::Validation(
            "group_name must be 1-128 characters: alphanumeric, hyphens, underscores, dots, plus, equals, at".to_owned(),
        ));
    }

    match state
        .catalog_store
        .create_group(&account_id, &body.group_name)
        .await
    {
        Ok(()) => (StatusCode::CREATED, "IAM group created").into_response(),
        Err(e) => op_err_to_response(OpError::from_storage(e)),
    }
}

/// `GET /management/accounts/{id}/groups` — list IAM groups in an account.
pub async fn list_groups(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    match state.catalog_store.list_groups(&account_id).await {
        Ok(rows) => {
            let entries: Vec<GroupEntry> = rows
                .into_iter()
                .map(
                    |(account_id, group_name, group_arn, created_at)| GroupEntry {
                        account_id,
                        group_name,
                        group_arn,
                        created_at: created_at
                            .format(&time::format_description::well_known::Rfc3339)
                            .unwrap_or_default(),
                    },
                )
                .collect();
            axum::Json(entries).into_response()
        }
        Err(e) => {
            tracing::error!("Management API: list IAM groups failed: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// `DELETE /management/accounts/{id}/groups/{name}` — delete an IAM group.
pub async fn delete_group(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, group_name)): Path<(String, String)>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    match state
        .catalog_store
        .delete_group(&account_id, &group_name)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => op_err_to_response(OpError::from_storage(e)),
    }
}

/// `POST /management/accounts/{id}/groups/{name}/members` — add a user to a group.
pub async fn add_member(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, group_name)): Path<(String, String)>,
    axum::Json(body): axum::Json<AddMemberRequest>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    match state
        .catalog_store
        .add_group_member(&account_id, &group_name, &body.user_name)
        .await
    {
        Ok(()) => (StatusCode::CREATED, "Member added").into_response(),
        Err(e) => op_err_to_response(OpError::from_storage(e)),
    }
}

/// `DELETE /management/accounts/{id}/groups/{name}/members/{user}` — remove a user from a group.
pub async fn remove_member(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, group_name, user_name)): Path<(String, String, String)>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    match state
        .catalog_store
        .remove_group_member(&account_id, &group_name, &user_name)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => op_err_to_response(OpError::from_storage(e)),
    }
}
