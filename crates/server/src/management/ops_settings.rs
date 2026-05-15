// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Settings validation and write operations.
//!
//! Validation logic lives here in the server layer. The actual database
//! write is delegated to the `SettingsStore` trait implementation.

use extenddb_storage::management_store::{OpError, OpResult};

/// Validator function for a setting value.
pub type Validator = fn(&str) -> Result<(), &'static str>;

/// Known writable setting keys and their validators.
pub const KNOWN_KEYS: &[(&str, Validator)] = &[
    ("allow_credential_import", validate_bool),
    ("control_plane_delay_seconds", validate_delay_seconds),
    ("gsi_propagation_delay_ms", validate_gsi_delay_ms),
    ("log_level", validate_log_level),
    ("sqlx_log_level", validate_log_level),
    ("throttling_enabled", validate_bool),
];

/// Read-only keys that cannot be changed via the settings API.
pub const READONLY_KEYS: &[&str] = &[
    "catalog_version",
    "data_database_connection_string",
    "data_database_name",
];

fn validate_log_level(value: &str) -> Result<(), &'static str> {
    match value {
        "trace" | "debug" | "info" | "warn" | "error" => Ok(()),
        _ => Err("must be one of: trace, debug, info, warn, error"),
    }
}

fn validate_bool(value: &str) -> Result<(), &'static str> {
    match value {
        "true" | "false" => Ok(()),
        _ => Err("must be 'true' or 'false'"),
    }
}

fn validate_delay_seconds(value: &str) -> Result<(), &'static str> {
    match value.parse::<f64>() {
        Ok(v) if (0.0..=300.0).contains(&v) => Ok(()),
        Ok(_) => Err("must be between 0 and 300"),
        Err(_) => Err("must be a non-negative number"),
    }
}

fn validate_gsi_delay_ms(value: &str) -> Result<(), &'static str> {
    match value.parse::<u32>() {
        Ok(0..=10000) => Ok(()),
        Ok(_) => Err("must be between 0 and 10000"),
        Err(_) => Err("must be a non-negative integer"),
    }
}

/// Set a runtime setting with validation.
///
/// Validates the key and value, then delegates the write to the
/// `SettingsStore` implementation. Validation stays in the server layer;
/// the storage layer trusts validated input.
///
/// # Errors
///
/// Returns `OpError::Validation` if the key is read-only, unknown, or the value
/// fails validation. Returns `OpError::Internal` on database errors.
pub async fn set_setting(
    store: &dyn extenddb_storage::management_store::SettingsStore,
    key: &str,
    value: &str,
) -> OpResult<()> {
    if READONLY_KEYS.contains(&key) {
        return Err(OpError::Validation(format!("Setting '{key}' is read-only")));
    }

    let known = KNOWN_KEYS.iter().find(|(k, _)| *k == key);
    if let Some((_, validator)) = known {
        validator(value).map_err(|reason| {
            OpError::Validation(format!("Invalid value for '{key}': {reason}"))
        })?;
    } else {
        return Err(OpError::Validation(format!(
            "Unknown setting '{key}'. Known writable keys: {}",
            KNOWN_KEYS
                .iter()
                .map(|(k, _)| *k)
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }

    store.set_setting(key, value).await?;

    tracing::warn!(
        target: "extenddb::audit::settings",
        "settings-set: key={key}, value={value}",
    );
    Ok(())
}
