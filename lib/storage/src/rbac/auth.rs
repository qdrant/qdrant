use chrono::Utc;

use super::{Access, AccessRequirements, AuthType, CollectionMultipass, CollectionPass};
use crate::audit::{AuditEvent, audit_log, is_audit_enabled};
use crate::content_manager::errors::StorageError;

/// Per-request authentication context.
///
/// Wraps the [`Access`] RBAC object together with request metadata (remote IP,
/// JWT `subject`, authentication method).  All access-check methods
/// additionally emit structured audit log entries when the global audit logger
/// is enabled.
#[derive(Clone, Debug)]
pub struct Auth {
    access: Access,
    subject: Option<String>,
    remote: Option<String>,
    auth_type: AuthType,
}

impl Auth {
    pub const fn new(
        access: Access,
        subject: Option<String>,
        remote: Option<String>,
        auth_type: AuthType,
    ) -> Self {
        Self {
            access,
            subject,
            remote,
            auth_type,
        }
    }

    pub const fn new_internal(access: Access) -> Self {
        Self {
            access,
            subject: None,
            remote: None,
            auth_type: AuthType::Internal,
        }
    }

    /// Borrow the inner [`Access`] object (e.g. to pass into library code that
    /// still expects `&Access`).
    ///
    /// Warn: this method is not recommended for general use, as it does not emit audit log entries.
    /// Consider using the `access()` method instead, which wraps access checks with audit logging.
    pub fn unlogged_access(&self) -> &Access {
        &self.access
    }

    /// Borrow the inner [`Access`] object (e.g. to pass into library code that
    /// still expects `&Access`).
    pub fn access(&self, method: &str) -> &Access {
        // Gives direct access to the inner `Access` object,
        // but also emits an audit log entry with "ok" status.
        self.emit_audit(method, None, &Ok(()));
        &self.access
    }

    // ------------------------------------------------------------------
    // Wrapped access-check methods with audit logging
    // ------------------------------------------------------------------

    /// Check global access and emit an audit log entry.
    pub fn check_global_access(
        &self,
        requirements: AccessRequirements,
        method: &str,
    ) -> Result<CollectionMultipass, StorageError> {
        let result = self.access.check_global_access(requirements);
        self.emit_audit(method, None, &result);
        result
    }

    /// Check collection-scoped access and emit an audit log entry.
    pub fn check_collection_access<'a>(
        &self,
        collection_name: &'a str,
        requirements: AccessRequirements,
        method: &str,
    ) -> Result<CollectionPass<'a>, StorageError> {
        let result = self
            .access
            .check_collection_access(collection_name, requirements);
        self.emit_audit(method, Some(collection_name), &result);
        result
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    pub(crate) fn emit_audit<T>(
        &self,
        method: &str,
        collection: Option<&str>,
        result: &Result<T, StorageError>,
    ) {
        if !is_audit_enabled() || self.auth_type == AuthType::Internal {
            return;
        }

        let (status, error) = match result {
            Ok(_) => ("ok", None),
            Err(e) => ("denied", Some(e.to_string())),
        };

        audit_log(AuditEvent {
            timestamp: Utc::now(),
            method: method.to_string(),
            auth_type: self.auth_type.clone(),
            subject: self.subject.clone(),
            remote: self.remote.clone(),
            collection: collection.map(String::from),
            result: status,
            error,
        });
    }
}
