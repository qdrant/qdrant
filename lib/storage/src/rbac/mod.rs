mod ops_checks;

use std::borrow::Cow;
use std::collections::HashMap;

use segment::json_path::JsonPath;
use segment::types::ValueVariants;
use serde::{Deserialize, Serialize};

use crate::content_manager::errors::StorageError;

/// A structure that defines access rights.
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
#[serde(untagged)]
pub enum Access {
    /// Cluster-wide access.
    Cluster(ClusterAccessMode),
    /// Access to specific collections.
    Collection(AccessCollection),
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct AccessCollection(pub Vec<AccessCollectionRule>);

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct AccessCollectionRule {
    /// Collection names that are allowed to be accessed
    pub collections: Vec<String>,

    pub access: CollectionAccessMode,

    /// Payload constraints.
    /// An object where each key is a JSON path, and each value is JSON value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<PayloadConstraint>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Debug)]
pub enum ClusterAccessMode {
    /// Read-only access to the whole cluster.
    #[serde(rename = "r")]
    Read,

    /// Read and write access to the whole cluster.
    #[serde(rename = "rw")]
    ReadWrite,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Debug)]
pub enum CollectionAccessMode {
    /// Read-only access to a collection.
    #[serde(rename = "r")]
    Read,

    /// Read and write access to a collection, with some restrictions.
    #[serde(rename = "rw")]
    ReadWrite,

    /// Read and write access to a collection, with no restrictions.
    #[serde(rename = "m")]
    Manage,
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct PayloadConstraint(pub HashMap<JsonPath, ValueVariants>);

impl Access {
    /// Create an `Access` object with full access.
    /// The ``_reason`` parameter is not used in the code, but serves as a mandatory commentary to
    /// explain why the access is granted, e.g. ``Access::full("Internal API")`` or
    /// ``Access::full("Test")``.
    pub const fn full(_reason: &'static str) -> Self {
        Self::Cluster(ClusterAccessMode::ReadWrite)
    }

    pub const fn full_ro(_reason: &'static str) -> Self {
        Self::Cluster(ClusterAccessMode::Read)
    }

    /// Check if the user has access to the whole cluster.
    pub fn check_cluster_access(
        &self,
        min_mode: ClusterAccessMode,
    ) -> Result<CollectionMultipass, StorageError> {
        match self {
            Access::Cluster(mode) if *mode >= min_mode => Ok(CollectionMultipass),
            _ => Err(StorageError::unauthorized("Cluster access is not allowed")),
        }
    }

    /// Check if the user has access to a collection with given constraints.
    ///
    /// If ``whole_collection`` is true, then the access should not limited by a payload
    /// restriction.
    pub fn check_collection_access<'a>(
        &self,
        collection_name: &'a str,
        whole_collection: bool,
        mode: CollectionAccessMode,
    ) -> Result<CollectionPass<'a>, StorageError> {
        match self {
            Access::Cluster(ClusterAccessMode::Read) => {
                if mode != CollectionAccessMode::Read {
                    return Err(StorageError::unauthorized(
                        "Only read-only access is allowed",
                    ));
                }
            }
            Access::Cluster(ClusterAccessMode::ReadWrite) => (),
            Access::Collection(rules) => {
                let view = rules.find_view(collection_name)?;
                if whole_collection {
                    view.check_whole_access()?;
                }
                view.check_access_mode(mode)?;
            }
        }
        Ok(CollectionPass(Cow::Borrowed(collection_name)))
    }
}

impl AccessCollection {
    pub(self) fn find_view<'a>(
        &'a self,
        collection_name: &'a str,
    ) -> Result<AccessCollectionView<'a>, StorageError> {
        let rule = self
            .0
            .iter()
            .find(|collections| {
                collections
                    .collections
                    .iter()
                    .any(|name| name == collection_name)
            })
            .ok_or_else(|| {
                StorageError::unauthorized(format!("Collection {collection_name} is not allowed"))
            })?;
        Ok(AccessCollectionView {
            collection: collection_name,
            access: rule.access,
            payload: &rule.payload,
        })
    }
}

struct AccessCollectionView<'a> {
    pub collection: &'a str,
    pub access: CollectionAccessMode,
    pub payload: &'a Option<PayloadConstraint>,
}

impl<'a> AccessCollectionView<'a> {
    pub(self) fn check_whole_access(&self) -> Result<(), StorageError> {
        if self.payload.is_some() {
            return incompatible_with_payload_constraint(self.collection);
        }
        Ok(())
    }

    pub(self) fn check_access_mode(
        &self,
        min_access: CollectionAccessMode,
    ) -> Result<(), StorageError> {
        if self.access < min_access {
            return Err(StorageError::unauthorized(format!(
                "Collection {} is not allowed",
                self.collection,
            )));
        }
        Ok(())
    }
}

pub struct CollectionMultipass;

impl CollectionMultipass {
    pub fn issue_pass<'a>(&self, name: &'a str) -> CollectionPass<'a> {
        CollectionPass(Cow::Borrowed(name))
    }
}

/// A pass that allows access to a specific collection.
pub struct CollectionPass<'a>(pub(self) Cow<'a, str>);

impl<'a> CollectionPass<'a> {
    pub fn name(&'a self) -> &'a str {
        &self.0
    }

    pub fn into_static(self) -> CollectionPass<'static> {
        CollectionPass(Cow::Owned(self.0.into_owned()))
    }
}

impl std::fmt::Display for CollectionPass<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Helper function to indicate that the operation is not allowed when `payload` constraint is
/// present.
fn incompatible_with_payload_constraint<T>(collection_name: &str) -> Result<T, StorageError> {
    Err(StorageError::unauthorized(format!(
        "This operation is not allowed when \"payload\" restriction is present for collection \
         {collection_name}"
    )))
}
