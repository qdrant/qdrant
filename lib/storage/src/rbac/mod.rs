use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use segment::json_path::JsonPath;
use segment::types::ValueVariants;
use serde::{Deserialize, Serialize};
use validator::{Validate, ValidateArgs, ValidationError, ValidationErrors};

use crate::content_manager::errors::StorageError;

mod ops_checks;

/// A structure that defines access rights.
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
#[serde(untagged)]
pub enum Access {
    /// Global access.
    Global(GlobalAccessMode),
    /// Access to specific collections.
    Collection(CollectionAccessList),
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct CollectionAccessList(pub Vec<CollectionAccess>);

#[derive(Serialize, Deserialize, Validate, PartialEq, Clone, Debug)]
pub struct CollectionAccess {
    /// Collection names that are allowed to be accessed
    #[validate(custom(
        function = "validate_unique_collections",
        arg = "&'v_a mut HashSet<String>"
    ))]
    pub collection: String,

    pub access: CollectionAccessMode,

    /// Payload constraints.
    /// An object where each key is a JSON path, and each value is JSON value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<PayloadConstraint>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Copy, Clone, Debug)]
pub enum GlobalAccessMode {
    /// Read-only access
    #[serde(rename = "r")]
    Read,

    /// Read and write access
    #[serde(rename = "m")]
    Manage,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Copy, Clone, Debug)]
pub enum CollectionAccessMode {
    /// Read-only access to a collection.
    #[serde(rename = "r")]
    Read,

    /// Read and write access to a collection, with some restrictions.
    #[serde(rename = "rw")]
    ReadWrite,
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct PayloadConstraint(pub HashMap<JsonPath, ValueVariants>);

impl Access {
    /// Create an `Access` object with full access.
    /// The ``_reason`` parameter is not used in the code, but serves as a mandatory commentary to
    /// explain why the access is granted, e.g. ``Access::full("Internal API")`` or
    /// ``Access::full("Test")``.
    pub const fn full(_reason: &'static str) -> Self {
        Self::Global(GlobalAccessMode::Manage)
    }

    pub const fn full_ro(_reason: &'static str) -> Self {
        Self::Global(GlobalAccessMode::Read)
    }

    /// Check if the user has global access.
    pub fn check_global_access(
        &self,
        requirements: AccessRequrements,
    ) -> Result<CollectionMultipass, StorageError> {
        match self {
            Access::Global(mode) => mode.meets_requirements(requirements)?,
            _ => return Err(StorageError::forbidden("Global access is required")),
        }
        Ok(CollectionMultipass)
    }

    /// Check if the user has access to a collection with given requirements.
    pub fn check_collection_access<'a>(
        &self,
        collection_name: &'a str,
        requirements: AccessRequrements,
    ) -> Result<CollectionPass<'a>, StorageError> {
        match self {
            Access::Global(mode) => mode.meets_requirements(requirements)?,
            Access::Collection(list) => list
                .find_view(collection_name)?
                .meets_requirements(requirements)?,
        }
        Ok(CollectionPass(Cow::Borrowed(collection_name)))
    }
}

impl CollectionAccessList {
    pub(self) fn find_view<'a>(
        &'a self,
        collection_name: &'a str,
    ) -> Result<CollectionAccessView<'a>, StorageError> {
        let access = self
            .0
            .iter()
            .find(|collections| collections.collection == collection_name)
            .ok_or_else(|| {
                StorageError::forbidden(format!(
                    "Access to collection {collection_name} is required"
                ))
            })?;
        Ok(CollectionAccessView {
            collection: collection_name,
            access: access.access,
            payload: &access.payload,
        })
    }
}

struct CollectionAccessView<'a> {
    pub collection: &'a str,
    pub access: CollectionAccessMode,
    pub payload: &'a Option<PayloadConstraint>,
}

impl<'a> CollectionAccessView<'a> {
    pub(self) fn check_whole_access(&self) -> Result<(), StorageError> {
        if self.payload.is_some() {
            return incompatible_with_payload_constraint(self.collection);
        }
        Ok(())
    }

    fn meets_requirements(&self, requirements: AccessRequrements) -> Result<(), StorageError> {
        let AccessRequrements {
            write,
            manage,
            whole,
        } = requirements;
        if write {
            match self.access {
                CollectionAccessMode::Read => {
                    return Err(StorageError::forbidden(format!(
                        "Write access to collection {} is required",
                        self.collection,
                    )))
                }
                CollectionAccessMode::ReadWrite => (),
            }
        }
        if manage {
            // Don't specify collection name since the manage access could be enabled globally, and
            // not per collection.
            return Err(StorageError::forbidden(
                "Manage access for this operation is required",
            ));
        }
        if whole && self.payload.is_some() {
            return incompatible_with_payload_constraint(self.collection);
        }
        Ok(())
    }
}

/// Creates [CollectionPass] objects for all collections
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

#[derive(Default, Debug, Copy, Clone)]
pub struct AccessRequrements {
    /// Write access is required.
    pub write: bool,
    /// Manage access is required, implies write access.
    pub manage: bool,
    /// If true, the access should be not limited by a payload restrictions.
    pub whole: bool,
}

impl AccessRequrements {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn write(&self) -> Self {
        Self {
            write: true,
            ..*self
        }
    }

    pub fn manage(&self) -> Self {
        Self {
            manage: true,
            ..*self
        }
    }

    pub fn whole(&self) -> Self {
        Self {
            whole: true,
            ..*self
        }
    }
}

impl GlobalAccessMode {
    fn meets_requirements(&self, requirements: AccessRequrements) -> Result<(), StorageError> {
        let AccessRequrements {
            write,
            manage,
            whole: _,
        } = requirements;
        if write || manage {
            match self {
                GlobalAccessMode::Read => {
                    return Err(StorageError::forbidden("Global manage access is required"))
                }
                GlobalAccessMode::Manage => (),
            }
        }
        Ok(())
    }
}

/// Helper function to indicate that the operation is not allowed when `payload` constraint is
/// present.
fn incompatible_with_payload_constraint<T>(collection_name: &str) -> Result<T, StorageError> {
    Err(StorageError::forbidden(format!(
        "This operation is not allowed when \"payload\" restriction is present for collection \
         {collection_name}"
    )))
}

impl Access {
    /// Return a list of validation errors in a format suitable for [ValidationErrors::merge_all].
    pub fn validate(&self) -> Vec<Result<(), ValidationErrors>> {
        match self {
            Access::Global(_) => Vec::new(),
            Access::Collection(list) => {
                let mut used_collections = HashSet::new();
                list.0
                    .iter()
                    .map(|x| {
                        ValidationErrors::merge(
                            Ok(()),
                            "access",
                            x.validate_args(&mut used_collections),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        }
    }
}

fn validate_unique_collections(
    collection: &str,
    used_collections: &mut HashSet<String>,
) -> Result<(), ValidationError> {
    let unique = used_collections.insert(collection.to_owned());
    if unique {
        Ok(())
    } else {
        Err(ValidationError {
            code: Cow::from("unique"),
            message: Some(Cow::from("Collection name should be unique")),
            params: HashMap::from([(Cow::from("collection"), collection.to_owned().into())]),
        })
    }
}
