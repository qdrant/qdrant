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

pub struct ExistingCollections {
    inner: HashSet<String>,
}

#[derive(Serialize, Deserialize, Validate, PartialEq, Clone, Debug)]
#[validate(context = ExistingCollections, mutable)]
pub struct CollectionAccess {
    /// Collection names that are allowed to be accessed
    #[validate(custom(function = "validate_unique_collections", use_context))]
    pub collection: String,

    pub access: CollectionAccessMode,

    /// Payload constraints.
    /// An object where each key is a JSON path, and each value is JSON value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<PayloadConstraint>,
}

impl CollectionAccess {
    fn view(&self) -> CollectionAccessView {
        CollectionAccessView {
            collection: &self.collection,
            access: self.access,
            payload: &self.payload,
        }
    }
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

    /// Points read and write - access to update and modify points in the collection,
    /// but not snapshots or payload indexes.
    #[serde(rename = "prw")]
    PointsReadWrite,
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
        requirements: AccessRequirements,
    ) -> Result<CollectionMultipass, StorageError> {
        match self {
            Access::Global(mode) => mode.meets_requirements(requirements)?,
            Access::Collection(_) => {
                return Err(StorageError::forbidden("Global access is required"))
            }
        }
        Ok(CollectionMultipass)
    }

    /// Check if the user has access to a collection with given requirements.
    pub fn check_collection_access<'a>(
        &self,
        collection_name: &'a str,
        requirements: AccessRequirements,
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
        Ok(access.view())
    }

    /// Lists the collections which fulfill the requirements.
    pub fn meeting_requirements(&self, requirements: AccessRequirements) -> Vec<&String> {
        self.0
            .iter()
            .filter(|access| access.view().meets_requirements(requirements).is_ok())
            .map(|access| &access.collection)
            .collect()
    }
}

#[derive(Debug)]
struct CollectionAccessView<'a> {
    pub collection: &'a str,
    pub access: CollectionAccessMode,
    pub payload: &'a Option<PayloadConstraint>,
}

impl CollectionAccessView<'_> {
    pub(self) fn check_whole_access(&self) -> Result<(), StorageError> {
        if self.payload.is_some() {
            return incompatible_with_payload_constraint(self.collection);
        }
        Ok(())
    }

    fn meets_requirements(&self, requirements: AccessRequirements) -> Result<(), StorageError> {
        let AccessRequirements {
            write,
            manage,
            whole,
            extras,
        } = requirements;

        if extras {
            match self.access {
                CollectionAccessMode::Read => {}      // Ok
                CollectionAccessMode::ReadWrite => {} // Ok
                CollectionAccessMode::PointsReadWrite => {
                    return Err(StorageError::forbidden(format!(
                        "Only points access is allowed for collection {}",
                        self.collection,
                    )))
                }
            }
        }

        if write {
            match self.access {
                CollectionAccessMode::Read => {
                    return Err(StorageError::forbidden(format!(
                        "Write access to collection {} is required",
                        self.collection,
                    )))
                }
                CollectionAccessMode::ReadWrite => (),
                CollectionAccessMode::PointsReadWrite => {
                    // Extras are checked above.
                }
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
#[derive(Debug)]
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
pub struct AccessRequirements {
    /// Write access is required.
    pub write: bool,
    /// Manage access is required, implies write access.
    pub manage: bool,
    /// If true, the access should be not limited by a payload restrictions.
    pub whole: bool,
    /// Require access to collection extras, like snapshots, payload indexes, cluster info.
    pub extras: bool,
}

impl AccessRequirements {
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

    pub fn extras(&self) -> Self {
        Self {
            extras: true,
            ..*self
        }
    }
}

impl GlobalAccessMode {
    fn meets_requirements(&self, requirements: AccessRequirements) -> Result<(), StorageError> {
        let AccessRequirements {
            write,
            manage,
            whole: _,
            extras: _,
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
                let mut used_collections = ExistingCollections {
                    inner: HashSet::new(),
                };
                list.0
                    .iter()
                    .map(|x| {
                        ValidationErrors::merge(
                            Ok(()),
                            "access",
                            x.validate_with_args(&mut used_collections),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        }
    }
}

fn validate_unique_collections(
    collection: &str,
    used_collections: &mut ExistingCollections,
) -> Result<(), ValidationError> {
    let unique = used_collections.inner.insert(collection.to_owned());
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

#[cfg(test)]
struct AccessCollectionBuilder(pub Vec<CollectionAccess>);

#[cfg(test)]
impl AccessCollectionBuilder {
    pub(self) fn new() -> Self {
        Self(Vec::new())
    }

    pub(self) fn add(mut self, name: &str, write: bool, whole: bool) -> Self {
        self.0.push(CollectionAccess {
            collection: name.to_string(),
            access: if write {
                CollectionAccessMode::ReadWrite
            } else {
                CollectionAccessMode::Read
            },
            payload: (!whole).then(|| PayloadConstraint::new_test(name)),
        });
        self
    }
}

#[cfg(test)]
impl From<AccessCollectionBuilder> for Access {
    fn from(builder: AccessCollectionBuilder) -> Self {
        Access::Collection(CollectionAccessList(builder.0))
    }
}

#[cfg(test)]
impl PayloadConstraint {
    /// Create a dummy value for testing.
    pub fn new_test(name: &str) -> Self {
        PayloadConstraint(HashMap::from([(
            format!("f_{name}").parse().unwrap(),
            ValueVariants::String(format!("v_{name}")),
        )]))
    }
}
