use std::borrow::Cow;
use std::collections::HashMap;

use segment::json_path::JsonPath;
use segment::types::ValueVariants;
use serde::{Deserialize, Serialize};

use crate::content_manager::claims::{
    check_collection_name, incompatible_with_collection_claim, incompatible_with_payload_claim,
};
use crate::content_manager::errors::StorageError;

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct Access {
    /// Collection names that are allowed to be accessed
    pub collections: Option<Vec<String>>,

    /// Payload constraints.
    /// An object where each key is a JSON path, and each value is JSON value.
    pub payload: Option<PayloadClaim>,
}

impl Access {
    // TODO: add an explanation comment in all places where this is used
    pub fn full() -> Self {
        Self {
            collections: None,
            payload: None,
        }
    }

    /// Check if a access object is allowed to manage everything.
    pub fn check_manage_rights(&self) -> Result<CollectionMultipass, StorageError> {
        let Access {
            collections,
            payload,
        } = self;
        if collections.is_some() {
            return incompatible_with_collection_claim();
        }
        if payload.is_some() {
            return incompatible_with_payload_claim();
        }
        Ok(CollectionMultipass)
    }

    /// Check if a claim object has access to the whole collection.
    pub fn check_whole_collection_rights<'a>(
        &self,
        collection_name: &'a str,
    ) -> Result<CollectionPass<'a>, StorageError> {
        let Access {
            collections,
            payload,
        } = self;
        if let Some(collections) = collections {
            check_collection_name(Some(collections), collection_name)?;
        }
        if payload.is_some() {
            return incompatible_with_payload_claim();
        }
        Ok(CollectionPass(Cow::Borrowed(collection_name)))
    }

    /// Check if a claim object has read access to a collection.
    pub fn check_partial_collection_rights<'a>(
        &self,
        collection_name: &'a str,
    ) -> Result<CollectionPass<'a>, StorageError> {
        let Access {
            collections,
            payload: _,
        } = self;
        if let Some(collections) = collections {
            check_collection_name(Some(collections), collection_name)?;
        }
        Ok(CollectionPass(Cow::Borrowed(collection_name)))
    }
}

pub struct CollectionMultipass;

impl CollectionMultipass {
    pub fn issue_pass<'a>(&self, name: &'a str) -> CollectionPass<'a> {
        CollectionPass(Cow::Borrowed(name))
    }
}

/// A pass that allows access to a specific collection.
pub struct CollectionPass<'a>(Cow<'a, str>);

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

pub type PayloadClaim = HashMap<JsonPath, ValueVariants>;
