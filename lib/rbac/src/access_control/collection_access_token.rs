use crate::access_control::error::AccessDeniedError;
use crate::access_control::AccessLevel;
use crate::jwt::PayloadClaim;

pub(crate) struct CollectionAccess {
    pub(crate) collection_name: String,
    pub(crate) access_level: AccessLevel,
    pub(crate) payload: Option<PayloadClaim>,
}

#[must_use]
pub struct CollectionsAccessToken {
    pub(crate) collections: Vec<CollectionAccess>,
}

impl CollectionsAccessToken {
    pub(crate) fn new(collections: Vec<CollectionAccess>) -> Self {
        Self { collections }
    }

    fn validate_all_collections_found(
        collection_names: &[&str],
        collections_access: &[CollectionAccess],
    ) -> Result<(), AccessDeniedError> {
        for collection_name in collection_names {
            if !collections_access
                .iter()
                .any(|collection_access| collection_access.collection_name == *collection_name)
            {
                return Err(AccessDeniedError::new(&format!(
                    "Access to collection {} is denied",
                    collection_name
                )));
            }
        }

        Ok(())
    }

    pub(crate) fn validate_read_collections(
        self,
        collection_names: &[&str],
    ) -> Result<Self, AccessDeniedError> {
        let collections: Vec<_> = self
            .collections
            .into_iter()
            .filter(|collection_access| {
                collection_names.contains(&collection_access.collection_name.as_str())
                    && collection_access.access_level.is_read_allowed()
            })
            .collect();

        // Check all collections are found
        Self::validate_all_collections_found(collection_names, &collections)?;

        Ok(Self { collections })
    }

    pub(crate) fn validate_write_collections(
        self,
        collection_names: &[&str],
    ) -> Result<Self, AccessDeniedError> {
        let collections: Vec<_> = self
            .collections
            .into_iter()
            .filter(|collection_access| {
                collection_names.contains(&collection_access.collection_name.as_str())
                    && collection_access.access_level.is_write_allowed()
            })
            .collect();

        // Check all collections are found
        Self::validate_all_collections_found(collection_names, &collections)?;

        Ok(Self { collections })
    }

    pub(crate) fn validate_manage_collections(
        self,
        collection_names: &[&str],
    ) -> Result<Self, AccessDeniedError> {
        let collections: Vec<_> = self
            .collections
            .into_iter()
            .filter(|collection_access| {
                collection_names.contains(&collection_access.collection_name.as_str())
                    && collection_access.access_level.is_manage_allowed()
            })
            .collect();

        // Check all collections are found
        Self::validate_all_collections_found(collection_names, &collections)?;

        Ok(Self { collections })
    }
}
