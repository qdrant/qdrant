use super::access::Access;
use super::access_level::AccessLevel;
use super::collection_access::{CollectionAccess, CollectionsAccessToken};
use super::error::AccessDeniedError;

pub enum AccessToken {
    Global(AccessLevel),
    Collections(CollectionsAccessToken),
}

impl TryFrom<Access> for AccessToken {
    type Error = AccessDeniedError;

    fn try_from(access: Access) -> Result<Self, Self::Error> {
        // TODO: this conversion should be refactored as structure of Access will be finalized.

        let allow_write = true;

        match (access.collections, access.payload) {
            (Some(collections), Some(payload)) => {
                let collection_access: Vec<_> = collections
                    .into_iter()
                    .map(|collection_name| CollectionAccess {
                        collection_name,
                        access_level: if allow_write {
                            AccessLevel::ReadWrite
                        } else {
                            AccessLevel::ReadOnly
                        },
                        payload: Some(payload.clone()),
                    })
                    .collect();
                Ok(AccessToken::Collections(CollectionsAccessToken::new(
                    collection_access,
                )))
            }
            (Some(collections), None) => {
                let collection_access: Vec<_> = collections
                    .into_iter()
                    .map(|collection_name| CollectionAccess {
                        collection_name,
                        access_level: if allow_write {
                            AccessLevel::ReadWrite
                        } else {
                            AccessLevel::ReadOnly
                        },
                        payload: None,
                    })
                    .collect();
                Ok(AccessToken::Collections(CollectionsAccessToken::new(
                    collection_access,
                )))
            }
            (None, Some(_payload)) => Err(AccessDeniedError::new(
                "Payload claim requires collection to be set.",
            )),
            (None, None) => {
                if allow_write {
                    Ok(AccessToken::Global(AccessLevel::Manage))
                } else {
                    Ok(AccessToken::Global(AccessLevel::ReadOnly))
                }
            }
        }
    }
}

impl AccessToken {
    /// Create a new access token to access given collections.
    pub fn into_collections_token_with_manage_access(
        self,
        collection_names: &[&str],
    ) -> Result<CollectionsAccessToken, AccessDeniedError> {
        match self {
            AccessToken::Global(AccessLevel::Manage) => {
                let collection_access: Vec<_> = collection_names
                    .iter()
                    .map(|collection_name| CollectionAccess {
                        collection_name: collection_name.to_string(),
                        access_level: AccessLevel::Manage,
                        payload: None,
                    })
                    .collect();
                Ok(CollectionsAccessToken::new(collection_access))
            }
            AccessToken::Global(AccessLevel::ReadOnly | AccessLevel::ReadWrite) => Err(
                AccessDeniedError::new("Not allowed to have manage access for collections"),
            ),
            AccessToken::Collections(collections_token) => {
                collections_token.validate_manage_collections(collection_names)
            }
        }
    }

    /// Create a new access token to access given collections with read-only access.
    pub fn into_collections_token_with_read_access(
        self,
        collection_names: &[&str],
    ) -> Result<CollectionsAccessToken, AccessDeniedError> {
        match self {
            AccessToken::Global(
                AccessLevel::Manage | AccessLevel::ReadWrite | AccessLevel::ReadOnly,
            ) => {
                let collection_access: Vec<_> = collection_names
                    .iter()
                    .map(|collection_name| CollectionAccess {
                        collection_name: collection_name.to_string(),
                        access_level: AccessLevel::ReadOnly,
                        payload: None,
                    })
                    .collect();
                Ok(CollectionsAccessToken::new(collection_access))
            }
            AccessToken::Collections(collections_token) => {
                collections_token.validate_read_collections(collection_names)
            }
        }
    }

    /// Create a new access token to access given collections with write access.
    pub fn into_collections_token_with_write_access(
        self,
        collection_names: &[&str],
    ) -> Result<CollectionsAccessToken, AccessDeniedError> {
        match self {
            AccessToken::Global(AccessLevel::Manage | AccessLevel::ReadWrite) => {
                let collection_access: Vec<_> = collection_names
                    .iter()
                    .map(|collection_name| CollectionAccess {
                        collection_name: collection_name.to_string(),
                        access_level: AccessLevel::ReadWrite,
                        payload: None,
                    })
                    .collect();
                Ok(CollectionsAccessToken::new(collection_access))
            }
            AccessToken::Global(AccessLevel::ReadOnly) => Err(AccessDeniedError::new(
                "Not allowed to have write access for collections",
            )),
            AccessToken::Collections(collections_token) => {
                collections_token.validate_write_collections(collection_names)
            }
        }
    }
}
