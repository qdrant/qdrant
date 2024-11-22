use api::rest::{PointInsertOperations, UpdateVectors};
use segment::types::{Filter, StrictModeConfig};

use super::{check_limit_opt, StrictModeVerification};
use crate::collection::Collection;
use crate::operations::payload_ops::{DeletePayload, SetPayload};
use crate::operations::point_ops::PointsSelector;
use crate::operations::types::CollectionError;
use crate::operations::vector_ops::DeleteVectors;

impl StrictModeVerification for PointsSelector {
    fn indexed_filter_write(&self) -> Option<&Filter> {
        match self {
            PointsSelector::FilterSelector(filter) => Some(&filter.filter),
            PointsSelector::PointIdsSelector(_) => None,
        }
    }

    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}

impl StrictModeVerification for DeleteVectors {
    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&Filter> {
        None
    }

    fn indexed_filter_write(&self) -> Option<&Filter> {
        self.filter.as_ref()
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}

impl StrictModeVerification for SetPayload {
    async fn check_custom(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> Result<(), CollectionError> {
        check_collection_size_limit(collection, strict_mode_config).await?;
        Ok(())
    }

    fn indexed_filter_write(&self) -> Option<&Filter> {
        self.filter.as_ref()
    }

    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}

impl StrictModeVerification for DeletePayload {
    fn indexed_filter_write(&self) -> Option<&Filter> {
        self.filter.as_ref()
    }

    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}

impl StrictModeVerification for PointInsertOperations {
    async fn check_custom(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> Result<(), CollectionError> {
        let len = match self {
            PointInsertOperations::PointsBatch(batch) => batch.batch.ids.len(),
            PointInsertOperations::PointsList(list) => list.points.len(),
        };
        check_limit_opt(
            Some(len),
            strict_mode_config.upsert_max_batchsize,
            "upsert limit",
        )?;

        check_collection_size_limit(collection, strict_mode_config).await?;

        Ok(())
    }

    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&Filter> {
        None
    }

    fn indexed_filter_write(&self) -> Option<&Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}

impl StrictModeVerification for UpdateVectors {
    async fn check_custom(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> Result<(), CollectionError> {
        check_collection_size_limit(collection, strict_mode_config).await?;
        Ok(())
    }

    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&Filter> {
        None
    }

    fn indexed_filter_write(&self) -> Option<&Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}

async fn check_collection_size_limit(
    collection: &Collection,
    strict_mode_config: &StrictModeConfig,
) -> Result<(), CollectionError> {
    if let Some(max_collection_size) = strict_mode_config.max_collection_size {
        let collection_size = collection.estimated_vector_storage_size_in_bytes().await;
        println!("collection: {collection_size}, max: {max_collection_size}");
        if collection_size >= max_collection_size {
            let size_in_mb = max_collection_size as f32 / (1024.0 * 1024.0);
            return Err(CollectionError::bad_request(format!(
                "Max collection size of {size_in_mb}MB reached!",
            )));
        }
    }

    Ok(())
}
