use api::grpc::models::{CollectionDescription, CollectionsResponse};
use collection::operations::types::CollectionInfo;
use collection::shard::ShardId;
use itertools::Itertools;
use std::collections::HashMap;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;

#[allow(unreachable_code)]
pub async fn do_get_collection(
    toc: &TableOfContent,
    name: &str,
    shard_selection: Option<ShardId>,
) -> Result<CollectionInfo, StorageError> {
    let collection = toc.get_collection(name).await?;
    #[cfg(feature = "consensus")]
    {
        return Ok(collection
            .info(shard_selection, &toc.peer_address_by_id().unwrap())
            .await?);
    }
    Ok(collection.info(shard_selection, &HashMap::new()).await?)
}

pub async fn do_list_collections(toc: &TableOfContent) -> CollectionsResponse {
    let collections = toc
        .all_collections()
        .await
        .into_iter()
        .map(|name| CollectionDescription { name })
        .collect_vec();

    CollectionsResponse { collections }
}
