use collection::operations::types::UpdateResult;
use collection::operations::CollectionUpdateOperations;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;

pub async fn do_update_points(
    toc: &TableOfContent,
    collection_name: &str,
    operation: CollectionUpdateOperations,
    wait: bool,
) -> Result<UpdateResult, StorageError> {
    toc.update(collection_name, operation, wait).await
}
