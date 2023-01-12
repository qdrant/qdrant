use collection::collection::Collection;

/// Handlers for migration data from one collection into another within single cluster


/// Spawns a task which will retrieve data from appropriate local shards of the `source` collection
/// into target collection.
pub async fn migrate(source: &Collection, target: &Collection) {
    let collection_state = source.state().await;


    for (shard_id, shard_info) in

}