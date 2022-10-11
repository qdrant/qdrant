


pub fn deactivate_replica(
    collection_name: CollectionId,
    shard_id: u32,
    peer_id: PeerId,
) -> Self {
    ConsensusOperations::CollectionMeta(
        CollectionMetaOperations::SetShardReplicaState(SetShardReplicaState {
            collection_name,
            shard_id,
            peer_id,
            active: false,
        })
        .into(),
    )
}