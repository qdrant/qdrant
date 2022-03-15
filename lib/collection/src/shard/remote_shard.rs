use crate::shard::{PeerId, ShardId};
use crate::CollectionId;

/// RemoteShard
///
/// Remote Shard is a representation of a shard that is located on a remote peer.
/// Currently a placeholder implementation for later work.
#[allow(dead_code)]
pub struct RemoteShard {
    id: ShardId,
    collection_id: CollectionId,
    peer_id: PeerId,
}
