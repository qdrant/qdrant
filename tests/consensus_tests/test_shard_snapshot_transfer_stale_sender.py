import pathlib

from .fixtures import create_collection, upsert_random_points
from .utils import *  # includes `time` module, `requests`, `processes`, etc.

# Recreates the sender side of a shard snapshot transfer driven by a stale sender, with real
# processes. A sender that restarted replayed a `Start` consensus had already aborted, and its
# driver asked the receiver to recover a shard snapshot while another transfer, from the
# registered source, was populating that very replica. The receiver cleared its shard before
# downloading, the download from the restarted sender failed, and the replica was left as a
# dummy that the legitimate transfer then marked `Active`.
#
# The receiver must refuse requests from a peer without a registered transfer into the shard,
# and the legitimate transfer must complete untouched.

N_PEERS = 3
COLLECTION_NAME = "test_collection"
SHARD_ID = 0
N_POINTS = 20000


def _count_points(peer_url):
    r = requests.post(
        f"{peer_url}/collections/{COLLECTION_NAME}/points/count",
        json={"exact": True},
    )
    assert_http_ok(r)
    return r.json()["result"]["count"]


STAGING_START_DELAY_ENV = "QDRANT_STAGING_SHARD_TRANSFER_START_DELAY_SEC"


def _local_shard(peer_url):
    """This peer's local replica of the shard as reported by the cluster info, `None` without one."""
    return next(
        (
            shard
            for shard in get_collection_cluster_info(peer_url, COLLECTION_NAME)["local_shards"]
            if shard["shard_id"] == SHARD_ID
        ),
        None,
    )


# The sender side of the same incident, with real processes. A staging delay in the sender's
# `Start` apply lets the test kill the sender while the entry is committed but not yet applied.
# On restart the entry is replayed before the peer rejoins consensus, which spawns the driver of a
# transfer the rest of the cluster has aborted and replaced meanwhile. That driver must not touch
# the receiver, whose replica is being repopulated from the other replica, and the cluster must
# converge with every point in place.
def test_restarted_sender_replaying_aborted_transfer_start_leaves_receiver_intact(
    tmp_path: pathlib.Path,
):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, 26500)
    p2p_ports = [proc.p2p_port for proc in processes]

    def kill_peer(index):
        proc = next(proc for proc in processes if proc.p2p_port == p2p_ports[index])
        proc.kill()
        processes.remove(proc)

    def start_killed_peer(index, log_file, extra_env=None):
        uri = start_peer(
            peer_dirs[index],
            log_file,
            bootstrap_uri,
            port=p2p_ports[index],
            extra_env=extra_env,
        )
        assert uri == peer_api_uris[index]
        return uri

    create_collection(peer_api_uris[0], shard_number=1, replication_factor=2)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris
    )
    upsert_random_points(peer_api_uris[0], N_POINTS, batch_size=500)

    # Two peers hold a replica: one is the stale sender, the other the source the receiver is
    # recovered from later. The sender restarts twice, so the bootstrap peer does not take that role.
    peer_ids = [get_cluster_info(uri)["peer_id"] for uri in peer_api_uris]
    holders = [i for i, uri in enumerate(peer_api_uris) if _local_shard(uri)]
    sender, source = max(holders), min(holders)
    receiver = next(i for i in range(N_PEERS) if i not in holders)
    source_uri, receiver_uri = peer_api_uris[source], peer_api_uris[receiver]

    # Let the sender pause while applying a transfer `Start`, so it can be killed with the entry
    # committed but not yet applied
    kill_peer(sender)
    wait_for_peer_online(
        start_killed_peer(
            sender, "peer_sender_delayed.log", extra_env={STAGING_START_DELAY_ENV: "60"}
        )
    )
    wait_for_all_replicas_active(source_uri, COLLECTION_NAME)

    r = requests.post(
        f"{source_uri}/collections/{COLLECTION_NAME}/cluster",
        json={
            "replicate_shard": {
                "shard_id": SHARD_ID,
                "from_peer_id": peer_ids[sender],
                "to_peer_id": peer_ids[receiver],
                "method": "snapshot",
            }
        },
    )
    assert_http_ok(r)

    # The other peers apply the `Start` right away. The sender pauses inside applying it, with
    # the entry committed but still pending. Kill it there.
    wait_for(
        lambda: bool(
            get_collection_cluster_info(source_uri, COLLECTION_NAME).get("shard_transfers")
        )
    )
    start_commit = get_cluster_info(source_uri)["raft_info"]["commit"]

    def sender_paused_in_apply():
        raft_info = get_cluster_info(peer_api_uris[sender])["raft_info"]
        return raft_info["commit"] >= start_commit and raft_info["pending_operations"] > 0

    wait_for(sender_paused_in_apply)
    kill_peer(sender)

    # An update now fails against the sender, which marks its replica `Dead` and aborts the
    # transfer. The receiver's replica goes `Dead` as well and is recovered from the source. It
    # stays `Partial` for a while: finishing that transfer waits for the dead sender to confirm
    # consensus first.
    upsert_random_points(source_uri, 100, offset=N_POINTS, batch_size=100)
    baseline = _count_points(source_uri)
    wait_for(
        lambda: (_local_shard(receiver_uri) or {}).get("state") == "Partial",
        wait_for_timeout=60,
    )

    # Restart the sender without the delay. It replays the `Start` before rejoining consensus and
    # spawns the driver of the aborted transfer, while the receiver is `Partial` in another one.
    wait_for_peer_online(
        start_killed_peer(sender, "peer_sender_restarted.log"), path="/healthz"
    )

    # The cluster converges with every point on every replica, the receiver's included
    wait_for(
        check_collection_shard_transfers_count,
        source_uri,
        COLLECTION_NAME,
        0,
        wait_for_timeout=120,
    )
    for uri in peer_api_uris:
        wait_for(check_all_replicas_active, uri, COLLECTION_NAME, wait_for_timeout=120)
        assert _count_points(uri) == baseline

    receiver_shard = _local_shard(receiver_uri)
    assert receiver_shard["state"] == "Active"
    assert receiver_shard["points_count"] == baseline
