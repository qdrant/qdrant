import os
import pathlib
import signal
from time import sleep
from typing import Any

from .fixtures import create_collection, upsert_random_points
from .utils import *

N_PEERS = 3
N_SHARDS = 1
N_REPLICAS = 1
COLLECTION_NAME = "test_collection"

# Points in the shard; enough that the streaming transfer is still running when
# the source peer gets paused shortly after the transfer starts
N_POINTS = 20_000


def test_consensus_snapshot_cleans_up_aborted_transfer_proxy(tmp_path: pathlib.Path):
    """
    A transfer source that misses the transfer abort must recover via consensus snapshot.

    Scenario (reproduces a 1.16.3 production incident):
    1. The source of a `stream_records` shard transfer wraps its local shard into a forward
       proxy, then gets paused (SIGSTOP) mid-transfer.
    2. The two other peers are killed and restarted while the source is paused. The fresh
       leader keeps the unresponsive source in raft probe state (so no log entries are
       streamed into the source's socket buffers), and aggressive WAL compaction removes
       the entries the source would need — the source can only ever catch up by consensus
       snapshot.
    3. While the source is unresponsive, the transfer is aborted through consensus and the
       target drops its partial shard. The source never applies the abort, so its local
       shard stays proxified.
    4. On resume, the source applies the consensus snapshot. Re-creating payload indexes
       during snapshot application sends an update operation through the stale forward
       proxy to the target, which no longer has the shard. The resulting error failed
       snapshot application and killed the consensus thread, leaving the source unable to
       ever catch up.

    The fix un-proxifies the local shard when snapshot application drops a transfer that
    consensus no longer knows about, so the snapshot applies cleanly.
    """
    assert_project_root()

    env = {
        # Aggressively compact consensus WAL, so the paused peer can only catch up by snapshot
        "QDRANT__CLUSTER__CONSENSUS__COMPACT_WAL_ENTRIES": "1",
    }

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, extra_env=env)

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICAS)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris,
    )

    # Payload index is part of the consensus snapshot; re-creating it during snapshot
    # application is what sends an update operation through the stale proxy
    r = requests.put(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/index?wait=true",
        json={"field_name": "city", "field_schema": "keyword"},
    )
    assert_http_ok(r)

    upsert_random_points(peer_api_uris[0], N_POINTS, batch_size=1000)

    # Find the peer holding the only shard (transfer source) and pick a target without it
    source_idx = None
    for idx, uri in enumerate(peer_api_uris):
        info = get_collection_cluster_info(uri, COLLECTION_NAME)
        if len(info["local_shards"]) > 0:
            source_idx = idx
            source_peer_id = info["peer_id"]
            shard_id = info["local_shards"][0]["shard_id"]
    assert source_idx is not None, "no peer holds the shard"

    target_idx = (source_idx + 1) % N_PEERS
    other_idx = (source_idx + 2) % N_PEERS
    source_uri = peer_api_uris[source_idx]
    target_uri = peer_api_uris[target_idx]
    other_uri = peer_api_uris[other_idx]
    target_peer_id = get_collection_cluster_info(target_uri, COLLECTION_NAME)["peer_id"]

    # Start a streaming move; the source wraps its local shard into a forward proxy
    r = requests.post(
        f"{source_uri}/collections/{COLLECTION_NAME}/cluster",
        json={
            "move_shard": {
                "shard_id": shard_id,
                "from_peer_id": source_peer_id,
                "to_peer_id": target_peer_id,
                "method": "stream_records",
            }
        },
    )
    assert_http_ok(r)

    # As soon as the transfer is registered, give the transfer driver on the source a
    # moment to proxify the local shard, then pause the source mid-transfer
    wait_for(
        lambda: get_shard_transfer_count(source_uri, COLLECTION_NAME) == 1,
        wait_for_interval=0.05,
    )
    sleep(0.3)
    source_process = processes[source_idx]
    os.kill(source_process.pid, signal.SIGSTOP)

    try:
        # The transfer must still be running, otherwise the source was paused too late and
        # its local shard is no longer proxified
        assert get_shard_transfer_count(other_uri, COLLECTION_NAME) == 1, \
            "transfer finished before the source was paused; increase N_POINTS"

        # Restart the two other peers. This drops the raft connections to the paused
        # source: a SIGSTOPped process still accepts TCP data into its socket buffers, so
        # a leader that stays up would deliver all missed log entries (including the
        # transfer abort below) right when the source resumes, and the source would never
        # need a consensus snapshot. A freshly elected leader instead keeps the
        # unresponsive source in raft probe state, and its aggressively compacted WAL
        # cannot bridge the source's log gap — snapshot is the only way to catch up.
        for idx in (target_idx, other_idx):
            peer_process = processes[idx]
            port = peer_process.p2p_port
            peer_process.kill()
            peer_api_uris[idx] = start_peer(
                peer_dirs[idx], f"peer_{idx}_restarted.log", bootstrap_uri, port=port, extra_env=env,
            )
        target_uri = peer_api_uris[target_idx]
        other_uri = peer_api_uris[other_idx]
        wait_all_peers_up([target_uri, other_uri])
        wait_for(
            lambda: get_cluster_info(other_uri)["raft_info"]["leader"] is not None,
        )

        # Abort the transfer; every live peer applies it, and the target drops its partial
        # shard. The paused source never learns about the abort. The restarted peers may
        # have auto-aborted the transfer already (reason: "Source or target peer
        # restarted"), in which case the explicit abort reports there is no transfer.
        r = requests.post(
            f"{other_uri}/collections/{COLLECTION_NAME}/cluster",
            json={
                "abort_transfer": {
                    "shard_id": shard_id,
                    "from_peer_id": source_peer_id,
                    "to_peer_id": target_peer_id,
                }
            },
        )
        if not r.ok:
            # Depending on how far the auto-abort got, the explicit abort is rejected
            # either by API validation (400 "There is no transfer ...") or on consensus
            # application (404 "Shard transfer ... does not exist")
            error = r.json()["status"]["error"].lower()
            assert "no transfer" in error or "does not exist" in error, error
        wait_for_collection_shard_transfers_count(other_uri, COLLECTION_NAME, 0)
        wait_for(
            lambda: len(get_collection_cluster_info(target_uri, COLLECTION_NAME)["local_shards"]) == 0,
        )

        # Accumulate consensus operations; with aggressive WAL compaction the paused peer
        # falls behind the compacted WAL and can only catch up by consensus snapshot.
        # Cluster metadata operations are pure consensus operations without shard
        # placement, so they cannot get stuck on the paused peer.
        for i in range(5):
            put_metadata_key(other_uri, f"churn_{i}", f"value_{i}")

        # Marker operation to verify the source catches up with consensus after resuming
        put_metadata_key(other_uri, "marker", "after-pause")
    finally:
        os.kill(source_process.pid, signal.SIGCONT)

    # The source must catch up by applying the consensus snapshot. Without the stale
    # transfer cleanup, snapshot application fails on the proxified shard and the source
    # stays behind forever.
    wait_for(
        peer_has_metadata_key, source_uri, "marker", "after-pause",
        wait_for_timeout=60,
    )
    wait_for_same_commit(peer_api_uris)

    status = get_cluster_info(source_uri)["consensus_thread_status"]["consensus_thread_status"]
    assert status == "working"

    # The source keeps its shard, the target must not have gotten it back
    assert len(get_collection_cluster_info(source_uri, COLLECTION_NAME)["local_shards"]) == 1
    assert len(get_collection_cluster_info(target_uri, COLLECTION_NAME)["local_shards"]) == 0
    assert get_shard_transfer_count(source_uri, COLLECTION_NAME) == 0

    wait_for_all_replicas_active(source_uri, COLLECTION_NAME)

    # Updates through the source must work again: its local shard is a regular local shard,
    # not a proxy forwarding to the removed target replica
    upsert_random_points(source_uri, 100, offset=N_POINTS)

    r = requests.post(
        f"{source_uri}/collections/{COLLECTION_NAME}/points/count",
        json={"exact": True},
    )
    assert_http_ok(r)
    assert r.json()["result"]["count"] == N_POINTS + 100


def put_metadata_key(peer_uri: str, key: str, value: Any):
    resp = requests.put(f"{peer_uri}/cluster/metadata/keys/{key}?wait=true", json=value)
    assert_http_ok(resp)


def peer_has_metadata_key(peer_uri: str, key: str, expected_value: Any) -> bool:
    try:
        resp = requests.get(f"{peer_uri}/cluster/metadata/keys/{key}")
        if not resp.ok:
            return False
        return resp.json()["result"] == expected_value
    except requests.RequestException:
        return False
