"""
Test that the consensus thread automatically restarts after a transient error
while applying a committed operation.

Uses the staging-only `test_transient_error` cluster operation, which fails with the
given probability when applied on the targeted peer, stopping its consensus thread
with a service error. Requires the `staging` cargo feature to be enabled in the
qdrant binary under test.
"""

import pathlib

import pytest

from .fixtures import create_collection
from .utils import *

COLLECTION_NAME = "test_collection"

# Failure probability of a single roll. Applying the poisoned entry is retried on every
# consensus thread restart (with exponential backoff between attempts), so higher values
# make the test slower: the entry has to pass the roll for the peer to recover.
FAILURE_PROBABILITY = 0.3

# P(no failure triggered) = (1 - FAILURE_PROBABILITY) ^ MAX_PROPOSALS ~= 0.01%
MAX_PROPOSALS = 25

# Recovery requires the poisoned entry to pass its rolls; with backoff doubling from 1s
# this covers ~9 consecutive failed restart attempts (P < 0.5%)
RECOVERY_TIMEOUT_SEC = 600


def propose_transient_error(peer_api_uri: str, target_peer_id: int) -> requests.Response:
    return requests.post(
        f"{peer_api_uri}/collections/{COLLECTION_NAME}/cluster?timeout=20",
        json={
            "test_transient_error": {
                "peer_id": target_peer_id,
                "failure_probability": FAILURE_PROBABILITY,
            }
        },
    )


def consensus_status(peer_api_uri: str) -> dict:
    return get_cluster_info(peer_api_uri)["consensus_thread_status"]


def check_consensus_restarting(peer_api_uri: str) -> bool:
    status = consensus_status(peer_api_uri)
    return "restarting consensus thread" in status.get("err", "")


def check_peer_recovered(peer_api_uris: list, target_uri: str) -> bool:
    if consensus_status(target_uri)["consensus_thread_status"] != "working":
        return False
    commits = set()
    for uri in peer_api_uris:
        info = get_cluster_info(uri)
        commits.add(info["raft_info"]["commit"])
        if info["raft_info"]["pending_operations"] != 0:
            return False
    return len(commits) == 1


def test_consensus_restart_on_transient_error(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(tmp_path, 3)

    create_collection(peer_api_uris[0], shard_number=1, replication_factor=3)
    wait_for_uniform_collection_existence(COLLECTION_NAME, peer_api_uris)

    # Target a follower, so the rest of the cluster keeps a quorum while it is down.
    # Propose through the target itself: when the roll fails on the proposing peer,
    # the API returns the simulated error, so the failure is detected deterministically.
    leader = get_leader(peer_api_uris[0])
    target_uri = next(
        uri for uri in peer_api_uris if get_cluster_info(uri)["peer_id"] != leader
    )
    target_peer_id = get_cluster_info(target_uri)["peer_id"]
    healthy_uris = [uri for uri in peer_api_uris if uri != target_uri]

    triggered = False
    for i in range(MAX_PROPOSALS):
        resp = propose_transient_error(target_uri, target_peer_id)

        # `ClusterOperations` is an untagged enum, unsupported operations are reported
        # as not matching any variant
        if resp.status_code == 400 and "did not match any variant" in resp.text:
            pytest.skip("qdrant is built without the `staging` feature")

        if resp.ok:
            # Applied without failure on the target, roll again
            continue

        assert "TestTransientError" in resp.text, resp.text
        print(f"Triggered transient consensus error after {i + 1} proposals")
        triggered = True
        break

    assert triggered, f"No transient error triggered in {MAX_PROPOSALS} proposals"

    # The target peer must report the restart loop being engaged
    wait_for(check_consensus_restarting, target_uri, wait_for_timeout=5)

    # The rest of the cluster keeps working while the target peer is down
    create_collection(
        healthy_uris[0],
        collection="collection_during_outage",
        shard_number=1,
        replication_factor=2,
    )
    for uri in healthy_uris:
        wait_for_collection(uri, "collection_during_outage")

    # The target peer eventually re-applies the poisoned entry successfully,
    # recovers its consensus thread and catches up with the cluster
    wait_for(
        check_peer_recovered,
        peer_api_uris,
        target_uri,
        wait_for_timeout=RECOVERY_TIMEOUT_SEC,
        wait_for_interval=1,
    )

    # Consensus operations propagate to the recovered peer again
    create_collection(
        peer_api_uris[0],
        collection="collection_after_recovery",
        shard_number=1,
        replication_factor=3,
    )
    wait_for_uniform_collection_existence("collection_after_recovery", peer_api_uris)
