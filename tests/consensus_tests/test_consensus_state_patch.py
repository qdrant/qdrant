import pathlib

from consensus_tests.fixtures import create_collection

from .utils import *

N_PEERS = 3
COLLECTION_NAME = "test_collection"


def test_consensus_state_patch(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_api_uris[0], shard_number=1, replication_factor=N_PEERS)
    wait_for_uniform_collection_existence(COLLECTION_NAME, peer_api_uris)
    for peer_api_uri in peer_api_uris:
        wait_for_all_replicas_active(peer_api_uri, COLLECTION_NAME)

    # Patch the last peer, so it can be restarted, and mark the first peer's replica dead in it
    target_uri = peer_api_uris[-1]
    other_uris = peer_api_uris[:-1]
    first_peer_id = get_cluster_info(peer_api_uris[0])["peer_id"]

    state = get_consensus_state(target_uri)
    assert COLLECTION_NAME in state["collections_data"]["collections"]

    diff = {
        "collections": {
            COLLECTION_NAME: {
                "update": {"shards": {"0": {"replicas": {str(first_peer_id): "Dead"}}}},
            },
        },
    }

    # Unknown fields are rejected
    resp = requests.patch(f"{target_uri}/cluster/consensus_state", json={"collection": {}})
    assert resp.status_code == 400, resp.text

    # Dry run is the default, it reports the change but doesn't apply it
    changes = patch_consensus_state(target_uri, diff, params={})
    assert [change["path"] for change in changes] == [
        f"collections.{COLLECTION_NAME}.shards.0.replicas.{first_peer_id}",
    ]
    assert replica_state(target_uri, first_peer_id) == "Active"

    # Applied patch changes the state of the target peer only
    changes = patch_consensus_state(target_uri, diff, params={"dry_run": "false"})
    assert len(changes) == 1
    assert replica_state(target_uri, first_peer_id) == "Dead"
    for peer_api_uri in other_uris:
        assert replica_state(peer_api_uri, first_peer_id) == "Active"

    # Applying the same patch again changes nothing
    changes = patch_consensus_state(target_uri, diff, params={"dry_run": "false"})
    assert changes == []

    # Patched state persists across restart
    processes.pop().kill()
    restarted_uri = start_peer(peer_dirs[-1], "peer_restarted.log", bootstrap_uri)
    wait_for_peer_online(restarted_uri)
    assert replica_state(restarted_uri, first_peer_id) == "Dead"


def get_consensus_state(peer_api_uri: str) -> dict:
    resp = requests.get(f"{peer_api_uri}/cluster/consensus_state")
    assert_http_ok(resp)
    return resp.json()["result"]


def patch_consensus_state(peer_api_uri: str, diff: dict, params: dict) -> list:
    resp = requests.patch(f"{peer_api_uri}/cluster/consensus_state", json=diff, params=params)
    assert_http_ok(resp)
    return resp.json()["result"]["changes"]


def replica_state(peer_api_uri: str, peer_id: int) -> str:
    """State of the replica of shard 0 on `peer_id`, as seen by the peer at `peer_api_uri`"""
    info = get_collection_cluster_info(peer_api_uri, COLLECTION_NAME)

    if info["peer_id"] == peer_id:
        shards = info["local_shards"]
    else:
        shards = [shard for shard in info["remote_shards"] if shard["peer_id"] == peer_id]

    [shard] = [shard for shard in shards if shard["shard_id"] == 0]
    return shard["state"]
