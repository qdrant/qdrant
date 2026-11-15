import json
import pathlib

from .assertions import assert_http_ok
from .utils import *

COLLECTION_NAME = "test_collection"
POINTS = 100


def collection_uuid(peer_dir: pathlib.Path):
    config_path = peer_dir / "storage" / "collections" / COLLECTION_NAME / "config.json"
    if not config_path.exists():
        return None
    return json.loads(config_path.read_text()).get("uuid")


def test_collection_uuid_migrated_from_single_node(tmp_path: pathlib.Path):
    """
    A collection created in standalone mode has no UUID. After migrating to a cluster,
    the original peer must persist the same UUID as the peers that replay its creation.
    Otherwise, applying a Raft snapshot recreates the collection and loses its data.
    """
    peer_dirs = make_peer_folders(tmp_path, 2)

    standalone_api_uri, _ = start_first_peer(
        peer_dirs[0], "standalone.log", extra_env={"QDRANT__CLUSTER__ENABLED": "false"}
    )
    wait_for_peer_online(standalone_api_uri)

    response = requests.put(
        f"{standalone_api_uri}/collections/{COLLECTION_NAME}",
        json={"vectors": {"size": 4, "distance": "Dot"}},
    )
    assert_http_ok(response)

    response = requests.put(
        f"{standalone_api_uri}/collections/{COLLECTION_NAME}/points?wait=true",
        json={
            "points": [
                {"id": i, "vector": [1.0, i / POINTS, 0.5, 0.25]} for i in range(1, POINTS + 1)
            ]
        },
    )
    assert_http_ok(response)

    assert collection_uuid(peer_dirs[0]) is None

    processes.pop().kill()

    bootstrap_api_uri, bootstrap_uri = start_first_peer(peer_dirs[0], "cluster.log")
    leader = wait_peer_added(bootstrap_api_uri)
    joined_api_uri = start_peer(peer_dirs[1], "joined.log", bootstrap_uri)
    peer_api_uris = [bootstrap_api_uri, joined_api_uri]
    wait_for_uniform_cluster_status(peer_api_uris, leader)
    wait_collection_exists_and_active_on_all_peers(COLLECTION_NAME, peer_api_uris)

    def uuids_match():
        uuids = [collection_uuid(peer_dir) for peer_dir in peer_dirs]
        return uuids[0] is not None and len(set(uuids)) == 1

    wait_for(uuids_match)

    response = requests.post(
        f"{bootstrap_api_uri}/collections/{COLLECTION_NAME}/points/count", json={"exact": True}
    )
    assert_http_ok(response)
    assert response.json()["result"]["count"] == POINTS
