import pathlib

from .assertions import assert_http_ok
from .utils import *


def test_aliases_migrated_from_single_node(tmp_path: pathlib.Path):
    peer_dirs = make_peer_folders(tmp_path, 2)

    standalone_api_uri, _ = start_first_peer(
        peer_dirs[0], "standalone.log", extra_env={"QDRANT__CLUSTER__ENABLED": "false"}
    )
    wait_for_peer_online(standalone_api_uri)

    for collection_name in ("current", "previous"):
        response = requests.put(
            f"{standalone_api_uri}/collections/{collection_name}",
            json={"vectors": {"size": 4, "distance": "Dot"}},
        )
        assert_http_ok(response)

    aliases = {"live": "current", "old": "previous"}
    response = requests.post(
        f"{standalone_api_uri}/collections/aliases",
        json={
            "actions": [
                {"create_alias": {"collection_name": collection, "alias_name": alias}}
                for alias, collection in aliases.items()
            ]
        },
    )
    assert_http_ok(response)

    processes.pop().kill()

    bootstrap_api_uri, bootstrap_uri = start_first_peer(peer_dirs[0], "cluster.log")
    leader = wait_peer_added(bootstrap_api_uri)
    joined_api_uri = start_peer(peer_dirs[1], "joined.log", bootstrap_uri)
    peer_api_uris = [bootstrap_api_uri, joined_api_uri]
    wait_for_uniform_cluster_status(peer_api_uris, leader)

    for collection_name in aliases.values():
        wait_collection_exists_and_active_on_all_peers(collection_name, peer_api_uris)

    def aliases_match(peer_api_uri):
        response = requests.get(f"{peer_api_uri}/aliases")
        return response.ok and {
            alias["alias_name"]: alias["collection_name"]
            for alias in response.json()["result"]["aliases"]
        } == aliases

    for peer_api_uri in peer_api_uris:
        wait_for(aliases_match, peer_api_uri)

        for alias in aliases:
            response = requests.post(f"{peer_api_uri}/collections/{alias}/points/count", json={})
            assert_http_ok(response)
            assert response.json()["result"]["count"] == 0
