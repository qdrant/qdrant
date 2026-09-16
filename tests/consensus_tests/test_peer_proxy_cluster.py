import pytest
import requests

from .fixtures import create_collection, upsert_points
from .utils import *


COLLECTION_NAME = "test_collection"
RECOVERY_POINT = "/qdrant.CollectionsInternal/GetShardRecoveryPoint"


def peer_has_metadata_value(uri, metadata_path, expected_value):
    response = requests.get(f"{uri}{metadata_path}", timeout=5)
    assert_http_ok(response)
    return response.json()["result"] == expected_value


@pytest.mark.parametrize("uris_in_env", [False, True], ids=["cli-uri", "env-uri"])
def test_peer_proxy_cluster_transfer_and_restart(tmp_path, uris_in_env):
    peer_uris, peer_dirs, bootstrap_uri = start_cluster(
        tmp_path, 3, uris_in_env=uris_in_env, use_peer_proxy=True,
    )
    peers = list(processes)
    peer_ids = [get_cluster_info(uri)["peer_id"] for uri in peer_uris]
    expected_addresses = {
        str(peer_id): peer.proxy.uri for peer_id, peer in zip(peer_ids, peers)
    }
    assert bootstrap_uri == peers[0].proxy.uri
    for uri in peer_uris:
        assert {
            peer_id: peer["uri"].rstrip("/")
            for peer_id, peer in get_cluster_info(uri)["peers"].items()
        } == expected_addresses

    create_collection(peer_uris[0], shard_number=1, replication_factor=3, write_consistency_factor=3)
    wait_collection_exists_and_active_on_all_peers(COLLECTION_NAME, peer_uris)
    points = [
        {"id": index, "vector": [1.0, 0.0, 0.0, 0.0], "payload": {"index": index}}
        for index in range(100)
    ]
    assert_http_ok(upsert_points(peer_uris[0], points))

    with peers[0].proxy.hold(RECOVERY_POINT) as gate:
        replicate_shard(peer_uris[2], COLLECTION_NAME, 0, peer_ids[2], peer_ids[0], method="wal_delta")
        gate.wait_for_request()
        transfer = get_collection_cluster_info(peer_uris[0], COLLECTION_NAME)["shard_transfers"]
        assert len(transfer) == 1
        assert (transfer[0]["from"], transfer[0]["to"], transfer[0]["shard_id"]) == (
            peer_ids[2], peer_ids[0], 0,
        )
        assert transfer[0]["method"] == "wal_delta"

        # This requires Raft progress while the selected transfer RPC is held.
        metadata_path = "/cluster/metadata/keys/proxy-check"
        assert_http_ok(requests.put(f"{peer_uris[1]}{metadata_path}?wait=true", json="while-held", timeout=10))
        for uri in peer_uris:
            wait_for(peer_has_metadata_value, uri, metadata_path, "while-held")
        assert not gate.cancelled.is_set()

    for uri in peer_uris:
        wait_for_collection_shard_transfers_count(uri, COLLECTION_NAME, 0)
        wait_for_all_replicas_active(uri, COLLECTION_NAME, min_local_replicas=1)

    # A restart must retain its advertised address even without repeating the
    # option. Otherwise surviving peers can bypass the proxy after the restart.
    restarted_peer = peers[2]
    restarted_peer.kill()
    processes.remove(restarted_peer)
    peer_uris[2] = start_peer(
        peer_dirs[2], "peer_2_restarted.log", bootstrap_uri,
        port=restarted_peer.p2p_port, uris_in_env=uris_in_env,
    )
    assert processes[-1].proxy is restarted_peer.proxy
    wait_for_peer_online(peer_uris[2])
    wait_collection_exists_and_active_on_all_peers(COLLECTION_NAME, peer_uris)

    for uri in peer_uris:
        assert get_cluster_info(uri)["peers"][str(peer_ids[2])]["uri"].rstrip("/") == restarted_peer.proxy.uri
        local_shards = get_collection_cluster_info(uri, COLLECTION_NAME)["local_shards"]
        assert len(local_shards) == 1
        assert local_shards[0]["points_count"] == len(points)
        response = requests.post(
            f"{uri}/collections/{COLLECTION_NAME}/points/scroll?consistency=all",
            json={"limit": 100, "with_payload": True, "with_vector": True},
            timeout=10,
        )
        assert_http_ok(response)
        assert response.json()["result"]["points"] == points

    proxies = [peer.proxy for peer in peers]
    kill_all_processes()
    assert not peer_proxies
    assert all(not proxy._thread.is_alive() for proxy in proxies)
    assert all(proxy.port not in busy_ports for proxy in proxies)
