import pathlib

from .fixtures import create_collection
from .utils import *
from .assertions import assert_http_ok

N_PEERS = 3
N_SHARDS = 1
N_REPLICA = 3


# Test update write ordering guarantees.
def test_write_ordering(tmp_path: pathlib.Path):
    assert_project_root()

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA)
    wait_collection_exists_and_active_on_all_peers(
        collection_name="test_collection",
        peer_api_uris=peer_api_uris
    )

    max_peer_url = fetch_highest_peer_id(peer_api_uris)
    url_index = peer_api_uris.index(max_peer_url)

    # Kill update leader peer
    print(f"Stopping update leader peer with url {max_peer_url} at index {url_index}")
    p = processes.pop(url_index)
    p.kill()
    peer_api_uris.pop(url_index)

    # write ordering weak (will trigger the detection of dead replicas)
    r = requests.put(
        f"{peer_api_uris[0]}/collections/test_collection/points?wait=true&ordering=weak", json={
            "points": [
                {
                    "id": 1,
                    "vector": [0.05, 0.61, 0.76, 0.74],
                    "payload": {
                        "city": "Berlin",
                        "country": "Germany",
                    }
                }
            ]
        })
    assert_http_ok(r)

    # Assert that there are dead replicas
    wait_for_some_replicas_not_active(peer_api_uris[0], "test_collection")

    # write ordering medium
    r = requests.put(
        f"{peer_api_uris[0]}/collections/test_collection/points?wait=true&ordering=medium", json={
            "points": [
                {
                    "id": 1,
                    "vector": [0.05, 0.61, 0.76, 0.74],
                    "payload": {
                        "city": "Munich",
                        "country": "Germany",
                    }
                }
            ]
        })
    # succeeds as Medium selects an Alive peer
    assert_http_ok(r)

    # Write ordering with filter update operation
    r = requests.post(
        f"{peer_api_uris[0]}/collections/test_collection/points/delete?wait=true&ordering=medium", json={
            "filter": {
                "must": [
                    {
                        "key": "a",
                        "match": {
                            "value": 100
                        }
                    }
                ]
            }
        }
    )

    # succeeds as Medium selects an Alive peer
    assert_http_ok(r)

    # write ordering strong
    r = requests.put(
        f"{peer_api_uris[0]}/collections/test_collection/points?wait=true&ordering=strong", json={
            "points": [
                {
                    "id": 1,
                    "vector": [0.05, 0.61, 0.76, 0.74],
                    "payload": {
                        "city": "Dresden",
                        "country": "Germany",
                    }
                }
            ]
        })

    # fails as highest peer is dead
    assert r.status_code == 500

    # Restart peer
    new_url = start_peer(peer_dirs[url_index], f"peer_{url_index}_restarted.log", bootstrap_uri)
    peer_api_uris.append(new_url)

    # Wait for peers to be online
    wait_for_peer_online(new_url)
    time.sleep(1)

    timeout = 10
    while True:
        if timeout < 0:
            raise Exception("Timeout waiting for all replicas to be active")

        all_active = True
        points_counts = set()
        for peer_api_uri in peer_api_uris:
            res = check_collection_cluster(peer_api_uri, "test_collection")
            points_counts.add(res['points_count'])
            if res['state'] != 'Active':
                all_active = False

        if all_active:
            if len(points_counts) != 1:
                for peer_api_uri in peer_api_uris:
                    res = requests.post(f"{peer_api_uri}/collections/test_collection/points/count", json={"exact": True})
                    print(res.json())

                assert False, f"Points count is not equal on all peers: {points_counts}"
            break

        time.sleep(1)
        timeout -= 1
