"""
Test consistency during updates and shard transfer
1. Create a 3-nodes cluster
2. Create a collection with 3 shards, replication factor: 3
3. Insert starting points (10k) with payload (timestamp)
4. Start updating points in the background (random nodes)
5. Kill 1 node
6. Wait for some time and start the node back again
7. Stop updating points
8. Check consistency
"""
import builtins
import logging
import multiprocessing
import pathlib
from concurrent.futures import ProcessPoolExecutor, as_completed
import random
from datetime import datetime
from http.client import RemoteDisconnected

from urllib3.exceptions import NewConnectionError, MaxRetryError, ProtocolError

from .fixtures import create_collection, random_dense_vector
from .utils import *

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

INITIAL_POINTS_COUNT = 10000
POINTS_COUNT_UNDER_CHECK = 10000
N_PEERS = 3
N_SHARDS = 3
N_REPLICAS = 2
COLLECTION_NAME = "test_collection"
INCONSISTENCY_CHECK_ATTEMPTS = 10
HEADERS = {}

pid = os.getpid()

def print(*args, **kwargs):
    new_args = args + (f"pid={pid}",)
    builtins.print(*new_args, **kwargs)


def upsert_random_points_with_payload(
    peer_url,
    num,
    collection_name=COLLECTION_NAME,
    fail_on_error=True,
    offset=0,
    batch_size=None,
    wait="true",
    ordering="weak",
    shard_key=None,
    headers=None
):
    if not headers:
        headers = HEADERS

    def get_vector():
        # Create points in first peer's collection
        vector = {
            "": random_dense_vector(),
        }

        return vector

    while num > 0:
        size = num if batch_size is None else min(num, batch_size)

        timestamp_str = datetime.now().isoformat()
        points = [
                    {
                        "id": i + offset,
                        "vector": get_vector(),
                        "payload": {
                            "a": f"keyword_{random.randint(1, 1000)}",
                            "timestamp": timestamp_str
                        },
                    }
                    for i in range(size)
                ]
        try:
            r_batch = requests.put(
                f"{peer_url}/collections/{collection_name}/points?wait={wait}&ordering={ordering}",
                json={
                    "points": points,
                    "shard_key": shard_key,
                },
                headers=headers,
            )
            if fail_on_error:
                assert_http_ok(r_batch)
        except requests.exceptions.ConnectionError:
            # print(f"Got connection error from {peer_url}. Skip")
            return
        except ConnectionRefusedError:
            # print(f"Got connection error from {peer_url}. Skip")
            return
        except NewConnectionError:
            # print(f"Got connection error from {peer_url}. Skip")
            return
        except MaxRetryError:
            # print(f"Got connection error from {peer_url}. Skip")
            return
        except RemoteDisconnected:
            # print(f"Got connection error from {peer_url}. Skip")
            return
        except ProtocolError:
            # print(f"Got connection error from {peer_url}. Skip")
            return

        num -= size
        offset += size


def update_points_in_loop(peer_api_uris, collection_name):
    limit = 100
    while True:
        offset = random.randint(0, INITIAL_POINTS_COUNT)
        peer_url = random.choice(peer_api_uris)
        upsert_random_points_with_payload(
            peer_url,
            num=limit,
            collection_name=collection_name,
            fail_on_error=False, offset=offset, wait='true')


def run_update_points_in_background(peer_api_uris, collection_name):
    p = multiprocessing.Process(target=update_points_in_loop, args=(peer_api_uris, collection_name))
    p.start()
    return p


def get_all_points(peer_url, collection_name):
    res = requests.post(
        f"{peer_url}/collections/{collection_name}/points/scroll",
        json={
            "limit": INITIAL_POINTS_COUNT,
            "with_vector": True,
            "with_payload": True,
        },
        timeout=30,
        headers=HEADERS
    )
    assert_http_ok(res)
    return res.json()["result"]


def wait_for_shard_transfers(peer_api_uris, collection_name):
    random_api_uri = peer_api_uris[0]
    source_uri = random_api_uri
    wait_for_collection_shard_transfers_count(source_uri, collection_name, 0, headers=HEADERS)

    return True


def get_points_ids_from_peer(uri, point_ids):
    if len(point_ids) == 0:
        print(
            f'level=INFO msg="Skipping because no check required for node" node={uri}'
        )
        return None

    try:
        response = requests.post(
            f"{uri}/collections/benchmark/points",
            headers=HEADERS,
            json={"ids": point_ids, "with_vector": False, "with_payload": True},
            timeout=10,
        )
    except requests.exceptions.Timeout:
        print(
            f'level=WARN msg="Request timed out after 10s, skipping all points all peers consistency check for node" uri="{uri}" api="/collections/benchmark/points"'
        )
        return None

    if response.status_code != 200:
        error_msg = response.text.strip()
        if error_msg in ("404 page not found", "Service Unavailable"):
            print(
                f'level=WARN msg="Node unreachable, skipping all points all peers consistency check" uri="{uri}" status_code={response.status_code} err="{error_msg}"'
            )
            return None
        else:
            # Some unknown error:
            print(
                f'level=ERROR msg="Failed to fetch points" uri="{uri}" status_code={response.status_code} err="{error_msg}"'
            )
            return None

    return {item["id"]: item["payload"] for item in response.json()["result"]}


def check_consistency(peer_api_uris, loop_num='final'):
    print("8. Check consistency")
    print('level=INFO msg="Starting all points all peers data consistency check script"')

    QC_NAME = "local"
    # POINTS_DIR = f"data/{QC_NAME}-points-dump/loop-{loop_num}"
    # os.makedirs(POINTS_DIR, exist_ok=True)

    def get_points_from_all_peers_parallel(qdrant_peers, attempt_number, node_points_map):
        for node_idx, uri in enumerate(qdrant_peers):
            if not node_points_map.get(node_idx):
                node_points_map[node_idx] = {}

        with ProcessPoolExecutor() as executor:
            future_to_uri = {
                executor.submit(get_points_ids_from_peer, uri, point_ids_for_node): (node_idx, uri) for
                node_idx, uri in enumerate(qdrant_peers)}
            for future in as_completed(future_to_uri):
                node_idx, uri = future_to_uri[future]
                fetched_points = future.result()

                if fetched_points:
                    fetched_points_count = len(fetched_points)
                    node_points_map[node_idx][attempt_number] = fetched_points
                else:
                    fetched_points_count = 0
                    fetched_points = {}

                print(
                    f'level=INFO msg="Fetched points" num_points={fetched_points_count} uri="{uri}"'
                )
                # with open(
                #         f"{POINTS_DIR}/node-{node_idx}-attempt-{attempt_number}.json", "w"
                # ) as f:
                #     json.dump(fetched_points, f)

        return node_points_map

    def check_for_consistency(node_to_points_map, attempt_number, consistent_points):
        print(
            f'level=INFO msg="Start checking points, attempt_number={attempt_number}"'
        )
        for point in initial_point_ids:
            if consistent_points[point]:
                # if point is already consistent, no need to check again
                continue

            # get point's payload from all nodes
            point_attempt_versions_list = []
            for node_idx, node in node_to_points_map.items():
                if not node or not node.get(attempt_number):
                    # print(f"level=INFO msg='No points for node, skip' node_idx={node_idx} attempt_number={attempt_number}")
                    continue
                try:
                    version = node[attempt_number][point]
                except KeyError:
                    # print(
                    #     f"level=WARN msg='Missing point for node' node_idx={node_idx} attempt_number={attempt_number} point={point}")
                    version = None
                point_attempt_versions_list.append(version)

            first_obj = point_attempt_versions_list[0]
            is_point_consistent = all(obj == first_obj for obj in point_attempt_versions_list)

            if is_point_consistent:
                consistent_points[point] = True
                continue

            point_history_nodes = []  # point history over different attempts for each node
            for node_idx, node in node_to_points_map.items():
                point_history = set()

                for attempt in range(attempt_number + 1):
                    if node.get(attempt):
                        payload = node.get(attempt).get(point, None)
                        if payload:
                            point_history.add(tuple(sorted(payload.items())))
                        else:
                            # point is missing for this node on this attempt
                            point_history.add((("a",""), ("timestamp","")))

                if point_history:
                    # if node has no data for this point (was unreachable all this time), skip it
                    point_history_nodes.append(point_history)

            common_objects = set.intersection(*point_history_nodes)
            common_objects = [dict(obj) for obj in common_objects]
            if len(common_objects) > 0:
                consistent_points[point] = True

        is_consistent = all(consistent_points.values())
        return is_consistent

    num_points_to_check = POINTS_COUNT_UNDER_CHECK
    initial_point_ids = list(range(num_points_to_check))
    point_ids_for_node = list(range(num_points_to_check))
    # is_data_consistent = False
    consistency_attempts_remaining = INCONSISTENCY_CHECK_ATTEMPTS
    node_to_points_map = {}
    consistent_points = {}

    while True:
        attempt_number = INCONSISTENCY_CHECK_ATTEMPTS - consistency_attempts_remaining

        node_to_points_map = get_points_from_all_peers_parallel(peer_api_uris, attempt_number, node_to_points_map)
        if attempt_number == 0:
            # initialize all points on the 1st attempt
            for point in initial_point_ids:
                consistent_points[point] = False

        is_data_consistent = check_for_consistency(node_to_points_map, attempt_number, consistent_points)

        consistency_attempts_remaining -= 1

        if is_data_consistent:
            print(
                f'level=INFO msg="All points all peers data consistency check succeeded" attempts={INCONSISTENCY_CHECK_ATTEMPTS - consistency_attempts_remaining}'
            )
            break
        else:
            inconsistent_point_ids = [i for i, val in consistent_points.items() if not val]

            if consistency_attempts_remaining == 0:
                print(
                    f'level=ERROR msg="All points all peers data consistency check failed" attempts={INCONSISTENCY_CHECK_ATTEMPTS - consistency_attempts_remaining} inconsistent_count={len(inconsistent_point_ids)} inconsistent_points="{inconsistent_point_ids[:20]}"'
                )

                last_fetched_node_inconsistent_points = []
                for point_id in inconsistent_point_ids:
                    point_data = {point_id: {}}
                    for node_idx, node_data in node_to_points_map.items():
                        point_data[point_id][f"node-{node_idx}"] = node_data[attempt_number][point_id] if node_data.get(
                            attempt_number) else None
                    last_fetched_node_inconsistent_points.append(point_data)

                print(
                    f'level=ERROR msg="Dumping inconsistent points (max 5)" last_fetched_points={last_fetched_node_inconsistent_points[:5]}')
                POINTS_DIR = f"data/{QC_NAME}-points-dump/loop-{loop_num}"
                os.makedirs(POINTS_DIR, exist_ok=True)
                with open(
                        f"{POINTS_DIR}/diff.json", "w"
                ) as f:
                    json.dump(last_fetched_node_inconsistent_points, f)

                break
            else:
                print(
                    f'level=WARN msg="Nodes might be inconsistent. Will retry" inconsistent_count={len(inconsistent_point_ids)} inconsistent_points="{inconsistent_point_ids[:20]}"'
                )
                print(
                    f'level=WARN msg="Retrying all points all peers data consistency check" attempts={INCONSISTENCY_CHECK_ATTEMPTS - consistency_attempts_remaining} remaining_attempts={consistency_attempts_remaining}'
                )
                point_ids_for_node = [x for x in inconsistent_point_ids]
                # Node might be unavailable which caused request to fail. Give some time to heal
                time.sleep(5)
                continue

    return is_data_consistent


def get_peer_id(peer_api_uri):
    r = requests.get(f"{peer_api_uri}/cluster")
    assert_http_ok(r)
    return r.json()["result"]["peer_id"]


def test_consistency(tmp_path: pathlib.Path):
    assert_project_root()

    print("\n1. Create a 3-nodes cluster")
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    print(f"2. Create a collection with {N_SHARDS} shards")
    create_collection(peer_api_uris[0], collection=COLLECTION_NAME, shard_number=N_SHARDS, replication_factor=N_REPLICAS, headers=HEADERS)
    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris, headers=HEADERS)

    print("3. Insert starting points (10k) with payload (timestamp)")
    upsert_random_points_with_payload(peer_api_uris[0], INITIAL_POINTS_COUNT, COLLECTION_NAME, batch_size=100, wait='false')
    print("     Wait for optimizations to complete")
    for uri in peer_api_uris:
        wait_collection_green(uri, COLLECTION_NAME)

    print("4. Start updating points in the background (random nodes)")
    upsert_process = run_update_points_in_background(peer_api_uris, COLLECTION_NAME)
    print("     Wait for some time")
    time.sleep(5)

    print("5-6. Kill 1 node. Wait for some time and start the node back again")
    process_peer_id = get_peer_id(peer_api_uris[-1])
    p = processes.pop()
    p_http_port = str(p.http_port)
    p_p2p_port = str(p.p2p_port)
    p_grpc_port = str(p.grpc_port)
    print(f"    Kill node http://localhost:{p_http_port}, p2p port: {p_p2p_port}, grpc port: {p_grpc_port}")
    p.kill()
    wait_for_some_replicas_not_active(peer_api_uris[0], COLLECTION_NAME)
    # Remove last peer from cluster
    res = requests.delete(
        f"{peer_api_uris[0]}/cluster/peer/{process_peer_id}?force=true"
    )
    assert_http_ok(res)
    time.sleep(3)
    new_url = start_peer(peer_dirs[-2], "peer_0_restarted.log", bootstrap_uri, given_grpc_port=p_grpc_port, given_http_port=p_http_port)
    print(f"    Restarted a node: {new_url}")
    wait_for_all_replicas_active(peer_api_uris[0], COLLECTION_NAME)

    upsert_process_2 = run_update_points_in_background([new_url], COLLECTION_NAME)

    print("7. Wait for some time and stop updating points")
    time.sleep(3)
    upsert_process.kill()
    upsert_process_2.kill()

    check_consistency(peer_api_uris)

    print("OK")
