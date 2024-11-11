"""
Test consistency during updates and shard transfer
1. Create a 3-nodes cluster
2. Create a collection with 2 shards
3. Insert starting points (10k) with payload (timestamp)
4. Start updating points in the background (random nodes)
5. Trigger one of the shards'  replication
6. Wait for the replication to complete
7. Stop updating points
8. Check consistency
"""
import logging
import multiprocessing
import pathlib
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
import random

import builtins

from .fixtures import create_collection, random_dense_vector
from .utils import *

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

INITIAL_POINTS_COUNT = 10000
POINTS_COUNT_UNDER_CHECK = 10000
N_PEERS = 3
N_SHARDS = 3
N_REPLICAS = 2
COLLECTION_NAME = "benchmark"
LOOPS_OF_REPLICATION = 30
INCONSISTENCY_CHECK_ATTEMPTS = 10
HEADERS = {}

pid = os.getpid()

def print(*args, **kwargs):
    new_args = args + (f"pid={pid}",)
    builtins.print(*new_args, **kwargs)

def create_index(peer_url, collection=COLLECTION_NAME,
                 field_name: str = 'keyword', field_schema: str = 'keyword',
                 wait='true', ordering='weak', headers={}):
    r_batch = requests.put(
        f"{peer_url}/collections/{collection}/index?wait={wait}&ordering={ordering}",
        json={
            "field_name": field_name,
            "field_schema": field_schema
        },
        headers=headers,
    )
    assert_http_ok(r_batch)


def delete_points(
    peer_url,
    num,
    collection_name=COLLECTION_NAME,
    fail_on_error=True,
    offset=0,
    batch_size=None,
    wait="true",
    ordering="weak",
    headers=None
):
    headers = headers if headers else HEADERS

    while num > 0:
        size = num if batch_size is None else min(num, batch_size)

        points = [ i + offset for i in range(size) ]
        # print(f"Delete points: {points[0]} - {points[-1]}")
        r_batch = requests.post(
            f"{peer_url}/collections/{collection_name}/points/delete?wait={wait}&ordering={ordering}",
            json={
                "points": points
            },
            headers=headers,
        )
        if fail_on_error:
            assert_http_ok(r_batch)

        num -= size
        offset += size


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
    headers = headers if headers else HEADERS

    def get_vector():
        vector = {
            "": random_dense_vector(),
        }

        return vector

    while num > 0:
        size = num if batch_size is None else min(num, batch_size)

        timestamp_str = str(datetime.now(timezone.utc))
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
        # print(f"Upsert points: {points[0]['id']} - {points[-1]['id']}")
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


def sweep_points_in_loop(peer_api_uris, collection_name):
    offset = 0
    batch_size = 100
    while True:
        peer_url = random.choice(peer_api_uris)
        delete_points(peer_url, num=batch_size, collection_name=collection_name, offset=offset, wait='true')
        upsert_random_points_with_payload(peer_url, num=batch_size, collection_name=collection_name, offset=offset, wait='true')
        offset += batch_size


def run_sweep_points_in_background(peer_api_uris):
    p = multiprocessing.Process(target=sweep_points_in_loop, args=(peer_api_uris, COLLECTION_NAME))
    p.start()
    return p


def run_update_points_in_background_bfb(grpc_uris):
    os.environ['QDRANT_API_KEY'] = HEADERS.get('api-key', '')
    grpc_uri = random.choice(grpc_uris)
    bfb_cmd_args = (f"--replication-factor {N_REPLICAS} --shards {N_SHARDS} --keywords 10 --timestamp-payload "
                    f"--dim 768 -n 1000000000 --batch-size 100 --threads 1 --parallel 1 --wait-on-upsert true "
                    f"--create-if-missing --quantization scalar --timing-threshold 1 --on-disk-vectors true "
                    f"--max-id {INITIAL_POINTS_COUNT} --delay 1000 --timeout 30 --retry 4 --retry-interval 1 "
                    f"--uri {grpc_uri} --collection-name {COLLECTION_NAME}")
    popen_args = ["/home/tt/RustroverProjects/bfb/target/debug/bfb", *bfb_cmd_args.split(" ")]
    p = Popen(popen_args, start_new_session=True)
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


def get_some_points(peer_url, collection_name, points_ids):
    res = requests.post(
        f"{peer_url}/collections/{collection_name}/points",
        json={
            "ids": points_ids,
            "with_vector": True,
            "with_payload": True,
        },
        timeout=30,
        headers=HEADERS
    )
    assert_http_ok(res)
    return res.json()["result"]


def replicate_shard(peer_api_uris, collection_name):
    random_api_uri = random.choice(peer_api_uris)
    collection_cluster_info = get_collection_cluster_info(random_api_uri, collection_name, headers=HEADERS)
    cluster_info = get_cluster_info(random_api_uri, headers=HEADERS)
    # get ids of all the peers in the cluster
    peer_ids = []
    for peer in cluster_info["peers"]:
        peer_ids.append(int(peer))
    # print(f"peer_ids: {peer_ids}")

    while not collection_cluster_info["local_shards"]:
        print(f"     selected node {random_api_uri} doesn't have local shards, pick another")
        random_api_uri = random.choice(peer_api_uris)
        collection_cluster_info = get_collection_cluster_info(random_api_uri, collection_name, headers=HEADERS)
        time.sleep(1)
    print(f"     selected node {random_api_uri}")

    source_uri = random_api_uri
    # shard_id = collection_cluster_info["local_shards"][0]["shard_id"]
    active_local_shards = [shard for shard in collection_cluster_info["local_shards"] if shard["state"] == "Active"]
    if not active_local_shards:
        print(f"     Skip replication. No active local shards found in {collection_cluster_info}")
        return
    shard_id = random.choice(active_local_shards)["shard_id"]
    source_peer_id = collection_cluster_info["peer_id"]

    # target_peer_id = collection_cluster_info["remote_shards"][0]["peer_id"]
    # exclude source peer
    peer_ids.remove(source_peer_id)
    target_peer_id = random.choice(peer_ids)

    res = requests.post(
        f"{source_uri}/collections/{collection_name}/cluster",
        json={
            "replicate_shard": {
                "shard_id": shard_id,
                "from_peer_id": source_peer_id,
                "to_peer_id": target_peer_id,
                "method": "wal_delta"
            }
        },
        headers=HEADERS,
        timeout=10
    )
    print(f"    Replicate shard {shard_id} from {source_peer_id} to {target_peer_id}")
    assert_http_ok(res)
    time.sleep(2)
    wait_for_collection_shard_transfers_count(source_uri, collection_name, 0, headers=HEADERS)

    return res.json()["result"]


def check_consistency(peer_api_uris, peer_dirs, loop_num='final'):
    print("8. Check consistency")
    attempts_counter = INCONSISTENCY_CHECK_ATTEMPTS
    diffs = {}
    ids = random.sample(range(INITIAL_POINTS_COUNT + 1), POINTS_COUNT_UNDER_CHECK)
    is_diff = False
    while attempts_counter > 0:
        print(f"attempts_counter {attempts_counter}")
        results = []
        for url in peer_api_uris:
            try:
                # res = get_all_points(url, COLLECTION_NAME)
                res = get_some_points(url, COLLECTION_NAME, ids)
                results.append(res)
            except Exception as ex:
                print(f"failed with {ex}")
                raise ex

        diffs[f'attempt_{attempts_counter}'] = {}
        is_diff = False
        for node_id, res in enumerate(results[1::]):
            diffs[f'attempt_{attempts_counter}'][f'node_{node_id + 1}'] = []
            # get_all_points
            # for idx, row in enumerate(res['points']):
            #     expected_row = results[0]['points'][idx]
            for idx, row in enumerate(res):
                # compare with node 0
                expected_row = results[0][idx]
                if row != expected_row:
                    diffs[f'attempt_{attempts_counter}'][f'node_{node_id + 1}'].append(f"inconsistency in point {row['id']}: {row['payload']} vs {expected_row['payload']}")
                    is_diff = True
        if is_diff:
            print(f"    There are inconsistencies found, wait for 1 sec and try again")
            time.sleep(1)
            attempts_counter -= 1
        else:
            print(f"    No inconsistencies found, continue to final check")
            break

    # if is_diff:
    #     print("Restart peers")
    #     restart_peers(peer_dirs, None, loop_num=loop_num)
    #     print("Final check after restart peers")
    # else:
    #     print("Final check")
    print("Final check")

    results = []
    for url in peer_api_uris:
        try:
            # res = get_all_points(url, COLLECTION_NAME)
            res = get_some_points(url, COLLECTION_NAME, ids)
            results.append(res)
        except Exception as ex:
            print(f"failed with {ex}")
            raise ex
    diffs['final'] = {}
    is_final_diff = False
    for node_id, res in enumerate(results[1::]):
        diffs['final'][f'node_{node_id + 1}'] = []
        # get_all_points
        # for idx, row in enumerate(res['points']):
        #     expected_row = results[0]['points'][idx]
        for idx, row in enumerate(res):
            expected_row = results[0][idx]
            if row != expected_row:
                diffs['final'][f'node_{node_id + 1}'].append(
                    f"inconsistency in point {row['id']}: {row['payload']} vs {expected_row['payload']}")
                is_final_diff = True

    with open(f'diff_{loop_num}.json', 'w') as diff_log:
        diff_log.write(json.dumps(diffs))

    if is_diff or is_final_diff:
        print("Consistency check FAILED")
        print(f"There are inconsistencies found after {INCONSISTENCY_CHECK_ATTEMPTS} + 1 attempts")
        return False
    else:
        print("Consistency check OK")
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


def check_consistency_2(peer_api_uris, loop_num='final'):
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


def restart_peers(peer_dirs, extra_env, loop_num):
    counter = len(processes) - 1
    processes_info = {}
    while processes:
        p = processes.pop()
        p_http_port = str(p.http_port)
        p_p2p_port = str(p.p2p_port)
        p_grpc_port = str(p.grpc_port)
        processes_info[counter] = {
            "http_port": int(p_http_port),
            "p2p_port": int(p_p2p_port),
            "grpc_port": int(p_grpc_port)
        }
        counter = counter - 1
        print(f"    Kill node http://localhost:{p_http_port}, p2p port: {p_p2p_port}")
        p.kill()

    time.sleep(3)
    (bootstrap_api_uri, bootstrap_uri) = start_first_peer(peer_dirs[0], f"peer_0_restarted_{loop_num}.log",
                                                          port=processes_info[0]["p2p_port"],
                                                          extra_env=extra_env,
                                                          given_grpc_port=processes_info[0]["grpc_port"],
                                                          given_http_port=processes_info[0]["http_port"]
                                                          )
    time.sleep(3)
    # Start other peers
    for i in range(1, len(peer_dirs)):
        start_peer(
            peer_dirs[i], f"peer_{i}_restarted_{loop_num}.log", bootstrap_uri, port=processes_info[i]["p2p_port"], extra_env=extra_env,
            given_grpc_port=processes_info[i]["grpc_port"],
            given_http_port=processes_info[i]["http_port"]
        )
    time.sleep(10)


def test_consistency(tmp_path: pathlib.Path):
    assert_project_root()

    print("\n1. Create a 3-nodes cluster")
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)
    # peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, extra_env={"QDRANT__STORAGE__SHARD_TRANSFER_METHOD": "snapshot"})
    grpc_uris = []
    for item in processes:
        grpc_uris.append(f"http://127.0.0.1:{item.grpc_port}")
    print(peer_dirs)

    print(f"2. Create a collection with {N_SHARDS} shards")
    create_collection(peer_api_uris[0], collection=COLLECTION_NAME, shard_number=N_SHARDS, replication_factor=N_REPLICAS, headers=HEADERS)
    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris, headers=HEADERS)
    # create index just 'cause chaos testing collection has 'em
    create_index(peer_api_uris[0], collection=COLLECTION_NAME, field_name='a', field_schema='keyword', headers=HEADERS)
    create_index(peer_api_uris[0], collection=COLLECTION_NAME, field_name='timestamp', field_schema='datetime', headers=HEADERS)

    print("3. Insert starting points (10k) with payload (timestamp)")
    upsert_random_points_with_payload(peer_api_uris[0], INITIAL_POINTS_COUNT, COLLECTION_NAME, batch_size=100, wait='false')
    print("     Wait for optimizations to complete")
    for uri in peer_api_uris:
        wait_collection_green(uri, COLLECTION_NAME, headers=HEADERS)

    print("4. Start updating points in the background (random nodes)")
    upsert_process = run_update_points_in_background(peer_api_uris, COLLECTION_NAME)
    # upsert_process = run_update_points_in_background_bfb(grpc_uris)
    # upsert_process = run_sweep_points_in_background(peer_api_uris)
    print("     Wait for some time")
    time.sleep(random.randint(5, 10))

    for i in range(LOOPS_OF_REPLICATION):
        for _ in range(1):
            print("5-6. Trigger one of the shards' replication. Wait for the replication to complete")
            replicate_shard(peer_api_uris, COLLECTION_NAME)
            print("     Wait for some time")
            time.sleep(random.randint(3, 7))

        print("     Check consistency after replication")
        print("     Stop updating points")
        upsert_process.kill()
        print("     Wait for some time")
        time.sleep(3)

        # result = check_consistency(peer_api_uris, peer_dirs, loop_num=str(i))
        # if not result:
        #     restart_peers(peer_dirs, None, loop_num=str(i))
        #     check_consistency(peer_api_uris, None, loop_num=f"{i}_again")
        result = check_consistency_2(peer_api_uris, loop_num=str(i))
        if not result:
            print("Final check")
            time.sleep(30)
            final_check = check_consistency_2(peer_api_uris, loop_num='final')

            if not final_check:
                raise AssertionError("FAILED")

            print("OK")
            exit(0)

        print("     Start updating points again")
        upsert_process = run_update_points_in_background(peer_api_uris, COLLECTION_NAME)
        # upsert_process = run_update_points_in_background_bfb(grpc_uris)
        # upsert_process = run_sweep_points_in_background(peer_api_uris)

    print("7. Stop updating points")
    upsert_process.kill()
    print("    Wait for some time")
    time.sleep(30)

    # final_check = check_consistency(peer_api_uris, peer_dirs, loop_num='final')
    print("Final check")
    final_check = check_consistency_2(peer_api_uris, loop_num='final')

    if not final_check:
        raise AssertionError("FAILED")

    print("OK")
