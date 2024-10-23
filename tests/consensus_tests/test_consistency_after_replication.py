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
from datetime import datetime, timezone
import random

import builtins
from .fixtures import create_collection, random_dense_vector
from .utils import *

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

INITIAL_POINTS_COUNT = 200000
POINTS_COUNT_UNDER_CHECK = 1000
N_PEERS = 3
N_SHARDS = 2
N_REPLICAS = 1
COLLECTION_NAME = "benchmark"
LOOPS_OF_REPLICATION = 3
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
    limit = 10
    while True:
        # offset = random.randint(0, INITIAL_POINTS_COUNT)
        # experiment 1 on Okt 4, found some inconsistent points but only when continue updates
        offset = 0
        peer_url = random.choice(peer_api_uris)
        upsert_random_points_with_payload(peer_url, num=limit, collection_name=collection_name, offset=offset, wait='true')


def run_update_points_in_background(peer_api_uris, collection_name):
    p = multiprocessing.Process(target=update_points_in_loop, args=(peer_api_uris, collection_name))
    p.start()
    return p


def run_update_points_in_background_bfb(grpc_uris, collection_name):
    os.environ['QDRANT_API_KEY'] = HEADERS.get('api-key', '')
    bfb_cmd_args = (f"--replication-factor {N_REPLICAS} --shards {N_SHARDS} --keywords 10 --timestamp-payload "
                    f"--dim 768 -n 1000000000 --batch-size 100 --threads 1 --parallel 1 --wait-on-upsert "
                    f"--create-if-missing --quantization scalar --timing-threshold 1 --on-disk-vectors true "
                    f"--max-id {INITIAL_POINTS_COUNT} --delay 1000 --timeout 30 --retry 4 --retry-interval 1 "
                    f"--uri {grpc_uris[0]} --collection-name {collection_name}")
    popen_args = ["./bfb/target/debug/bfb", *bfb_cmd_args.split(" ")]
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
    shard_id = collection_cluster_info["local_shards"][0]["shard_id"]
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
                "to_peer_id": target_peer_id
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

    if is_diff:
        print("Restart peers")
        restart_peers(peer_dirs, None, loop_num=loop_num)
        print("Final check after restart peers")
    else:
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


def check(qdrant_uris=None):
    print('level=INFO msg="Starting data consistency check script"')

    QC_NAME = os.getenv("QC_NAME", "qdrant-chaos-testing")


    if QC_NAME == "qdrant-chaos-testing":
        POINTS_DIR = "data/points-dump"
    elif QC_NAME == "qdrant-chaos-testing-debug":
        POINTS_DIR = "data/points-dump-debug"
    elif QC_NAME == "qdrant-chaos-testing-three":
        POINTS_DIR = "data/points-dump-three"
    else:
        raise NotImplementedError(f"Unknown cluster name {QC_NAME}")

    # Ensure the data/points-dump directory exists
    os.makedirs(POINTS_DIR, exist_ok=True)

    # Environment variables with default values if not set
    QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "")
    QDRANT_CLUSTER_URL = os.getenv("QDRANT_CLUSTER_URL", "")
    CONSISTENCY_ATTEMPTS_TOTAL = INCONSISTENCY_CHECK_ATTEMPTS

    is_data_consistent = False
    first_node_points = []
    consistency_attempts_remaining = CONSISTENCY_ATTEMPTS_TOTAL


    def calculate_inconsistent_points(source_points, target_points, point_ids):
        source_point_idx_to_point = {
            point["id"]: (point["vector"], point["payload"]) for point in source_points
        }
        target_point_idx_to_point = {
            point["id"]: (point["vector"], point["payload"]) for point in target_points
        }

        # Mismatching or missing points
        inconsistent_point_ids_by_vector = []
        inconsistent_point_ids_by_payload = []

        for point_id in point_ids:
            source_vector, source_payload = source_point_idx_to_point.get(
                point_id, (None, None)
            )
            target_vector, target_payload = target_point_idx_to_point.get(
                point_id, (None, None)
            )

            if source_vector != target_vector:
                inconsistent_point_ids_by_vector.append(point_id)

            if source_payload != target_payload:
                inconsistent_point_ids_by_payload.append(point_id)

        return (
            (inconsistent_point_ids_by_vector, inconsistent_point_ids_by_payload),
            (source_point_idx_to_point, target_point_idx_to_point)
        )

    def calculate_inconsistent_points_no_vector(source_points, target_points, point_ids):
        source_point_idx_to_point = {
            point["id"]: (point["payload"]) for point in source_points
        }
        target_point_idx_to_point = {
            point["id"]: (point["payload"]) for point in target_points
        }

        # Mismatching or missing points
        inconsistent_point_ids_by_payload = []

        for point_id in point_ids:
            source_payload = source_point_idx_to_point.get(
                point_id, None
            )
            target_payload = target_point_idx_to_point.get(
                point_id, None
            )

            if source_payload != target_payload:
                inconsistent_point_ids_by_payload.append(point_id)

        return (
            inconsistent_point_ids_by_payload,
            (source_point_idx_to_point, target_point_idx_to_point)
        )

    # Generate random numbers between 0 and points_count and convert into JSON array
    # num_points_to_check = POINTS_COUNT_UNDER_CHECK
    # initial_point_ids = random.sample(range(INITIAL_POINTS_COUNT + 1), num_points_to_check)
    # Check all the points
    num_points_to_check = INITIAL_POINTS_COUNT
    initial_point_ids = list(range(num_points_to_check + 1))
    point_ids_for_node = [
        initial_point_ids for _ in range(4)
    ]  # point ids to check for node-0 to node-3

    while True:
        try:
            if qdrant_uris:
                cluster_response = requests.get(
                    f"{qdrant_uris[0]}/cluster",
                    headers={"api-key": QDRANT_API_KEY},
                    timeout=10,
                )
            else:
                cluster_response = requests.get(
                    f"https://{QDRANT_CLUSTER_URL}:6333/cluster",
                    headers={"api-key": QDRANT_API_KEY},
                    timeout=10,
                )
        except requests.exceptions.Timeout as e:
            print(
                f'level=ERROR msg="Request timed out after 10s" uri="{QDRANT_CLUSTER_URL}" api="/cluster"'
            )
            exit(1)

        if cluster_response.status_code != 200:
            print(
                f'level=ERROR msg="Got error in response" status_code={cluster_response.status_code} api="/cluster" response="{cluster_response.text}"'
            )
            exit(1)
        result = cluster_response.json()['result']
        num_peers = len(result["peers"])
        pending_operations = result["raft_info"]["pending_operations"]
        peer_id = result['peer_id']

        if num_peers >= 5 and pending_operations == 0:
            print(f'level=CRITICAL msg="Fetched cluster peers. Found too many peers" num_peers={num_peers} peer_id={peer_id} response={result}')
        else:
            print(f'level=INFO msg="Fetched cluster peers" peer_id={peer_id} num_peers={num_peers}')

        if not qdrant_uris:
            QDRANT_URIS = [
                f"https://node-{idx}-{QDRANT_CLUSTER_URL}:6333" for idx in range(num_peers)
            ]
        else:
            QDRANT_URIS = qdrant_uris

        node_idx = 0
        first_node_points = []

        for uri in QDRANT_URIS:
            if node_idx >= len(point_ids_for_node):
                print(
                    f'level=CRITICAL msg="Unexpected node index found. Breaking loop" node_idx={node_idx}'
                )
                break

            point_ids = point_ids_for_node[node_idx]

            if len(point_ids) == 0:
                is_data_consistent = True
                print(
                    f'level=INFO msg="Skipping because no check required for node" node={node_idx}'
                )
                node_idx += 1
                continue

            try:
                response = requests.post(
                    f"{uri}/collections/benchmark/points",
                    headers={"api-key": QDRANT_API_KEY, "content-type": "application/json"},
                    # json={"ids": point_ids, "with_vector": True, "with_payload": True},
                    json={"ids": point_ids, "with_vector": False, "with_payload": True},
                    timeout=30,
                )
            except requests.exceptions.Timeout as e:
                print(
                    f'level=WARN msg="Request timed out after 10s, skipping consistency check for node" uri="{uri}" api="/collections/benchmark/points"'
                )
                node_idx += 1
                continue

            if response.status_code != 200:
                error_msg = response.text.strip()
                if error_msg in ("404 page not found", "Service Unavailable"):
                    print(
                        f'level=WARN msg="Node unreachable, skipping consistency check" uri="{uri}" status_code={response.status_code} err="{error_msg}"'
                    )
                    # point_ids_for_node[node_idx] = []
                    node_idx += 1
                    continue
                else:
                    # Some unknown error:
                    print(
                        f'level=ERROR msg="Failed to fetch points" uri="{uri}" status_code={response.status_code} err="{error_msg}"'
                    )
                    is_data_consistent = False
                    break

            fetched_points = sorted(response.json()["result"], key=lambda x: x["id"])
            fetched_points_count = len(fetched_points)

            print(
                f'level=INFO msg="Fetched points" num_points={fetched_points_count} uri="{uri}"'
            )

            attempt_number = CONSISTENCY_ATTEMPTS_TOTAL - consistency_attempts_remaining
            with open(
                f"{POINTS_DIR}/node-{node_idx}-attempt-{attempt_number}.json", "w"
            ) as f:
                json.dump(fetched_points, f)

            if len(first_node_points) == 0:
                first_node_points = fetched_points
            elif fetched_points == first_node_points:
                print(f'level=INFO msg="Node {node_idx} is consistent with node-0" uri="{uri}"')
                point_ids_for_node[node_idx] = []
                is_data_consistent = True
            else:
                print(f'level=INFO msg="Checking points of node" uri="{uri}"')
                (inconsistent_ids_by_vector, inconsistent_ids_by_payload), (first_node_points_map, fetched_node_points_map)  = (
                    calculate_inconsistent_points(
                        first_node_points, fetched_points, point_ids
                    )
                )
                if (
                    len(inconsistent_ids_by_vector) == 0
                    and len(inconsistent_ids_by_payload) == 0
                ):
                    print(f'level=INFO msg="Node {node_idx} is consistent" uri="{uri}"')
                    point_ids_for_node[node_idx] = []
                    is_data_consistent = True
                    continue
                inconsistent_point_ids = list(
                    set(inconsistent_ids_by_vector).union(inconsistent_ids_by_payload)
                )
                print(
                    f'level=WARN msg="Node {node_idx} might be inconsistent compared to node-0. Need to retry" uri="{uri}" inconsistent_count={len(inconsistent_point_ids)} inconsistent_by_vector="{inconsistent_ids_by_vector}" inconsistent_by_payload="{inconsistent_ids_by_payload}" inconsistent_points="{inconsistent_point_ids}"'
                )

                point_ids_for_node[node_idx] = inconsistent_point_ids

                is_data_consistent = False
                break

            node_idx += 1

        consistency_attempts_remaining -= 1

        if is_data_consistent:
            print(
                f'level=INFO msg="Data consistency check succeeded" attempts={CONSISTENCY_ATTEMPTS_TOTAL - consistency_attempts_remaining}'
            )
            break
        else:
            if consistency_attempts_remaining == 0:
                try:
                    print(
                        f'level=ERROR msg="Data consistency check failed" attempts={CONSISTENCY_ATTEMPTS_TOTAL - consistency_attempts_remaining} inconsistent_count={len(inconsistent_point_ids)} inconsistent_by_vector="{inconsistent_ids_by_vector}" inconsistent_by_payload="{inconsistent_ids_by_payload}" inconsistent_points="{inconsistent_point_ids}"'
                    )
                    first_node_inconsistent_points = []
                    last_fetched_node_inconsistent_points = []

                    for point_id in inconsistent_point_ids:
                        first_node_inconsistent_points.append(first_node_points_map[point_id])
                        last_fetched_node_inconsistent_points.append(fetched_node_points_map[point_id])

                    print(f'level=ERROR msg="Dumping inconsistent points compared to node-0" node="node-{node_idx}" expected_points="{first_node_inconsistent_points}" fetched_points={last_fetched_node_inconsistent_points}')

                except Exception as e:
                    print(f'level=ERROR msg="Failed while printing inconsistent points" err={e}')

                break
            else:
                print(
                    f'level=WARN msg="Retrying data consistency check" attempts={CONSISTENCY_ATTEMPTS_TOTAL - consistency_attempts_remaining} remaining_attempts={consistency_attempts_remaining}'
                )
                # Node might be unavailable which caused request to fail. Give some time to heal
                time.sleep(5)
                first_node_points = []
                continue

    if not is_data_consistent:
        return False
    else:
        return True


def test_consistency(tmp_path: pathlib.Path):
    assert_project_root()

    # print("\n1. Create a 3-nodes cluster")
    # peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)
    # # peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, extra_env={"QDRANT__STORAGE__SHARD_TRANSFER_METHOD": "stream_records"})
    # grpc_uris = []
    # for item in processes:
    #     grpc_uris.append(f"http://127.0.0.1:{item.grpc_port}")
    # print(peer_dirs)
    #
    # print(f"2. Create a collection with {N_SHARDS} shards")
    # create_collection(peer_api_uris[0], collection=COLLECTION_NAME, shard_number=N_SHARDS, replication_factor=N_REPLICAS, headers=HEADERS)
    # wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris, headers=HEADERS)
    # # create index just 'cause chaos testing collection has 'em
    # create_index(peer_api_uris[0], collection=COLLECTION_NAME, field_name='a', field_schema='keyword', headers=HEADERS)
    # create_index(peer_api_uris[0], collection=COLLECTION_NAME, field_name='timestamp', field_schema='datetime', headers=HEADERS)
    #
    # print("3. Insert starting points (10k) with payload (timestamp)")
    # upsert_random_points_with_payload(peer_api_uris[0], INITIAL_POINTS_COUNT, COLLECTION_NAME, batch_size=100, wait='false')
    # print("     Wait for optimizations to complete")
    # for uri in peer_api_uris:
    #     wait_collection_green(uri, COLLECTION_NAME, headers=HEADERS)
    #
    # print("4. Start updating points in the background (random nodes)")
    # # upsert_process = run_update_points_in_background(peer_api_uris, COLLECTION_NAME)
    # upsert_process = run_update_points_in_background_bfb(grpc_uris, COLLECTION_NAME)
    # print("     Wait for some time")
    # time.sleep(5)
    #
    # for i in range(LOOPS_OF_REPLICATION):
    #     print("5-6. Trigger one of the shards' replication. Wait for the replication to complete")
    #     replicate_shard(peer_api_uris, COLLECTION_NAME)
    #     print("     Wait for some time")
    #     time.sleep(random.randint(3, 15))
    #
    #     # random_bool = random.choice([True, False])
    #     # check consistency after each replication
    #     random_bool = True
    #     if random_bool:
    #         print("     Stop updating points")
    #         upsert_process.kill()
    #         print("         Wait for some time")
    #         time.sleep(3)
    #
    #         # result = check_consistency(peer_api_uris, peer_dirs, loop_num=str(i))
    #         result = check(INITIAL_POINTS_COUNT,peer_api_uris, INCONSISTENCY_CHECK_ATTEMPTS, POINTS_COUNT_UNDER_CHECK)
    #         if not result:
    #             restart_peers(peer_dirs, None, loop_num=str(i))
    #             # check_consistency(peer_api_uris, loop_num=f"{i}_again")
    #         print("     Start updating points again")
    #         # upsert_process = run_update_points_in_background(peer_api_uris, COLLECTION_NAME)
    #         upsert_process = run_update_points_in_background_bfb(grpc_uris, COLLECTION_NAME)
    #
    # print("7. Stop updating points")
    # upsert_process.kill()
    # print("    Wait for some time")
    # time.sleep(30)

    # final_check = check_consistency(peer_api_uris, peer_dirs, loop_num='final')
    final_check = check(None)

    if not final_check:
        raise AssertionError("FAILED")

    print("OK")
