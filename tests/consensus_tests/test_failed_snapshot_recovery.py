import pathlib
import subprocess
import tempfile
import shutil
import requests
from time import sleep

from .fixtures import create_collection, upsert_random_points, random_dense_vector, search
from .utils import *

N_PEERS = 3
N_SHARDS = 1
N_REPLICAS = 3
COLLECTION_NAME = "test_collection"

def create_snapshot(peer_api_uri):
    r = requests.post(f"{peer_api_uri}/collections/{COLLECTION_NAME}/snapshots")
    assert_http_ok(r)
    return r.json()["result"]["name"]


def get_peer_id(peer_api_uri):
    r = requests.get(f"{peer_api_uri}/cluster")
    assert_http_ok(r)
    return r.json()["result"]["peer_id"]


def get_local_shards(peer_api_uri):
    r = requests.get(f"{peer_api_uri}/collections/{COLLECTION_NAME}/cluster")
    assert_http_ok(r)
    return r.json()["result"]['local_shards']


def get_remote_shards(peer_api_uri):
    r = requests.get(f"{peer_api_uri}/collections/{COLLECTION_NAME}/cluster")
    assert_http_ok(r)
    return r.json()["result"]['remote_shards']


def fail_to_recover_snapshot(peer_api_uri, snapshot_url):
    r = requests.put(f"{peer_api_uri}/collections/{COLLECTION_NAME}/snapshots/recover",
                     json={"location": snapshot_url})
    assert r.status_code == 500
    assert "Failed to read segment state" in r.json()["status"]["error"]

def first_segment_name(peer_storage: str, collection_name: str) -> str:
    # get first segment name from storage
    shard_path = f"{peer_storage}/storage/collections/{collection_name}/0/segments"
    segment_name_path = next(filter(lambda x: x.is_dir(), pathlib.Path(shard_path).iterdir()))
    return segment_name_path.name

def shard_initializing_flag(peer_storage: str, collection_name: str, shard_id: int) -> str:
    return f"{peer_storage}/storage/collections/{collection_name}/shard_{shard_id}.initializing"

def corrupt_snapshot(snapshot_path: pathlib.Path, segment_name: str):
    with tempfile.TemporaryDirectory() as temp_dir:
        # Extract the snapshot tar
        subprocess.run(["tar", "--extract", "--sparse", "--file", snapshot_path, "--directory", temp_dir], check=True)

        # Find the inner segment tar file
        segment_archive = f"0/segments/{segment_name}.tar"
        extracted_segment_tar = os.path.join(temp_dir, segment_archive)

        if os.path.exists(extracted_segment_tar):
            # Modify the segment tar to remove the `segment.json` file
            corrupted_inner_tar = extracted_segment_tar + "-corrupted"
            remove_file_from_tar(extracted_segment_tar, "snapshot/files/segment.json", corrupted_inner_tar)

            # Replace the original inner segment tar with the corrupted one
            os.rename(corrupted_inner_tar, extracted_segment_tar)

            # Repack the snapshot tar after modifying the inner segment tar
            new_segment_archive = f"{temp_dir}-corrupted"
            subprocess.run(["tar", "--create", "--sparse", "--file", new_segment_archive, "-C", temp_dir, "."], check=True)

            # Replace the original snapshot tar with the corrupted one
            os.rename(new_segment_archive, snapshot_path)

def corrupt_shard_dir(shard_path: pathlib.Path):
    wal_path = shard_path / "wal"
    shutil.rmtree(wal_path)

def remove_file_from_tar(original_tar, file_to_remove, new_tar):
    file_to_remove = file_to_remove.replace(os.sep, "/")

    with tempfile.TemporaryDirectory() as temp_dir:
        # Extract the inner tar
        subprocess.run(["tar", "--extract", "--sparse", "--file", original_tar, "--directory", temp_dir], check=True)

        # Remove the target file
        file_path = os.path.join(temp_dir, file_to_remove)
        if os.path.exists(file_path):
            os.remove(file_path)
        else:
            assert False, f"File {file_to_remove} not found in {original_tar}"

        # Repack the inner tar
        subprocess.run(["tar", "--create", "--sparse", "--file", new_tar, "-C", temp_dir, "."], check=True)


# The test validates that a node can recover from a corrupted snapshot
def test_corrupted_snapshot_recovery(tmp_path: pathlib.Path):
    assert_project_root()


    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICAS)
    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris)

    wait_for_same_commit(peer_api_uris=peer_api_uris)

    n_points = 3_000
    upsert_random_points(peer_api_uris[0], n_points)

    query_city = "London"

    dense_query_vector = random_dense_vector()
    initial_dense_search_result = search(peer_api_uris[0], dense_query_vector, query_city)
    assert len(initial_dense_search_result) > 0

    snapshot_name = create_snapshot(peer_api_uris[-1])
    assert snapshot_name is not None

    snapshot_path = Path(peer_dirs[-1]) / "snapshots" / COLLECTION_NAME / snapshot_name
    assert snapshot_path.exists()

    # get first segment name from storage
    segment_name = first_segment_name(peer_dirs[-1], COLLECTION_NAME)

    # corrupt snapshot
    corrupt_snapshot(snapshot_path=snapshot_path, segment_name=segment_name)

    # All nodes share the same snapshot directory, so it is fine to use any
    snapshot_url = f"{peer_api_uris[-1]}/collections/{COLLECTION_NAME}/snapshots/{snapshot_name}"

    print(f"Recovering snapshot {snapshot_url} on {peer_api_uris[-1]}")

    # Recover snapshot should fail because the snapshot is corrupted
    fail_to_recover_snapshot(peer_api_uris[-1], snapshot_url)

    # Assert storage contains initialized flag
    flag_path = shard_initializing_flag(peer_dirs[-1], COLLECTION_NAME, 0)
    assert os.path.exists(flag_path)

    # Kill last peer
    p = processes.pop()
    p.kill()
    sleep(1) # Give killed peer time to release WAL lock

    # Restart same peer
    peer_api_uris[-1] = start_peer(peer_dirs[-1], f"peer_{N_PEERS}_restarted.log", bootstrap_uri)

    # Assert the node does not crash when starting with data from corrupted snapshot
    try:
        def is_qdrant_started() -> bool:
            try:
                res = requests.get(f"{peer_api_uris[-1]}/collections")
            except requests.exceptions.ConnectionError:
                return False
            if not res.ok:
                return False
            collections = set(collection['name'] for collection in res.json()["result"]['collections'])
            return COLLECTION_NAME in collections

        wait_for(is_qdrant_started)
    except Exception as e:
        raise Exception(f"Qdrant did not start in time after recovering corrupt snapshot, maybe it crashed: {e}")

    # Assert storage contains initialized flag after restart (this means a dummy replica is loaded)
    flag_path = shard_initializing_flag(peer_dirs[-1], COLLECTION_NAME, 0)
    assert os.path.exists(flag_path)

    # Upsert one point to mark dummy replica as dead, that will trigger recovery transfer
    upsert_random_points(peer_api_uris[-1], 1)

    # Assert storage does not contain initialized flag when transfer is started
    print("Checking that the shard initializing flag was removed after recovery")
    flag_path = shard_initializing_flag(peer_dirs[-1], COLLECTION_NAME, 0)
    try:
        wait_for(lambda : not os.path.exists(flag_path))
    except Exception as e:
        raise Exception(f"Flag {flag_path} still exists after recovery: {e}")

    # There are two other replicas, try moving shards into broken state
    local_shards = get_local_shards(peer_api_uris[0])
    assert len(local_shards) == 1
    assert local_shards[0]["shard_id"] == 0
    assert local_shards[0]["state"] == "Active"
    assert local_shards[0]["points_count"] == n_points

    # Wait for end of shard transfer
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 0)

    # Wait for all replicas to be active on the receiving peer
    wait_for_all_replicas_active(peer_api_uris[-1], COLLECTION_NAME)

    # Assert that the local shard is active and not empty
    local_shards = get_local_shards(peer_api_uris[-1])
    assert len(local_shards) == 1
    assert local_shards[0]["shard_id"] == 0
    assert local_shards[0]["state"] == "Active"
    assert local_shards[0]["points_count"] == n_points

    # Assert that the remote shards are active and not empty
    # The peer used as source for the transfer is used as remote to have at least one
    remote_shards = get_remote_shards(peer_api_uris[-1])
    assert len(remote_shards) == 2
    for shard in remote_shards:
        assert shard["state"] == "Active"

    # Assert that the remote shards are active and not empty
    remote_shards = get_remote_shards(peer_api_uris[0])
    assert len(remote_shards) == 2
    for shard in remote_shards:
        assert shard["state"] == "Active"

    # Check that 'search' returns the same results after recovery
    new_dense_search_result = search(peer_api_uris[-1], dense_query_vector, query_city)
    assert len(new_dense_search_result) == len(initial_dense_search_result)
    for i in range(len(new_dense_search_result)):
        assert new_dense_search_result[i]["id"] == initial_dense_search_result[i]["id"]


@pytest.mark.parametrize("transfer_method", ["snapshot", "stream_records", "wal_delta"])
def test_dirty_shard_handling_with_active_replicas(tmp_path: pathlib.Path, transfer_method: str):
    assert_project_root()
    extra_env = {
        "QDRANT__STORAGE__SHARD_TRANSFER_METHOD": transfer_method,
        "QDRANT__STORAGE__HANDLE_COLLECTION_LOAD_ERRORS": "false"
    }

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(
        tmp_path,
        N_PEERS,
        extra_env=extra_env,
    )

    create_collection(
        peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICAS
    )
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris
    )

    wait_for_same_commit(peer_api_uris=peer_api_uris)

    n_points = 3_000
    upsert_random_points(peer_api_uris[0], n_points)

    query_city = "London"

    dense_query_vector = random_dense_vector()
    initial_dense_search_result = search(
        peer_api_uris[0], dense_query_vector, query_city
    )
    assert len(initial_dense_search_result) > 0

    # Simulate killing on snapshot recovery (corrupting shard dir and adding a shard initializing flag)
    # this can happen in practice when a node is killed while shard directory was being moved from /qdrant/snapshots to /qdrant/storage
    # the initializing flag is created but never deleted in such cases, when Qdrant restarts it considers it as dirty shard and tries to recover it
    shard_id = 0
    flag_path = shard_initializing_flag(peer_dirs[-1], COLLECTION_NAME, shard_id)
    Path(flag_path).touch()

    # Delete some of the files from the shard:
    shard_path = Path(peer_dirs[-1]) / "storage" / "collections" / COLLECTION_NAME / f"{shard_id}"
    assert shard_path.exists()

    corrupt_shard_dir(shard_path)

    # Kill last peer
    p = processes.pop()
    p.kill()
    sleep(1) # Give killed peer time to release WAL lock

    # Restart same peer
    peer_api_uris[-1] = start_peer(
        peer_dirs[-1], f"peer_{N_PEERS}_restarted.log", bootstrap_uri,
        extra_env=extra_env
    )

    # Upsert one point to mark dummy replica as dead, that will trigger recovery transfer
    upsert_random_points(peer_api_uris[0], 1)

    # Wait for start of shard transfer
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 1)

    # Kill again after transfer starts (shard initializing flag has been deleted and shard is empty)
    p = processes.pop()
    p.kill()
    sleep(1) # Give killed peer time to release WAL lock

    # Restart same peer again
    peer_api_uris[-1] = start_peer(
        peer_dirs[-1], f"peer_{N_PEERS}_restarted.log", bootstrap_uri,
        extra_env=extra_env
    )

    wait_for_same_commit(peer_api_uris=peer_api_uris)

    # We expect transfer to be started again if stopped in between because of node crash
    wait_for_collection_shard_transfers_count(peer_api_uris[-1], COLLECTION_NAME, 1)

    # Wait for end of shard transfer
    wait_for_collection_shard_transfers_count(peer_api_uris[-1], COLLECTION_NAME, 0)

    # Wait for all replicas to be active on the receiving peer
    wait_for_all_replicas_active(peer_api_uris[-1], COLLECTION_NAME)

    # Assert that the local shard is active and not empty
    [local_shard] = get_local_shards(peer_api_uris[-1])
    assert local_shard["shard_id"] == 0
    assert local_shard["state"] == "Active"
    assert local_shard["points_count"] == n_points

    assert not os.path.exists(flag_path) # shard initializing flag should be dropped after recovery is successful

    # Assert that the remote shards are active and not empty
    # The peer used as source for the transfer is used as remote to have at least one
    remote_shards = get_remote_shards(peer_api_uris[-1])
    assert len(remote_shards) == 2
    for shard in remote_shards:
        assert shard["state"] == "Active"

    # Assert that the remote shards are active and not empty
    remote_shards = get_remote_shards(peer_api_uris[0])
    assert len(remote_shards) == 2
    for shard in remote_shards:
        assert shard["state"] == "Active"

    # Check that 'search' returns the same results after recovery
    new_dense_search_result = search(peer_api_uris[-1], dense_query_vector, query_city)
    assert len(new_dense_search_result) == len(initial_dense_search_result)
    for i in range(len(new_dense_search_result)):
        assert new_dense_search_result[i]["id"] == initial_dense_search_result[i]["id"]
