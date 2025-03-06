import pathlib
from time import sleep
from typing import Any, Literal

from .assertions import assert_hw_measurements_equal_many
from .fixtures import upsert_random_points, create_collection, get_telemetry_hw_info
from .utils import *


COLLECTION_NAME = "test_collection"


def test_resharding_state_transitions(tmp_path: pathlib.Path):
    """
    Tests allowed state transitions during resharding
    """

    # Bootstrap resharding cluster
    peer_uris, _ = bootstrap_resharding(tmp_path)

    # Check that we can't (re)apply unexpected resharding state transitions
    try_requests(peer_uris[0], 400, [
        start_resharding,
        lambda peer_uri: start_resharding(peer_uri, direction="down"),
        commit_write_hashring,
        finish_resharding,
    ])

    # Commit read hashring
    resp = commit_read_hashring(peer_uris[0])
    assert_http_ok(resp)

    # Check that we can't (re)apply unexpected resharding state transitions or abort resharding
    try_requests(peer_uris[0], 400, [
        start_resharding,
        lambda peer_uri: start_resharding(peer_uri, direction="down"),
        commit_read_hashring,
        finish_resharding,
        abort_resharding,
    ])

    # Commit write hashring
    resp = commit_write_hashring(peer_uris[0])
    assert_http_ok(resp)

    # Check that we can't (re)apply unexpected resharding state transitions or abort resharding
    try_requests(peer_uris[0], 400, [
        start_resharding,
        lambda peer_uri: start_resharding(peer_uri, direction="down"),
        commit_read_hashring,
        commit_write_hashring,
        abort_resharding,
    ])

    # Finish resharding
    resp = finish_resharding(peer_uris[0])
    assert_http_ok(resp)

    # Wait for resharding to finish
    wait_for_collection_resharding_operations_count(peer_uris[0], COLLECTION_NAME, 0)

def test_resharding_abort(tmp_path: pathlib.Path):
    """
    Tests that resharding can be aborted
    """

    # Bootstrap resharding cluster
    peer_uris, _ = bootstrap_resharding(tmp_path)

    # Abort resharding
    resp = abort_resharding(peer_uris[0])
    assert_http_ok(resp)

    # Wait for resharding to abort
    wait_for_collection_resharding_operations_count(peer_uris[0], COLLECTION_NAME, 0)

def test_resharding_abort_with_replicas(tmp_path: pathlib.Path):
    """
    Tests that resharding can be aborted after replication
    Covers bug <https://github.com/qdrant/qdrant/pull/5792>.
    """

    # Bootstrap resharding cluster
    peer_uris, peer_ids = bootstrap_resharding(
        tmp_path,
        shard_number=1,
        replication_factor=3,
        peers=3,
        upsert_points=1000,
    )

    # Migrate resharding points from shard 0 into 1
    migrate_points(peer_uris[0], peer_ids[0], 0, peer_ids[0], 1, "up")
    activate_replica(peer_uris[0], peer_ids[0], 1)

    # Replicate new shard twice to match replication factor
    replicate_shard(peer_uris[0], peer_ids[0], peer_ids[1], 1)
    replicate_shard(peer_uris[0], peer_ids[0], peer_ids[2], 1)

    # Assert that resharding is still in progress
    info = get_collection_cluster_info(peer_uris[0], COLLECTION_NAME)
    assert "resharding_operations" in info and len(info["resharding_operations"]) == 1

    # Abort resharding
    resp = abort_resharding(peer_uris[0])
    assert_http_ok(resp)

    # Wait for resharding to abort
    wait_for_collection_resharding_operations_count(peer_uris[0], COLLECTION_NAME, 0)

def test_resharding_abort_on_delete_collection(tmp_path: pathlib.Path):
    """
    Testa that resharding is automatically aborted, when collection is deleted
    """

    # Bootstrap resharding cluster
    peer_uris, peer_ids = bootstrap_resharding(tmp_path, peer_idx=-1)

    # Remove collection
    resp = requests.delete(f"{peer_uris[0]}/collections/{COLLECTION_NAME}")
    assert_http_ok(resp)

    # TODO: Wait for/check... *what*? 🤔

def test_resharding_abort_on_delete_shard_key(tmp_path: pathlib.Path):
    """
    Tests that resharding is automatically aborted, when custom shard key is deleted
    """

    # Bootstrap resharding cluster
    peer_uris, peer_ids = bootstrap_resharding(
        tmp_path,
        shard_keys=["custom_shard_key_1", "custom_shard_key_2"],
        shard_key="custom_shard_key_2",
    )

    # Delete shard key
    resp = requests.post(f"{peer_uris[0]}/collections/{COLLECTION_NAME}/shards/delete", json={
        "shard_key": "custom_shard_key_2",
    })

    assert_http_ok(resp)

    # Wait for resharding to abort
    wait_for_collection_resharding_operations_count(peer_uris[0], COLLECTION_NAME, 0)

def test_resharding_abort_on_remove_peer(tmp_path: pathlib.Path):
    """
    Tests that resharding is automatically aborted, when we force-remove resharding peer
    """

    # Bootstrap resharding cluster
    peer_uris, peer_ids = bootstrap_resharding(tmp_path, peer_idx=-1)

    # Remove target peer
    resp = requests.delete(f"{peer_uris[0]}/cluster/peer/{peer_ids[-1]}?force=true")
    assert_http_ok(resp)

    # Wait for resharding to abort
    wait_for_collection_resharding_operations_count(peer_uris[0], COLLECTION_NAME, 0)

def test_resharding_try_remove_target_shard(tmp_path: pathlib.Path):
    """
    Tests that new shard can't be removed during resharding (until it has been replicated)
    """

    # Bootstrap resharding cluster
    peer_uris, peer_ids = bootstrap_resharding(tmp_path)

    # Try to remove target shard
    resp = requests.post(f"{peer_uris[0]}/collections/{COLLECTION_NAME}/cluster", json={
        "drop_replica": {
            "peer_id": peer_ids[0],
            "shard_id": 3,
        }
    })

    assert_http(resp, 400)

@pytest.mark.parametrize("direction, peers", [("up", 3), ("down", 3)])
def test_resharding_forward(tmp_path: pathlib.Path, direction: Literal["up", "down"], peers: int):
    """
    Tests that updates are forwarded to target replicas
    """

    # Bootstrap resharding cluster
    peer_uris, peer_ids = bootstrap_resharding(tmp_path, direction=direction, peers=peers)

    # Upsert points to collection
    upsert_random_points(peer_uris[0], 1000, collection_name=COLLECTION_NAME)

    # Get collection cluster info
    info = get_collection_cluster_info(peer_uris[0], COLLECTION_NAME)

    # Select target replica
    target_shard_id = peers if direction == "up" else peers - 1
    target_peer_id, target_peer_uri = find_replica(target_shard_id, info, peer_uris, peer_ids)

    # Create a list to collect replica URIs selected during test
    replica_uris = []

    for shard_id in range(target_shard_id):
        # Find replicas of selected shard
        peer_id, peer_uri = find_replica(shard_id, info, peer_uris, peer_ids)

        # Assert that all points were correctly forwarded
        assert_resharding_points(peer_uri, shard_id, target_peer_uri, target_shard_id)

        # Append peer URI to the list of replica URIs
        replica_uris.append(peer_uri)

    # Append target replica to the list of replica URIs
    replica_uris.append(target_peer_uri)

    # Assert total count of migrated points
    assert_resharding_points_count(replica_uris)

@pytest.mark.parametrize("direction, peers", [("up", 3), ("down", 3)])
def test_resharding_transfer(tmp_path: pathlib.Path, direction: Literal["up", "down"], peers: int):
    """
    Tests that resharding transfers migrate points to target replicas
    """

    # Bootstrap resharding cluster
    peer_uris, peer_ids = bootstrap_resharding(tmp_path, upsert_points=1000, direction=direction, peers=peers)

    # Get collection cluster info
    info = get_collection_cluster_info(peer_uris[0], COLLECTION_NAME)

    # Select target replica
    target_shard_id = peers if direction == "up" else peers - 1
    target_peer_id, target_peer_uri = find_replica(target_shard_id, info, peer_uris, peer_ids)

    # Create a list to collect replica URIs selected during test
    replica_uris = []

    for shard_id in range(target_shard_id):
        # Find replica of selected shard
        peer_id, peer_uri = find_replica(shard_id, info, peer_uris, peer_ids)

        # Collect all nodes hardware measurements before transferring
        hw = [get_telemetry_hw_info(uri, COLLECTION_NAME) for uri in peer_uris]

        # Migrate resharding points
        migrate_points(peer_uris[0], peer_id, shard_id, target_peer_id, target_shard_id, direction)

        # Assert that no hardware measurements have been measured for the transfer
        new_hw = [get_telemetry_hw_info(uri, COLLECTION_NAME) for uri in peer_uris]
        assert_hw_measurements_equal_many(hw, new_hw)

        # Assert that all points were correctly migrated
        assert_resharding_points(peer_uri, shard_id, target_peer_uri, target_shard_id)

        # Append peer URI to the list of replica URIs
        replica_uris.append(peer_uri)

    # Append target replica to the list of replica URIs
    replica_uris.append(target_peer_uri)

    # Assert total count of migrated points
    assert_resharding_points_count(replica_uris)

@pytest.mark.parametrize("peers", [(3)])
def test_resharding_down_abort_cleanup(tmp_path: pathlib.Path, peers: int):
    """
    Tests that migrated points are cleaned up from target replicas, when scale-down resharding is aborted
    """

    # Bootstrap resharding cluster
    peer_uris, peer_ids = bootstrap_resharding(tmp_path, upsert_points=500, direction="down", peers=peers)

    # Upsert points to collection
    upsert_random_points(peer_uris[0], 500, collection_name=COLLECTION_NAME)

    # Get collection cluster info
    info = get_collection_cluster_info(peer_uris[0], COLLECTION_NAME)

    # Select target replica
    target_shard_id = peers - 1
    target_peer_id, _ = find_replica(target_shard_id, info, peer_uris, peer_ids)

    # Create a list to collect replica URIs selected during test
    replica_uris = []

    for shard_id in range(target_shard_id):
        # Find replica of selected shard
        peer_id, peer_uri = find_replica(shard_id, info, peer_uris, peer_ids)

        # Migrate resharding points
        migrate_points(peer_uris[0], peer_id, shard_id, target_peer_id, target_shard_id, "down")

        # Assert that some points were forwarded and/or migrated to selected replica
        resharding_points_count = count_local_points(peer_uri, shard_id, target_shard_id, exact=True)
        assert resharding_points_count > 0

        # Append peer URI to the list of replica URIs
        replica_uris.append(peer_uri)

    # Abort resharding
    resp = abort_resharding(peer_uris[0])
    assert_http_ok(resp)

    # Wait for resharding to abort
    wait_for_collection_resharding_operations_count(peer_uris[0], COLLECTION_NAME, 0)

    # Assert that all replicas are in `Active` state
    info = get_collection_cluster_info(peer_uris[0], COLLECTION_NAME)

    for replica in all_replicas(info):
        assert replica["state"] == "Active"

    # Assert that forwarded and/or migrated points were deleted from non-target replicas
    for shard_id, peer_uri in enumerate(replica_uris):
        resharding_points_count = count_local_points(peer_uri, shard_id, target_shard_id, exact=True)
        assert resharding_points_count == 0


def bootstrap_resharding(
    tmp_path: pathlib.Path,
    collection: str = COLLECTION_NAME,
    peer_idx: int | None = None,
    **kwargs,
):
    # Bootstrap cluster
    peer_uris, peer_ids = bootstrap_cluster(tmp_path, collection, **kwargs)

    # Select target peer
    peer_id = None

    if peer_idx:
        try:
            peer_id = peer_ids[peer_idx]
        finally:
            pass

    # Start resharding
    resp = start_resharding(peer_uris[0], collection, peer_id=peer_id, **kwargs)
    assert_http_ok(resp)

    # Wait for resharding to start
    wait_for_collection_resharding_operations_count(peer_uris[0], collection, 1)

    return (peer_uris, peer_ids)

def bootstrap_cluster(
    tmp_path: pathlib.Path,
    collection: str = COLLECTION_NAME,
    shard_number: int = 3,
    replication_factor: int = 2,
    shard_keys: list[str] | str | None = None,
    upsert_points: int = 0,
    peers: int = 3,
    **kwargs,
) -> tuple[list[str], list[int]]:
    assert_project_root()

    # Prevent optimizers messing with point counts
    env = {
        "QDRANT__STORAGE__OPTIMIZERS__INDEXING_THRESHOLD_KB": "0",
    }

    # Start cluster
    peer_uris, _, _ = start_cluster(tmp_path, peers, extra_env=env)

    # Collect peer IDs
    peer_ids = []
    for peer_uri in peer_uris:
        peer_ids.append(get_cluster_info(peer_uri)["peer_id"])

    # Create collection
    create_collection(
        peer_uris[0],
        collection,
        shard_number,
        replication_factor,
        sharding_method="auto" if shard_keys is None else "custom",
    )

    wait_collection_exists_and_active_on_all_peers(collection, peer_uris)

    # Create custom shard keys (if required), and upload points to collection
    if type(shard_keys) is not list:
        shard_keys: list[str | None] = [shard_keys]

    for shard_key in shard_keys:
        # Create custom shard key (if required)
        if shard_key is not None:
            resp = requests.put(f"{peer_uris[0]}/collections/{collection}/shards", json={
                "shard_key": shard_key,
                "shards_number": shard_number,
                "replication_factor": replication_factor,
            })

            assert_http_ok(resp)

        # Upsert points to collection
        if upsert_points > 0:
            upsert_random_points(
                peer_uris[0],
                upsert_points,
                collection_name=collection,
                shard_key=shard_key,
            )

    return (peer_uris, peer_ids)


def start_resharding(
    peer_uri: str,
    collection: str = COLLECTION_NAME,
    direction: Literal["up", "down"] = "up",
    peer_id: int | None = None,
    shard_key: str | None = None,
    **kwargs,
):
    return requests.post(f"{peer_uri}/collections/{collection}/cluster", json={
        "start_resharding": {
            "direction": direction,
            "peer_id": peer_id,
            "shard_key": shard_key,
        }
    })

def commit_read_hashring(peer_uri: str, collection: str = COLLECTION_NAME):
    return requests.post(f"{peer_uri}/collections/{collection}/cluster", json={
        "commit_read_hash_ring": {}
    })

def commit_write_hashring(peer_uri: str, collection: str = COLLECTION_NAME):
    return requests.post(f"{peer_uri}/collections/{collection}/cluster", json={
        "commit_write_hash_ring": {}
    })

def finish_resharding(peer_uri: str, collection: str = COLLECTION_NAME):
    return requests.post(f"{peer_uri}/collections/{collection}/cluster", json={
        "finish_resharding": {}
    })

def abort_resharding(peer_uri: str, collection: str = COLLECTION_NAME):
    return requests.post(f"{peer_uri}/collections/{collection}/cluster", json={
        "abort_resharding": {}
    })


def get_local_points(
    peer_uri: str,
    shard_id: int,
    point_ids: list[int],
    collection: str = COLLECTION_NAME,
):
    resp = requests.post(f"{peer_uri}/collections/{collection}/shards/{shard_id}/points", json = {
        "ids": point_ids,
        "with_vector": True,
        "with_payload": True,
    })

    assert_http_ok(resp)
    return resp.json()['result']

def scroll_local_points(
    peer_uri: str,
    shard_id: int,
    filter_shard_id: int | None = None,
    offset: int | None = None,
    limit: int | None = None,
    collection: str = COLLECTION_NAME,
):
    resp = requests.post(f"{peer_uri}/collections/{collection}/shards/{shard_id}/points/scroll", json={
        "limit": limit,
        "offset": offset,
        "hash_ring_filter": None if filter_shard_id is None else {
            "expected_shard_id": filter_shard_id,
        },
        "with_vector": True,
        "with_payload": True,
    })

    assert_http_ok(resp)
    return resp.json()['result']

def count_local_points(
    peer_uri: str,
    shard_id: int,
    filter_shard_id: int | None = None,
    exact: bool = True,
    collection: str = COLLECTION_NAME,
) -> int:
    resp = requests.post(f"{peer_uri}/collections/{collection}/shards/{shard_id}/points/count", json={
        "exact": exact,
        "hash_ring_filter": None if filter_shard_id is None else {
            "expected_shard_id": filter_shard_id,
        },
    })

    assert_http_ok(resp)
    return resp.json()['result']['count']


def migrate_points(
    peer_uri: str,
    peer_id: int,
    shard_id: int,
    target_peer_id: int,
    target_shard_id: int,
    direction: Literal["up", "down"],
    collection: str = COLLECTION_NAME,
    info: dict[str, Any] | None = None,
) -> tuple[int, int]:
    """
    Migrates resharding points to or from `shard_id`, depending on resharding `direction`:
    - if `direction` is "up", migrates points from `shard_id` to target shard
    - if `direction` is "down", migrates points from target shard to `shard_id`
    """

    # Select replicas for resharding transfer
    if direction == "up":
        from_shard_id = shard_id
        from_peer_id = peer_id
        to_shard_id = target_shard_id
        to_peer_id = target_peer_id
    else:
        from_shard_id = target_shard_id
        from_peer_id = target_peer_id
        to_shard_id = shard_id
        to_peer_id = peer_id

    # Start resharding transfer
    resp = requests.post(f"{peer_uri}/collections/{collection}/cluster", json={
        "replicate_shard": {
            "from_peer_id": from_peer_id,
            "to_peer_id": to_peer_id,
            "shard_id": from_shard_id,
            "to_shard_id": to_shard_id,
            "method": "resharding_stream_records",
        }
    })

    assert_http_ok(resp)

    # Wait for resharding transfer to start
    sleep(1)

    # Wait for resharding transfer to finish or abort
    wait_for_collection_shard_transfers_count(peer_uri, collection, 0)

    # Assert that resharding transfer finished successfully
    info = get_collection_cluster_info(peer_uri, collection)

    # Assert that resharding is still in progress
    assert "resharding_operations" in info and len(info["resharding_operations"]) > 0

    # Assert that replica `to_shard_id`@`to_peer_id` is in `Resharding` state
    migration_successful = False

    for replica in all_replicas(info):
        if replica["shard_id"] == to_shard_id and replica["peer_id"] == to_peer_id and replica["state"] in ("Resharding", "ReshardingScaleDown"):
            migration_successful = True
            break

    assert migration_successful

    # Return replicas used for resharding transfer
    return (peer_id, target_peer_id)

def activate_replica(
    peer_uri: str,
    peer_id: int,
    shard_id: int,
    collection: str = COLLECTION_NAME,
):
    """
    Activate a resharding replica that as been migrated into.
    """

    # Activate migrated replica
    resp = requests.post(f"{peer_uri}/collections/{collection}/cluster", json={
        "finish_migrating_points": {
            "peer_id": peer_id,
            "shard_id": shard_id,
        }
    })

    assert_http_ok(resp)

    # Wait for replica activation
    sleep(1)

    # Assert that resharding transfer finished successfully
    info = get_collection_cluster_info(peer_uri, collection)

    # Assert that resharding is still in progress
    assert "resharding_operations" in info and len(info["resharding_operations"]) > 0

    # Assert that replica `to_shard_id`@`to_peer_id` is in `Active` state
    migration_successful = False

    for replica in all_replicas(info):
        if replica["shard_id"] == shard_id and replica["peer_id"] == peer_id and replica["state"] in ("Active"):
            migration_successful = True
            break

    assert migration_successful

def replicate_shard(
    peer_uri: str,
    from_peer_id: int,
    to_peer_id: int,
    shard_id: int,
    collection: str = COLLECTION_NAME,
):
    """
    Replicate a shard from `from_peer_id` to `to_peer_id`
    """

    # Start resharding transfer
    resp = requests.post(f"{peer_uri}/collections/{collection}/cluster", json={
        "replicate_shard": {
            "from_peer_id": from_peer_id,
            "to_peer_id": to_peer_id,
            "shard_id": shard_id,
            "method": "stream_records",
        }
    })

    assert_http_ok(resp)

    # Wait for resharding transfer to start
    sleep(1)

    # Wait for resharding transfer to finish or abort
    wait_for_collection_shard_transfers_count(peer_uri, collection, 0)

def assert_resharding_points(peer_uri: str, shard_id: int, target_peer_uri: str, target_shard_id: int):
    """
    Asserts that target replica contains all resharding points from selected replica
    and these points are exactly the same between replicas
    """

    offset = 0

    while offset is not None:
        # Scroll resharding points in selected replica
        resp = scroll_local_points(peer_uri, shard_id, target_shard_id, offset, 1000)

        # Fetch the same points from target replica
        target_resp = get_local_points(target_peer_uri, target_shard_id, [point['id'] for point in resp['points']])

        # Assert that target replica contains all resharding points from selected replica
        # and these points are exactly the same between replicas
        assert target_resp == resp['points']

        offset = resp['next_page_offset']

def assert_resharding_points_count(replica_uris: list[str]):
    """
    Asserts that target replica points count matches total resharding points count in all other replicas
    """

    # Select target replica
    target_shard_id = len(replica_uris) - 1
    target_shard_uri = replica_uris[-1]

    # Get points count in target replica
    target_points_count = count_local_points(target_shard_uri, target_shard_id, exact=True)

    # Calculate total resharding points count in all other replicas
    total_resharding_points_count = 0

    for shard_id, shard_uri in enumerate(replica_uris[:-1]):
        total_resharding_points_count += count_local_points(shard_uri, shard_id, target_shard_id, exact=True)

    # Assert target replica points count matches total resharding points count
    assert target_points_count == total_resharding_points_count

def find_replica(shard_id: int, info: dict[str, Any], peer_uris: list[str], peer_ids: list[int]) -> tuple[int, str]:
    for replica in all_replicas(info):
        if replica["shard_id"] == shard_id:
            peer_id = replica["peer_id"]
            peer_uri = peer_uris[peer_ids.index(peer_id)]
            return (peer_id, peer_uri)

    raise Exception(f"replica of shard {shard_id} not found: {info}")

def all_replicas(info: dict[str, Any]):
    for local in info["local_shards"]:
        local["peer_id"] = info["peer_id"]
        yield local

    for remote in info["remote_shards"]:
        yield remote

def try_requests(
    peer_uri: str,
    expected_status: int,
    reqs: list[Callable[[str], requests.Response]],
):
    """
    Execute multiple requests and asserts that all requests return expected status code
    """

    for req in reqs:
        resp = req(peer_uri)
        assert_http(resp, expected_status)

def assert_http(resp: requests.Response, expected_status: int):
    assert resp.status_code == expected_status, (
        f"`{resp.url}` "
        f"returned an unexpected status code (expected {expected_status}, received {resp.status_code}):\n"
        f"{resp.json()}"
    )
