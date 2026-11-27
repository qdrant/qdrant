"""
Test how a Dead replica picks the source of its automatic WAL delta transfer.

The recovering replica asks the active replicas for their recovery points and WAL delta bounds,
ranks them, and requests the transfer from the best one. A replica that has every operation of the
recovering replica ranks above one that has to repair its orphan operations.
"""

import pathlib
from threading import Thread

from .fixtures import create_collection, upsert_random_points
from .test_shard_wal_delta_orphan_operation import (
    AHEAD_ON_OWN_CLOCKS_LOG,
    COLLECTION_NAME,
    FALLBACK_LOG,
    check_replica_state,
    has_point,
)
from .utils import *

# RPC that sends an upsert to a remote replica
POINTS_UPSERT_RPC = "/qdrant.PointsInternal/Upsert"

# Point that R's last update upserts, which reaches C but not A
PARTIAL_POINT_ID = 1_000_000

# Right after a restart, a peer may not know the versions of all peers yet, and then picks the
# default transfer method instead of WAL delta for automatic recovery
WAL_DELTA_TRANSFER_ENV = {
    "QDRANT__STORAGE__SHARD_TRANSFER_METHOD": "wal_delta",
}


def upsert_point_ignoring_errors(peer_uri: str, point_id: int):
    try:
        upsert_random_points(peer_uri, 1, offset=point_id, fail_on_error=False)
    except requests.exceptions.RequestException:
        pass


def test_wal_delta_source_with_partial_update(tmp_path: pathlib.Path):
    """R's last update reached C but not A, when R died.

    For R, A misses an operation on R's own clock, so A would repair it as an orphan operation and
    erase it. C has it. R must recover from C with a WAL delta, and keep the update. A still misses
    it.
    """
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, 3, use_peer_proxy=True)
    a_uri, c_uri, r_uri = peer_api_uris
    a_process = processes[0]

    c_peer_id = get_cluster_info(c_uri)["peer_id"]
    r_peer_id = get_cluster_info(r_uri)["peer_id"]

    create_collection(a_uri, shard_number=1, replication_factor=3)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris,
    )

    # Upsert through R, so every replica knows R's clock
    upsert_random_points(r_uri, 100)

    # Upsert a point through R, and hold its update to A at A's proxy
    with a_process.proxy.hold_rpc(POINTS_UPSERT_RPC) as gate:
        upsert = Thread(target=upsert_point_ignoring_errors, args=(r_uri, PARTIAL_POINT_ID), daemon=True)
        upsert.start()

        gate.wait_for_request()
        wait_for(has_point, c_uri, PARTIAL_POINT_ID)

        # Kill R. Its held update to A is cancelled, so A never receives it
        r_process = processes.pop()
        r_port = r_process.p2p_port
        r_process.kill()

        wait_for(gate.cancelled.is_set)

    upsert.join()

    # Upsert through A. A fails to reach R and marks it as Dead
    upsert_random_points(a_uri, 10, offset=1000)
    wait_for(check_replica_state, a_uri, r_peer_id, "Dead")

    assert not has_point(a_uri, PARTIAL_POINT_ID)

    # Restart R, it recovers its replica automatically
    r_uri = start_peer(
        peer_dirs[-1], "peer_0_2_restarted.log", bootstrap_uri, port=r_port, extra_env=WAL_DELTA_TRANSFER_ENV,
    )
    peer_api_uris[-1] = r_uri
    wait_for_peer_online(r_uri, "/")
    wait_for(check_replica_state, a_uri, r_peer_id, "Active")
    wait_for_collection_shard_transfers_count(a_uri, COLLECTION_NAME, 0)
    wait_for_all_replicas_active(a_uri, COLLECTION_NAME)

    log_folder = pathlib.Path(init_pytest_log_folder())

    # R ranked C first and requested a WAL delta from it, which C resolved without falling back
    r_log_text = (log_folder / "peer_0_2_restarted.log").read_text()
    assert f"by requesting it from {c_peer_id}, ranked as WAL delta source: Covers" in r_log_text

    c_log_text = (log_folder / "peer_0_1.log").read_text()
    assert f"transfer to peer {r_peer_id} using diff transfer" in c_log_text
    assert FALLBACK_LOG not in c_log_text

    # A didn't treat R's update as an orphan operation
    a_log_text = (log_folder / "peer_0_0.log").read_text()
    assert AHEAD_ON_OWN_CLOCKS_LOG not in a_log_text

    # R kept its update, A still misses it. Each peer reads from its own replica here, because all
    # of them are Active
    assert has_point(r_uri, PARTIAL_POINT_ID)
    assert has_point(c_uri, PARTIAL_POINT_ID)
    assert not has_point(a_uri, PARTIAL_POINT_ID)
