"""`/profiler/consensus_lag` reports how far each peer trails at applying entries.

Every peer keeps a ring of the entries it applied, stamped with its own clock. The
endpoint collects those rings from the whole cluster and measures each entry from
whichever peer applied it first, so a lag is never negative.

Needs `--features staging` for `test_slow_down`.
"""

import json
import pathlib

from .fixtures import create_collection, upsert_random_points
from .utils import *

N_PEERS = 3
COLLECTION_NAME = "test_collection"


def get_lag(peer_api_uri):
    r = requests.get(f"{peer_api_uri}/profiler/consensus_lag")
    assert_http_ok(r)
    return r.json()["result"]


def test_consensus_lag_report(tmp_path: pathlib.Path):
    """Every peer answers, and a settled cluster reports no peer behind."""
    assert_project_root()
    peer_api_uris, _, _ = start_cluster(tmp_path, N_PEERS)
    create_collection(peer_api_uris[0], shard_number=2, replication_factor=2)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris
    )
    upsert_random_points(peer_api_uris[0], 100)

    for uri in peer_api_uris:
        report = get_lag(uri)
        print(f"\n=== settled: {uri} ===\n{json.dumps(report, indent=2)}")

        assert len(report["peers"]) == N_PEERS, report
        assert report["unreachable_peers"] == [], report
        assert report["applied_window"] is not None, report

        most_advanced = report["most_advanced_peer"]
        assert any(p["peer_id"] == most_advanced for p in report["peers"]), report

        for peer in report["peers"]:
            assert peer["last_applied_index"] is not None, peer
            assert peer["behind_entries"] == 0, peer
            assert not peer["beyond_window"], peer
            assert peer["lag_ms"] is not None, peer
            assert peer["lag_ms"]["samples"] > 0, peer
            # Measured from the first peer to apply each entry, so never negative.
            # `max_ms` can still run into seconds: that is the bootstrap, where
            # late joiners replayed entries the first peer applied before they existed.
            assert peer["lag_ms"]["min_ms"] >= 0, peer
            assert peer["lag_ms"]["avg_ms"] >= 0, peer
            assert peer["lag_ms"]["max_ms"] >= 0, peer
            assert peer["newest_applied_age_ms"] < 60_000, peer

        fastest_id = report["fastest_peer"]
        fastest = next(p for p in report["peers"] if p["peer_id"] == fastest_id)
        for peer in report["peers"]:
            assert peer["lag_ms"]["avg_ms"] >= fastest["lag_ms"]["avg_ms"], (
                fastest,
                peer,
            )


def test_consensus_lag_frozen_peer(tmp_path: pathlib.Path):
    """A peer whose apply loop is stalled must show up as behind."""
    assert_project_root()
    peer_api_uris, _, _ = start_cluster(tmp_path, N_PEERS)
    create_collection(peer_api_uris[0], shard_number=1, replication_factor=2)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris
    )

    frozen_id = get_cluster_info(peer_api_uris[2])["peer_id"]
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster",
        json={"test_slow_down": {"peer_id": frozen_id, "duration": 60.0}},
    )
    assert_http_ok(r)
    time.sleep(1)

    # Cluster metadata with `wait=false` is the cheapest consensus entry that does
    # not block on the frozen peer. An operation that waits for cluster
    # confirmation can outlast the freeze, leaving nothing to measure.
    for i in range(5):
        r = requests.put(
            f"{peer_api_uris[0]}/cluster/metadata/keys/lag_probe_{i}?wait=false",
            json=i,
        )
        assert_http_ok(r)
    time.sleep(3)

    report = get_lag(peer_api_uris[0])
    print(f"\n=== frozen peer {frozen_id} ===\n{json.dumps(report, indent=2)}")

    frozen = next(p for p in report["peers"] if p["peer_id"] == frozen_id)
    healthy = [p for p in report["peers"] if p["peer_id"] != frozen_id]

    assert frozen["behind_entries"] > 0, frozen
    assert frozen["pending_operations"] > 0, frozen

    for peer in healthy:
        assert peer["behind_entries"] == 0, peer
        assert peer["pending_operations"] == 0, peer
        # The stall shows in the newest entry's age, not in `lag_ms`: everything
        # the frozen peer did apply, it applied on time, so its overlap
        # statistics stay healthy while it sits stuck part-way through an entry.
        assert frozen["newest_applied_age_ms"] > peer["newest_applied_age_ms"], (
            frozen,
            peer,
        )
