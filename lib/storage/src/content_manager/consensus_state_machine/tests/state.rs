use collection::operations::types::PeerMetadata;
use semver::Version;
use tonic::transport::Uri;

use super::*;

#[test]
fn all_peers_at_version_empty() {
    let state = ClusterState::default();

    assert!(state.all_peers_at_version(&version("1.14.2-dev")));
}

#[test]
fn all_peers_at_version_all_new_enough() {
    let state = cluster_state_with_peers(&[(PEER_ID, "1.14.2"), (OTHER_PEER_ID, "1.15.0")]);

    assert!(state.all_peers_at_version(&version("1.14.2-dev")));
}

#[test]
fn all_peers_at_version_old_peer() {
    let state = cluster_state_with_peers(&[(PEER_ID, "1.14.0"), (OTHER_PEER_ID, "1.15.0")]);

    assert!(!state.all_peers_at_version(&version("1.14.2-dev")));
}

#[test]
fn all_peers_at_version_missing_metadata() {
    let mut state = cluster_state_with_peers(&[(PEER_ID, "1.15.0")]);
    state
        .peer_address_by_id
        .insert(OTHER_PEER_ID, peer_address(OTHER_PEER_ID));

    assert!(!state.all_peers_at_version(&version("1.14.2-dev")));
}

fn cluster_state_with_peers(peers: &[(PeerId, &str)]) -> ClusterState {
    let peer_address_by_id = peers
        .iter()
        .map(|&(peer_id, _)| (peer_id, peer_address(peer_id)))
        .collect();

    let peer_metadata_by_id = peers
        .iter()
        .map(|&(peer_id, version)| (peer_id, PeerMetadata::new(self::version(version))))
        .collect();

    ClusterState {
        peer_address_by_id,
        peer_metadata_by_id,
        ..Default::default()
    }
}

fn peer_address(peer_id: PeerId) -> Uri {
    format!("http://peer-{peer_id}")
        .parse()
        .expect("valid peer URI")
}

fn version(version: &str) -> Version {
    version.parse().expect("valid version")
}
