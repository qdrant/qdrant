//! `SetQuotaConfig` operation

use super::*;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::*;
use crate::quota::QuotaConfig;

#[test]
fn set_quota_config() {
    let mut machine = state_machine(ClusterState::default());
    let outcome = machine.apply(&ConsensusOperations::SetQuotaConfig(quota_config(true)));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("a quota config should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::SetQuotaConfig { .. }],
    ));

    assert_eq!(machine.state().quota_config, quota_config(true));
}

#[test]
fn set_quota_config_replace() {
    let state = ClusterState {
        quota_config: quota_config(true),
        ..Default::default()
    };

    let mut machine = state_machine(state);
    let outcome = machine.apply(&ConsensusOperations::SetQuotaConfig(quota_config(false)));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("another quota config should be accepted, got {outcome:?}");
    };

    assert!(matches!(
        actions.as_slice(),
        [Action::SetQuotaConfig { .. }],
    ));

    assert_eq!(machine.state().quota_config, quota_config(false));
}

#[test]
fn set_quota_config_replay() {
    let state = ClusterState {
        quota_config: quota_config(true),
        ..Default::default()
    };

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&ConsensusOperations::SetQuotaConfig(quota_config(true)));

    let Ok(Plan { actions, .. }) = outcome else {
        panic!("replay of an applied quota config should be accepted, got {outcome:?}");
    };

    // Action is emitted even if state already matches: applying it also drops recorded verdicts
    assert!(matches!(
        actions.as_slice(),
        [Action::SetQuotaConfig { .. }],
    ));

    assert_eq!(machine.state(), &state);
}

fn quota_config(enabled: bool) -> QuotaConfig {
    QuotaConfig {
        enabled,
        max_resident_memory_percent: None,
        max_disk_usage_percent: None,
        release_margin_percent: None,
    }
}
