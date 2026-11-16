//! `SetQuotaConfig` operation

use super::*;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::*;
use crate::content_manager::errors::StorageError;
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

#[test]
fn set_quota_config_boundaries() {
    let lower = QuotaConfig {
        max_resident_memory_percent: Some(1),
        max_disk_usage_percent: Some(1),
        release_margin_percent: Some(0),
        ..quota_config(true)
    };

    let upper = QuotaConfig {
        max_resident_memory_percent: Some(100),
        max_disk_usage_percent: Some(100),
        release_margin_percent: Some(100),
        ..quota_config(true)
    };

    for config in [lower, upper] {
        let mut machine = state_machine(ClusterState::default());
        let outcome = machine.apply(&ConsensusOperations::SetQuotaConfig(config));

        let Ok(Plan { actions, .. }) = outcome else {
            panic!(
                "quota config at range boundaries should be accepted, {config:?}, got {outcome:?}"
            );
        };

        assert!(matches!(
            actions.as_slice(),
            [Action::SetQuotaConfig { .. }],
        ));

        assert_eq!(machine.state().quota_config, config);
    }
}

#[test]
fn set_quota_config_reject_invalid() {
    // Storage rejects invalid limits even while quotas are disabled
    for enabled in [false, true] {
        let base = quota_config(enabled);

        let invalid = [
            (
                "max_resident_memory_percent",
                QuotaConfig {
                    max_resident_memory_percent: Some(0),
                    ..base
                },
            ),
            (
                "max_resident_memory_percent",
                QuotaConfig {
                    max_resident_memory_percent: Some(101),
                    ..base
                },
            ),
            (
                "max_disk_usage_percent",
                QuotaConfig {
                    max_disk_usage_percent: Some(0),
                    ..base
                },
            ),
            (
                "max_disk_usage_percent",
                QuotaConfig {
                    max_disk_usage_percent: Some(101),
                    ..base
                },
            ),
            (
                "release_margin_percent",
                QuotaConfig {
                    release_margin_percent: Some(101),
                    ..base
                },
            ),
        ];

        for (field, config) in invalid {
            set_quota_config_reject(field, config);
        }
    }
}

fn set_quota_config_reject(field: &str, config: QuotaConfig) {
    let state = ClusterState {
        quota_config: quota_config(true),
        ..Default::default()
    };

    let mut machine = state_machine(state.clone());
    let outcome = machine.apply(&ConsensusOperations::SetQuotaConfig(config));

    let Err(StorageError::BadRequest { description }) = outcome else {
        panic!(
            "invalid quota config should be rejected as bad request, {config:?}, got {outcome:?}"
        );
    };

    assert!(
        description.contains(field),
        "error should name invalid field `{field}`, got: {description}",
    );

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
