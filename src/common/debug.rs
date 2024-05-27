use std::sync::Arc;

use parking_lot::Mutex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::common::pyroscope_state::pyro::PyroscopeState;
use crate::settings::Settings;

#[derive(Serialize, JsonSchema, Debug, Deserialize, Clone)]
pub struct PyroscopeConfig {
    pub url: String,
    pub identifier: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub sampling_rate: Option<u32>,
}

#[derive(Default, Debug, Serialize, JsonSchema, Deserialize, Clone)]
pub struct DebugConfig {
    pub pyroscope: Option<PyroscopeConfig>,
}

#[derive(Debug, Serialize, JsonSchema, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DebugConfigPatch {
    Pyroscope(Option<PyroscopeConfig>),
}

pub struct DebugState {
    pub pyroscope: Arc<Mutex<Option<PyroscopeState>>>,
}

impl DebugState {
    pub fn from_settings(settings: &Settings) -> Self {
        let pyroscope_config = settings.debugger.pyroscope.clone();
        Self {
            pyroscope: Arc::new(Mutex::new(PyroscopeState::from_config(pyroscope_config))),
        }
    }

    pub fn get_config(&self) -> DebugConfig {
        let pyroscope_config = {
            #[cfg(target_os = "linux")]
            {
                let pyroscope_state_guard = self.pyroscope.lock();
                pyroscope_state_guard.as_ref().map(|s| s.config.clone())
            }
            #[cfg(not(target_os = "linux"))]
            {
                None
            }
        };

        DebugConfig {
            pyroscope: pyroscope_config,
        }
    }

    pub fn apply_config_patch(&self, patch: DebugConfigPatch) -> bool {
        #[cfg(target_os = "linux")]
        {
            match patch {
                DebugConfigPatch::Pyroscope(new_config) => {
                    let mut pyroscope_guard = self.pyroscope.lock();
                    if let Some(pyroscope_state) = pyroscope_guard.as_mut() {
                        let stopped = pyroscope_state.stop_agent();
                        if !stopped {
                            return false;
                        }
                    }

                    if let Some(new_config) = new_config {
                        *pyroscope_guard = PyroscopeState::from_config(Some(new_config));
                    }
                    true
                }
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = patch; // Ignore new_config on non-linux OS
            false
        }
    }
}
