use std::sync::{Arc, Mutex};

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

pub struct DebugState {
    pub pyroscope: Arc<Mutex<Option<PyroscopeState>>>,
}

impl DebugState {
    pub fn from_settings(settings: &Settings) -> Self {
        let pyroscope_config = settings.debug.pyroscope.clone();
        Self {
            pyroscope: Arc::new(Mutex::new(PyroscopeState::from_config(pyroscope_config))),
        }
    }

    pub fn get_config(&self) -> DebugConfig {
        let pyroscope_config = {
            #[cfg(target_os = "linux")]
            {
                let pyroscope_state_guard = self.pyroscope.lock().unwrap();
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

    pub fn apply_config_patch(&self, config_patch: DebugConfig) -> bool {
        #[cfg(target_os = "linux")]
        {
            // FIXME: Need to differentiate between null and non existing key.
            // Serde seems to be passing both cases as None
            // If null, disable pyroscope. If non existing, skip.
            if let Some(pyro_config) = config_patch.pyroscope {
                let mut pyroscope_state_guard = self.pyroscope.lock().unwrap();
                *pyroscope_state_guard = PyroscopeState::from_config(Some(pyro_config));
            } else {
                let mut pyroscope_guard = self.pyroscope.lock().unwrap();
                *pyroscope_guard = None; // TODO: Find out if this actually calls drop (shutdown agent) or not?
            }
            true
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = config_patch; // Ignore new_config on non-linux OS
            false
        }
    }
}
