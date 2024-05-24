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

#[derive(Default, Debug, Serialize, JsonSchema, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum PyroscopeConfigPatch {
    Patch(Option<PyroscopeConfig>),
    #[default]
    Null,
}

#[derive(Default, Debug, Serialize, JsonSchema, Deserialize, Clone)]
pub struct DebugConfigPatch {
    pub pyroscope: PyroscopeConfigPatch,
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
            match patch.pyroscope {
                PyroscopeConfigPatch::Patch(pyro_config) => {
                    log::info!("Patch matched");
                    if let Some(pyro_config) = pyro_config {
                        log::info!("Patch matched: Config found");
                        if let Some(mut pyroscope_state) = self.pyroscope.lock().take() {
                            log::info!("Patch matched: Config found: State found");
                            return pyroscope_state.stop_agent();
                        }

                        let mut pyroscope_guard = self.pyroscope.lock();
                        *pyroscope_guard = PyroscopeState::from_config(Some(pyro_config));
                        return true;
                    }
                }
                PyroscopeConfigPatch::Null => {
                    log::info!("Null matched");
                    if let Some(mut pyroscope_state) = self.pyroscope.lock().take() {
                        log::info!("Null matched: Config found");
                        return pyroscope_state.stop_agent();
                    }
                }
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
