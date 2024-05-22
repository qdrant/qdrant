use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

use crate::{common::pyroscope_state::pyro::PyroscopeState, settings::Settings};

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
        Self {
            pyroscope: Arc::new(Mutex::new(PyroscopeState::from_settings(settings))),
        }
    }

    pub fn update_pyroscope_config(&self, config: PyroscopeConfig) {
        let mut pyroscope_guard = self.pyroscope.lock().unwrap();
        let pyroscope_state = pyroscope_guard.as_mut().unwrap();
        pyroscope_state.restart_agent(config);
    }
}
