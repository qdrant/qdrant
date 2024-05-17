use std::sync::{Arc, Mutex};

use api::grpc::models::PyroscopeConfig;
use pyroscope::pyroscope::PyroscopeAgentRunning;
use pyroscope::PyroscopeAgent;
use pyroscope_pprofrs::{pprof_backend, PprofConfig};

use crate::settings::Settings;

#[derive(Clone)]
pub struct PyroscopeState {
    pub config: Arc<Mutex<PyroscopeConfig>>,
    pub agent: Arc<Mutex<Option<PyroscopeAgent<PyroscopeAgentRunning>>>>,
}

impl PyroscopeState {
    fn build_agent(config: &PyroscopeConfig) -> PyroscopeAgent<PyroscopeAgentRunning> {
        let pprof_config = PprofConfig::new().sample_rate(config.sampling_rate.unwrap_or(100));
        let backend_impl = pprof_backend(pprof_config);

        log::info!(
            "Starting pyroscope agent with identifier {}",
            &config.identifier
        );
        let agent = PyroscopeAgent::builder(config.url.to_string(), "qdrant".to_string())
            .backend(backend_impl)
            .tags([("app", "Qdrant"), ("identifier", &config.identifier)].to_vec())
            .build()
            .expect("Couldn't build pyroscope agent");

        agent.start().unwrap()
    }

    /// Update agent config and restart
    pub fn update_agent(&self, config: &PyroscopeConfig) {
        let mut agent_guard = self.agent.lock().unwrap();
        if let Some(running_agent) = agent_guard.take() {
            let ready_agent = running_agent.stop().unwrap();
            ready_agent.shutdown();
        }

        *agent_guard = Some(PyroscopeState::build_agent(config));

        let mut config_guard = self.config.lock().unwrap();
        *config_guard = config.clone();

        log::info!("Pyroscope agent started");
    }

    pub fn from_settings(settings: &Settings) -> Option<Self> {
        settings
            .debug
            .pyroscope
            .clone()
            .map(|pyroscope_config| PyroscopeState {
                config: Arc::new(Mutex::new(pyroscope_config.clone())),
                agent: Arc::new(Mutex::new(Some(PyroscopeState::build_agent(
                    &pyroscope_config,
                )))),
            })
    }
}

impl Drop for PyroscopeState {
    fn drop(&mut self) {
        log::info!("Stopping pyroscope agent");
        let mut agent_guard = self.agent.lock().unwrap();
        if let Some(running_agent) = agent_guard.take() {
            let ready_agent = running_agent.stop().unwrap();
            ready_agent.shutdown();
        }

        log::info!("Pyroscope agent stopped");
    }
}
