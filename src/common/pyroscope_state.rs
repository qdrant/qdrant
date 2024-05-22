#[cfg(target_os = "linux")]
pub mod pyro {
    use std::sync::{Arc, Mutex};

    use pyroscope::pyroscope::PyroscopeAgentRunning;
    use pyroscope::PyroscopeAgent;
    use pyroscope_pprofrs::{pprof_backend, PprofConfig};

    use crate::common::debug::PyroscopeConfig;
    use crate::settings::Settings;

    #[derive(Clone)]
    pub struct PyroscopeState {
        pub config: PyroscopeConfig,
        #[cfg(target_os = "linux")]
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
            // TODO: Add more tags like peerId and peerUrl
            let agent = PyroscopeAgent::builder(config.url.to_string(), "qdrant".to_string())
                .backend(backend_impl)
                .tags(vec![("app", "Qdrant"), ("identifier", &config.identifier)])
                .build()
                .expect("Couldn't build pyroscope agent");

            agent.start().unwrap()
        }

        /// Update agent config and restart
        pub fn restart_agent(&mut self, config: PyroscopeConfig) {
            // Stop the running agent if it exists:
            let mut agent_guard = self.agent.lock().unwrap();
            if let Some(running_agent) = agent_guard.take() {
                let ready_agent = running_agent.stop().unwrap();
                ready_agent.shutdown();
            }

            *agent_guard = Some(PyroscopeState::build_agent(&config));
            self.config = config.clone();
            log::info!("Pyroscope agent started");
        }

        pub fn from_settings(settings: &Settings) -> Option<Self> {
            settings
                .debug
                .pyroscope
                .clone()
                .map(|pyroscope_config| PyroscopeState {
                    config: pyroscope_config.clone(),
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
}

#[cfg(not(target_os = "linux"))]
pub mod pyro {
    pub struct PyroscopeState {}

    impl PyroscopeState {
        pub fn from_settings(settings: &Settings) -> Option<Self> {
            None
        }
    }
}
