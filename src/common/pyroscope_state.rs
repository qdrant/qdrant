#[cfg(target_os = "linux")]
pub mod pyro {

    use pyroscope::pyroscope::PyroscopeAgentRunning;
    use pyroscope::PyroscopeAgent;
    use pyroscope_pprofrs::{pprof_backend, PprofConfig};

    use crate::common::debug::PyroscopeConfig;

    pub struct PyroscopeState {
        pub config: PyroscopeConfig,
        pub agent: Option<PyroscopeAgent<PyroscopeAgentRunning>>,
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

        pub fn from_config(config: Option<PyroscopeConfig>) -> Option<Self> {
            match config {
                Some(pyro_config) => Some(PyroscopeState {
                    config: pyro_config.clone(),
                    agent: Some(PyroscopeState::build_agent(&pyro_config)),
                }),
                None => {
                    log::info!("Pyroscope agent not started");
                    None
                }
            }
        }
    }

    impl Drop for PyroscopeState {
        fn drop(&mut self) {
            log::info!("Stopping pyroscope agent");
            if let Some(agent) = self.agent.take() {
                // Use take() to replace self.agent with None and get the contained value
                let stopped_agent = agent.stop().unwrap(); // Now you can call stop() on the agent directly
                stopped_agent.shutdown();
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
