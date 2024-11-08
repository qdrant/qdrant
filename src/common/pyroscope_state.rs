#[cfg(target_os = "linux")]
pub mod pyro {

    use pyroscope::pyroscope::PyroscopeAgentRunning;
    use pyroscope::{PyroscopeAgent, PyroscopeError};
    use pyroscope_pprofrs::{pprof_backend, PprofConfig};

    use crate::common::debugger::PyroscopeConfig;

    pub struct PyroscopeState {
        pub config: PyroscopeConfig,
        pub agent: Option<PyroscopeAgent<PyroscopeAgentRunning>>,
    }

    impl PyroscopeState {
        fn build_agent(
            config: &PyroscopeConfig,
        ) -> Result<PyroscopeAgent<PyroscopeAgentRunning>, PyroscopeError> {
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
                .build()?;
            let running_agent = agent.start()?;

            Ok(running_agent)
        }

        pub fn from_config(config: Option<PyroscopeConfig>) -> Option<Self> {
            match config {
                Some(pyro_config) => {
                    let agent = PyroscopeState::build_agent(&pyro_config);
                    match agent {
                        Ok(agent) => Some(PyroscopeState {
                            config: pyro_config,
                            agent: Some(agent),
                        }),
                        Err(err) => {
                            log::warn!("Pyroscope agent failed to start {}", err);
                            None
                        }
                    }
                }
                None => None,
            }
        }

        pub fn stop_agent(&mut self) -> bool {
            log::info!("Stopping pyroscope agent");
            if let Some(agent) = self.agent.take() {
                match agent.stop() {
                    Ok(stopped_agent) => {
                        log::info!("Stopped pyroscope agent. Shutting it down");
                        stopped_agent.shutdown();
                        log::info!("Pyroscope agent shut down completed.");
                        return true;
                    }
                    Err(err) => {
                        log::warn!("Pyroscope agent failed to stop {}", err);
                        return false;
                    }
                }
            }
            true
        }
    }

    impl Drop for PyroscopeState {
        fn drop(&mut self) {
            self.stop_agent();
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub mod pyro {
    use crate::common::debugger::PyroscopeConfig;

    pub struct PyroscopeState {}

    impl PyroscopeState {
        pub fn from_config(_config: Option<PyroscopeConfig>) -> Option<Self> {
            None
        }
    }
}
