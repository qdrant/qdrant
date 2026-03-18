#[cfg(target_os = "linux")]
pub mod pyro {

    use pyroscope::backend::jemalloc::jemalloc_backend;
    use pyroscope::backend::{BackendConfig, PprofConfig, pprof_backend};
    use pyroscope::pyroscope::{PyroscopeAgentBuilder, PyroscopeAgentRunning};
    use pyroscope::{PyroscopeAgent, PyroscopeError};

    use crate::common::debugger::PyroscopeConfig;

    pub struct PyroscopeState {
        pub config: PyroscopeConfig,
        pub cpu_agent: Option<PyroscopeAgent<PyroscopeAgentRunning>>,
        pub heap_agent: Option<PyroscopeAgent<PyroscopeAgentRunning>>,
    }

    impl PyroscopeState {
        fn build_cpu_agent(
            config: &PyroscopeConfig,
        ) -> Result<PyroscopeAgent<PyroscopeAgentRunning>, PyroscopeError> {
            // pprof uses tempfile::NamedTempFile which respects TMPDIR.
            // Qdrant's clear_all_tmp_directories() deletes TMPDIR on startup,
            // so we must ensure it exists before the profiler tries to create temp files.
            if let Ok(tmpdir) = std::env::var("TMPDIR") {
                let path = std::path::Path::new(&tmpdir);
                if !path.exists()
                    && let Err(err) = fs_err::create_dir_all(path)
                {
                    log::warn!("Failed to create TMPDIR {tmpdir}: {err}");
                }
            }

            let sample_rate = config.sampling_rate.unwrap_or(100);
            let pprof_config = PprofConfig { sample_rate };
            let backend_impl = pprof_backend(pprof_config, BackendConfig::default());

            log::info!(
                "Starting pyroscope CPU agent with identifier {}",
                &config.identifier
            );
            // TODO: Add more tags like peerId and peerUrl
            let agent = PyroscopeAgentBuilder::new(
                config.url.clone(),
                "qdrant",
                sample_rate,
                "pyroscope-rs",
                env!("CARGO_PKG_VERSION"),
                backend_impl,
            )
            .tags(vec![("app", "Qdrant"), ("identifier", &config.identifier)])
            .build()?;
            let running_agent = agent.start()?;

            Ok(running_agent)
        }

        fn build_heap_agent(
            config: &PyroscopeConfig,
        ) -> Result<PyroscopeAgent<PyroscopeAgentRunning>, PyroscopeError> {
            let backend_impl = jemalloc_backend();

            log::info!(
                "Starting pyroscope heap agent with identifier {}",
                &config.identifier
            );
            let agent = PyroscopeAgentBuilder::new(
                config.url.clone(),
                "qdrant",
                0,
                "pyroscope-rs",
                env!("CARGO_PKG_VERSION"),
                backend_impl,
            )
            .tags(vec![("app", "Qdrant"), ("identifier", &config.identifier)])
            .build()?;
            let running_agent = agent.start()?;

            Ok(running_agent)
        }

        fn is_jemalloc_profiling_enabled() -> bool {
            std::env::var("MALLOC_CONF")
                .map(|conf| conf.split(',').any(|opt| opt.trim() == "prof:true"))
                .unwrap_or(false)
        }

        pub fn from_config(config: Option<PyroscopeConfig>) -> Option<Self> {
            match config {
                Some(pyro_config) => {
                    let cpu_agent = match PyroscopeState::build_cpu_agent(&pyro_config) {
                        Ok(agent) => Some(agent),
                        Err(err) => {
                            log::warn!("Pyroscope CPU agent failed to start: {err}");
                            return None;
                        }
                    };

                    let heap_agent = if Self::is_jemalloc_profiling_enabled() {
                        match PyroscopeState::build_heap_agent(&pyro_config) {
                            Ok(agent) => {
                                log::info!(
                                    "Pyroscope heap agent started (MALLOC_CONF has prof:true)"
                                );
                                Some(agent)
                            }
                            Err(err) => {
                                log::warn!("Pyroscope heap agent failed to start: {err}");
                                None
                            }
                        }
                    } else {
                        log::info!(
                            "Jemalloc profiling not enabled, skipping heap agent. Set MALLOC_CONF=\"prof:true,prof_active:true\" to enable."
                        );
                        None
                    };

                    Some(PyroscopeState {
                        config: pyro_config,
                        cpu_agent,
                        heap_agent,
                    })
                }
                None => None,
            }
        }

        fn stop_single_agent(agent: PyroscopeAgent<PyroscopeAgentRunning>, name: &str) -> bool {
            match agent.stop() {
                Ok(stopped_agent) => {
                    log::info!("Stopped pyroscope {name} agent. Shutting it down");
                    stopped_agent.shutdown();
                    log::info!("Pyroscope {name} agent shut down completed.");
                    true
                }
                Err(err) => {
                    log::warn!("Pyroscope {name} agent failed to stop: {err}");
                    false
                }
            }
        }

        pub fn stop_agent(&mut self) -> bool {
            log::info!("Stopping pyroscope agents");
            let mut success = true;

            if let Some(agent) = self.cpu_agent.take()
                && !Self::stop_single_agent(agent, "CPU")
            {
                success = false;
            }

            if let Some(agent) = self.heap_agent.take()
                && !Self::stop_single_agent(agent, "heap")
            {
                success = false;
            }

            success
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
