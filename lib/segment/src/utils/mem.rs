use crate::entry::entry_point::{OperationError, OperationResult};

#[derive(Debug)]
pub struct Mem {
    #[cfg(target_os = "linux")]
    cgroups: Option<cgroups_mem::CgroupsMem>,
    sysinfo: sysinfo_mem::SysinfoMem,
}

impl Mem {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            #[cfg(target_os = "linux")]
            cgroups: cgroups_mem::CgroupsMem::new(),
            sysinfo: sysinfo_mem::SysinfoMem::new(),
        }
    }

    #[allow(dead_code)]
    pub fn refresh(&mut self) {
        #[cfg(target_os = "linux")]
        if let Some(cgroups) = &mut self.cgroups {
            cgroups.refresh();
        }

        self.sysinfo.refresh();
    }

    pub fn total_memory_bytes(&self) -> u64 {
        #[cfg(target_os = "linux")]
        if let Some(cgroups) = &self.cgroups {
            if let Some(memory_limit_bytes) = cgroups.memory_limit_bytes() {
                return memory_limit_bytes;
            }
        }

        self.sysinfo.total_memory_bytes()
    }

    pub fn available_memory_bytes(&self) -> u64 {
        #[cfg(target_os = "linux")]
        if let Some(cgroups) = &self.cgroups {
            if let Some(memory_limit_bytes) = cgroups.memory_limit_bytes() {
                return memory_limit_bytes.saturating_sub(cgroups.used_memory_bytes());
            }
        }

        self.sysinfo.available_memory_bytes()
    }
}

pub fn assert_available_memory_during_segment_load(
    mem: &Mem,
    data_size_bytes: u64,
    available_memory_percent: u64,
) -> OperationResult<()> {
    let total_memory_bytes = mem.total_memory_bytes();
    let available_memory_bytes = mem.available_memory_bytes();

    let available_memory_after_data_load_bytes =
        available_memory_bytes.saturating_sub(data_size_bytes);

    let required_available_memory_bytes = total_memory_bytes / 100 * available_memory_percent;

    // Create segment only if at least 15% of RAM available (to prevent OOM on load)
    if available_memory_after_data_load_bytes >= required_available_memory_bytes {
        Ok(())
    } else {
        Err(OperationError::service_error(format!(
            "less than {available_memory_percent}% of RAM available \
             ({available_memory_after_data_load_bytes} bytes out of {total_memory_bytes} bytes) \
             while loading DB segment, segment load aborted to prevent OOM"
        )))
    }
}

#[cfg(target_os = "linux")]
mod cgroups_mem {
    use cgroups_rs::{hierarchies, memory, Cgroup};
    use procfs::process::Process;

    #[derive(Clone, Debug)]
    pub struct CgroupsMem {
        mem_controller: memory::MemController,
        memory_limit_bytes: Option<u64>,
        used_memory_bytes: u64,
    }

    impl CgroupsMem {
        pub fn new() -> Option<Self> {
            let memory_cgroup_path = match get_current_process_memory_cgroup_path() {
                Ok(memory_cgroup_path) => memory_cgroup_path?,
                Err(err) => {
                    log::error!(
                        "Failed to query current process info \
                         while initializing CgroupsMem: {err}"
                    );

                    return None;
                }
            };

            let cgroup = Cgroup::load(hierarchies::auto(), memory_cgroup_path);

            let mem_controller: &memory::MemController = cgroup.controller_of()?;

            let mut mem = Self {
                mem_controller: mem_controller.clone(),
                memory_limit_bytes: None,
                used_memory_bytes: 0,
            };

            mem.refresh();

            Some(mem)
        }

        pub fn refresh(&mut self) {
            let stat = self.mem_controller.memory_stat();
            self.memory_limit_bytes = stat.limit_in_bytes.try_into().ok();
            self.used_memory_bytes = stat.usage_in_bytes;
        }

        pub fn memory_limit_bytes(&self) -> Option<u64> {
            self.memory_limit_bytes
        }

        pub fn used_memory_bytes(&self) -> u64 {
            self.used_memory_bytes
        }
    }

    fn get_current_process_memory_cgroup_path() -> procfs::ProcResult<Option<String>> {
        let process = Process::myself()?;
        let cgroups = process.cgroups()?;

        for cgroup in cgroups {
            // TODO: Can a process belong to multiple cgroups v2 hierarchies!?
            let is_v2_hierarchy = cgroup.controllers.is_empty();

            // TODO: Can a process belong to multiple cgroups v1 hierarchies with the same (e.g., memory) controller!?
            let is_v1_memory_cgroup = cgroup
                .controllers
                .iter()
                .any(|controller| controller == "memory");

            if is_v2_hierarchy || is_v1_memory_cgroup {
                return Ok(Some(cgroup.pathname));
            }
        }

        Ok(None)
    }
}

mod sysinfo_mem {
    use sysinfo::{RefreshKind, System, SystemExt as _};

    #[derive(Debug)]
    pub struct SysinfoMem {
        system: System,
    }

    impl SysinfoMem {
        pub fn new() -> Self {
            let mut system = System::new_with_specifics(RefreshKind::new().with_memory());
            system.refresh_memory();
            Self { system }
        }

        pub fn refresh(&mut self) {
            self.system.refresh_memory();
        }

        pub fn total_memory_bytes(&self) -> u64 {
            self.system.total_memory()
        }

        pub fn available_memory_bytes(&self) -> u64 {
            self.system.available_memory()
        }
    }
}
