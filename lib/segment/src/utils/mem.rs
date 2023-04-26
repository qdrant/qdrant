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
    use cgroups_rs::{hierarchies, memory, Hierarchy as _, Subsystem};

    #[derive(Clone, Debug)]
    pub struct CgroupsMem {
        mem_controller: memory::MemController,
        memory_limit_bytes: Option<u64>,
        used_memory_bytes: u64,
    }

    impl CgroupsMem {
        pub fn new() -> Option<Self> {
            let cgroup = hierarchies::auto().root_control_group();
            let subsystems = cgroup.subsystems();

            let mem_controller = subsystems.iter().find_map(|ctrl| match ctrl {
                Subsystem::Mem(mem_controller) => Some(mem_controller.clone()),
                _ => None,
            })?;

            let mut mem = Self {
                mem_controller,
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
