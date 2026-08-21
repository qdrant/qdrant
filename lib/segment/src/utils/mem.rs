use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// TTL for the cached total memory; matches the disk-usage cache.
const CACHE_TTL: Duration = Duration::from_secs(5);

static TOTAL_MEMORY_CACHE: Mutex<Option<(Instant, u64)>> = Mutex::new(None);

/// Total memory in bytes (cgroup limit if set, else host), cached for `CACHE_TTL`
/// since `Mem::new()` is not free. Short TTL keeps it in step with limit resizes.
pub fn total_memory_bytes() -> u64 {
    let mut cache = TOTAL_MEMORY_CACHE.lock();
    let now = Instant::now();

    if let Some((cached_at, value)) = *cache
        && now.saturating_duration_since(cached_at) < CACHE_TTL
    {
        return value;
    }

    let value = Mem::new().total_memory_bytes();
    *cache = Some((now, value));
    value
}

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

    pub fn refresh(&mut self) {
        #[cfg(target_os = "linux")]
        if let Some(cgroups) = &mut self.cgroups {
            cgroups.refresh();
        }

        self.sysinfo.refresh();
    }

    pub fn total_memory_bytes(&self) -> u64 {
        #[cfg(target_os = "linux")]
        if let Some(cgroups) = &self.cgroups
            && let Some(memory_limit_bytes) = cgroups.memory_limit_bytes()
        {
            return memory_limit_bytes;
        }

        self.sysinfo.total_memory_bytes()
    }

    pub fn available_memory_bytes(&self) -> u64 {
        #[cfg(target_os = "linux")]
        if let Some(cgroups) = &self.cgroups
            && let Some(memory_limit_bytes) = cgroups.memory_limit_bytes()
        {
            return memory_limit_bytes.saturating_sub(cgroups.used_memory_bytes());
        }

        self.sysinfo.available_memory_bytes()
    }
}

#[cfg(target_os = "linux")]
mod cgroups_mem {
    use std::io;
    use std::path::{Path, PathBuf};

    use fs_err as fs;
    use procfs::process::{MountInfos, Process};

    /// Mount point of the unified (v2) hierarchy.
    const UNIFIED_MOUNTPOINT: &str = "/sys/fs/cgroup";

    /// With no limit set, `memory.limit_in_bytes` (v1) reads back as `LONG_MAX` rounded down to the
    /// page size. Treat anything that large as "no limit", rather than a ~9 EB total memory.
    const V1_UNLIMITED_THRESHOLD: u64 = i64::MAX as u64 - (1 << 20);

    #[derive(Clone, Debug)]
    pub struct CgroupsMem {
        limit_path: PathBuf,
        usage_path: PathBuf,
        memory_limit_bytes: Option<u64>,
        used_memory_bytes: u64,
    }

    impl CgroupsMem {
        pub fn new() -> Option<Self> {
            let (dir, is_v2) = match memory_cgroup_dir() {
                Ok(dir) => dir?,
                Err(err) => {
                    log::error!(
                        "Failed to query current process info \
                         while initializing CgroupsMem: {err}"
                    );

                    return None;
                }
            };

            let (limit_file, usage_file) = if is_v2 {
                ("memory.max", "memory.current")
            } else {
                ("memory.limit_in_bytes", "memory.usage_in_bytes")
            };

            let limit_path = dir.join(limit_file);

            let memory_limit_bytes = match read_memory_limit(&limit_path) {
                Ok(memory_limit_bytes) => memory_limit_bytes,
                // The memory controller is not available in this cgroup
                Err(err) if err.kind() == io::ErrorKind::NotFound => return None,
                Err(err) => {
                    log::error!("Failed to read memory limit while initializing CgroupsMem: {err}");

                    return None;
                }
            };

            let usage_path = dir.join(usage_file);

            let used_memory_bytes = match read_memory_usage(&usage_path) {
                Ok(used_memory_bytes) => used_memory_bytes,
                Err(err) if err.kind() == io::ErrorKind::NotFound => return None,
                Err(err) => {
                    log::error!("Failed to read memory usage while initializing CgroupsMem: {err}");

                    return None;
                }
            };

            Some(Self {
                limit_path,
                usage_path,
                memory_limit_bytes,
                used_memory_bytes,
            })
        }

        /// Failed reads keep the last known value: dropping the limit would silently fall back to
        /// host memory, and zeroing the usage would report the whole limit as free.
        pub fn refresh(&mut self) {
            if let Ok(memory_limit_bytes) = read_memory_limit(&self.limit_path) {
                self.memory_limit_bytes = memory_limit_bytes;
            }

            if let Ok(used_memory_bytes) = read_memory_usage(&self.usage_path) {
                self.used_memory_bytes = used_memory_bytes;
            }
        }

        pub fn memory_limit_bytes(&self) -> Option<u64> {
            self.memory_limit_bytes
        }

        pub fn used_memory_bytes(&self) -> u64 {
            self.used_memory_bytes
        }
    }

    /// Directory holding the memory controller files of the current process, and whether it belongs
    /// to the unified (v2) hierarchy.
    fn memory_cgroup_dir() -> procfs::ProcResult<Option<(PathBuf, bool)>> {
        let process = Process::myself()?;

        if is_cgroup2_unified_mode() {
            // TODO: Can a process belong to multiple v2 cgroups!?
            let dir = process
                .cgroups()?
                .into_iter()
                // The v2 entry is the one with hierarchy ID 0
                .find(|cgroup| cgroup.hierarchy == 0)
                .map(|cgroup| join_cgroup_path(Path::new(UNIFIED_MOUNTPOINT), &cgroup.pathname));

            return Ok(dir.map(|dir| (dir, true)));
        }

        let Some(mount_point) = v1_memory_mount_point(process.mountinfo()?) else {
            return Ok(None);
        };

        // TODO: Can a process belong to multiple v1 cgroups, with some of these cgroups having the same controllers (e.g., memory)!?
        let dir = process
            .cgroups()?
            .into_iter()
            .find(|cgroup| cgroup.controllers.iter().any(|c| c == "memory"))
            .map(|cgroup| join_cgroup_path(&mount_point, &cgroup.pathname));

        Ok(dir.map(|dir| (dir, false)))
    }

    fn is_cgroup2_unified_mode() -> bool {
        Path::new(UNIFIED_MOUNTPOINT)
            .join("cgroup.controllers")
            .exists()
    }

    /// Where the v1 memory controller is mounted.
    fn v1_memory_mount_point(mountinfo: MountInfos) -> Option<PathBuf> {
        mountinfo
            .into_iter()
            .find(|mount| mount.fs_type == "cgroup" && mount.super_options.contains_key("memory"))
            .map(|mount| mount.mount_point)
    }

    /// Cgroup pathnames are relative to the mount point of their hierarchy, but start with a `/`.
    fn join_cgroup_path(mount_point: &Path, pathname: &str) -> PathBuf {
        mount_point.join(pathname.trim_start_matches('/'))
    }

    /// `None` if no limit is set, either as v2 `max` or as the v1 `LONG_MAX` sentinel.
    fn read_memory_limit(path: &Path) -> io::Result<Option<u64>> {
        let raw = fs::read_to_string(path)?;
        let raw = raw.trim();

        if raw == "max" {
            return Ok(None);
        }

        let memory_limit_bytes = raw
            .parse::<u64>()
            .ok()
            .filter(|&memory_limit_bytes| memory_limit_bytes < V1_UNLIMITED_THRESHOLD);

        Ok(memory_limit_bytes)
    }

    fn read_memory_usage(path: &Path) -> io::Result<u64> {
        let raw = fs::read_to_string(path)?;

        raw.trim()
            .parse()
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn read_limit(contents: &str) -> Option<u64> {
            let dir = tempfile::Builder::new().tempdir().unwrap();
            let path = dir.path().join("memory.max");
            fs::write(&path, contents).unwrap();
            read_memory_limit(&path).unwrap()
        }

        #[test]
        fn memory_limit_parsing() {
            assert_eq!(read_limit("1073741824\n"), Some(1073741824));
            // v2 reports "max" when unlimited
            assert_eq!(read_limit("max\n"), None);
            // v1 reports LONG_MAX rounded down to the page size when unlimited
            assert_eq!(read_limit("9223372036854771712\n"), None);
            assert_eq!(read_limit("9223372036854710272\n"), None);
            assert_eq!(read_limit("garbage\n"), None);
        }

        #[test]
        fn v1_memory_mount_point_is_picked_by_super_option() {
            let mountinfo = MountInfos(
                [
                    "30 23 0:26 / /sys/fs/cgroup ro,nosuid,nodev,noexec - tmpfs tmpfs ro,mode=755",
                    "32 30 0:28 / /sys/fs/cgroup/cpu,cpuacct rw,relatime - cgroup cgroup rw,cpu,cpuacct",
                    "33 30 0:29 / /sys/fs/cgroup/memory rw,relatime - cgroup cgroup rw,memory",
                ]
                .into_iter()
                .map(|line| procfs::process::MountInfo::from_line(line).unwrap())
                .collect(),
            );

            assert_eq!(
                v1_memory_mount_point(mountinfo),
                Some(PathBuf::from("/sys/fs/cgroup/memory")),
            );
        }

        #[test]
        fn cgroup_paths_are_joined_onto_the_mount_point() {
            let mount_point = Path::new("/sys/fs/cgroup");

            // A container with its own cgroup namespace sees the root
            assert_eq!(
                join_cgroup_path(mount_point, "/"),
                Path::new("/sys/fs/cgroup"),
            );
            assert_eq!(
                join_cgroup_path(mount_point, "/user.slice/session-2.scope"),
                Path::new("/sys/fs/cgroup/user.slice/session-2.scope"),
            );
        }

        fn cgroup_mem(dir: &Path) -> CgroupsMem {
            let limit_path = dir.join("memory.max");
            let usage_path = dir.join("memory.current");
            fs::write(&limit_path, "1073741824").unwrap();
            fs::write(&usage_path, "1024").unwrap();

            CgroupsMem {
                limit_path,
                usage_path,
                memory_limit_bytes: Some(1073741824),
                used_memory_bytes: 1024,
            }
        }

        #[test]
        fn unreadable_memory_usage_keeps_the_last_known_value() {
            let dir = tempfile::Builder::new().tempdir().unwrap();
            let mut mem = cgroup_mem(dir.path());

            fs::write(&mem.usage_path, "garbage").unwrap();
            mem.refresh();

            assert_eq!(mem.used_memory_bytes(), 1024);
        }

        #[test]
        fn unreadable_memory_limit_keeps_the_last_known_value() {
            let dir = tempfile::Builder::new().tempdir().unwrap();
            let mut mem = cgroup_mem(dir.path());

            fs::remove_file(&mem.limit_path).unwrap();
            mem.refresh();

            assert_eq!(mem.memory_limit_bytes(), Some(1073741824));

            // A limit lifted at runtime still clears
            fs::write(&mem.limit_path, "max").unwrap();
            mem.refresh();

            assert_eq!(mem.memory_limit_bytes(), None);
        }

        #[test]
        fn missing_memory_limit_file_is_not_found() {
            let dir = tempfile::Builder::new().tempdir().unwrap();
            let err = read_memory_limit(&dir.path().join("memory.max")).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::NotFound);

            let err = read_memory_usage(&dir.path().join("memory.current")).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::NotFound);
        }
    }
}

mod sysinfo_mem {
    use sysinfo::{MemoryRefreshKind, RefreshKind, System};

    #[derive(Debug)]
    pub struct SysinfoMem {
        system: System,
    }

    impl SysinfoMem {
        pub fn new() -> Self {
            let system = System::new_with_specifics(
                RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
            );
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
