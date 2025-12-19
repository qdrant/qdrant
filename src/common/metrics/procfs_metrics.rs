use std::cmp::min;

use ahash::AHashSet;
use itertools::Itertools;
use procfs::ProcError;
use procfs::process::{LimitValue, Process};
use prometheus::proto::MetricType;

use super::common::{counter, gauge, metric_family};
use super::{MetricsData, MetricsProvider};

type Pid = i32;

/// Structure for holding /procfs metrics, that can be easily populated in metrics API.
pub(super) struct ProcFsMetrics {
    thread_count: usize,
    mmap_count: usize,
    system_mmap_limit: u64,
    open_fds: usize,
    max_fds_soft: LimitValue,
    max_fds_hard: LimitValue,
    minor_page_faults: u64,
    major_page_faults: u64,
    minor_children_page_faults: u64,
    major_children_page_faults: u64,
    minor_alive_child_page_faults: u64,
    major_alive_child_page_faults: u64,
}

impl ProcFsMetrics {
    /// Collect metrics from /procfs.
    pub(super) fn collect() -> Result<Self, procfs::ProcError> {
        let current_process = Process::myself()?;

        let thread_count = current_process.tasks()?.flatten().count();
        let stat = current_process.stat()?;
        let limits = current_process.limits()?;

        let (minor_alive_child_page_faults, major_alive_child_page_faults) =
            faults_for_all_children(current_process.pid)?;

        Ok(Self {
            thread_count,
            mmap_count: current_process.maps()?.len(),
            system_mmap_limit: procfs::sys::vm::max_map_count()?,
            open_fds: current_process.fd_count()?,
            max_fds_soft: limits.max_open_files.soft_limit,
            max_fds_hard: limits.max_open_files.hard_limit,
            minor_page_faults: stat.minflt,
            major_page_faults: stat.majflt,
            minor_children_page_faults: stat.cminflt,
            major_children_page_faults: stat.cmajflt,
            minor_alive_child_page_faults,
            major_alive_child_page_faults,
        })
    }
}

impl MetricsProvider for ProcFsMetrics {
    fn add_metrics(&self, metrics: &mut MetricsData, prefix: Option<&str>) {
        metrics.push_metric(metric_family(
            "process_threads",
            "count of active threads",
            MetricType::GAUGE,
            vec![gauge(self.thread_count as f64, &[])],
            prefix,
        ));

        metrics.push_metric(metric_family(
            "process_open_mmaps",
            "count of open mmaps",
            MetricType::GAUGE,
            vec![gauge(self.mmap_count as f64, &[])],
            prefix,
        ));

        metrics.push_metric(metric_family(
            "system_max_mmaps",
            "system wide limit of open mmaps",
            MetricType::GAUGE,
            vec![gauge(self.system_mmap_limit as f64, &[])],
            prefix,
        ));

        metrics.push_metric(metric_family(
            "process_open_fds",
            "count of currently open file descriptors",
            MetricType::GAUGE,
            vec![gauge(self.open_fds as f64, &[])],
            prefix,
        ));

        let fds_limit = match (self.max_fds_soft, self.max_fds_hard) {
            (LimitValue::Unlimited, LimitValue::Value(hard)) => hard, // soft unlimited, use hard
            (LimitValue::Value(soft), LimitValue::Unlimited) => soft, // hard unlimited, use soft
            (LimitValue::Value(soft), LimitValue::Value(hard)) => min(soft, hard), // both limited, use minimum
            (LimitValue::Unlimited, LimitValue::Unlimited) => 0,                   // both unlimited
        };
        metrics.push_metric(metric_family(
            "process_max_fds",
            "limit for open file descriptors",
            MetricType::GAUGE,
            vec![gauge(fds_limit as f64, &[])],
            prefix,
        ));

        let minor_page_faults = self.minor_page_faults
            + self.minor_children_page_faults
            + self.minor_alive_child_page_faults;

        let major_page_faults = self.major_page_faults
            + self.major_children_page_faults
            + self.major_alive_child_page_faults;

        metrics.push_metric(metric_family(
            "process_minor_page_faults_total",
            "count of minor page faults which didn't cause a disk access",
            MetricType::COUNTER,
            vec![counter(minor_page_faults as f64, &[])],
            prefix,
        ));

        metrics.push_metric(metric_family(
            "process_major_page_faults_total",
            "count of disk accesses caused by a mmap page fault",
            MetricType::COUNTER,
            vec![counter(major_page_faults as f64, &[])],
            prefix,
        ));
    }
}

/// Returns the minor and major page faults of all children including grandchildren, etc.
pub fn faults_for_all_children(parent: Pid) -> Result<(u64, u64), ProcError> {
    let children = child_processes_helper(parent)?;
    let (mut min, mut maj) = (0, 0);
    for child in children {
        let stat = match Process::new(child).and_then(|i| i.stat()) {
            Ok(proc) => proc,
            // Ignore children that don't exist anymore.
            Err(ProcError::NotFound(_)) => continue,
            Err(err) => return Err(err),
        };

        min += stat.minflt + stat.cminflt;
        maj += stat.majflt + stat.cmajflt;
    }
    Ok((min, maj))
}

/// Returns a list of all children of a given process. This works recursively, including grandchildren, etc.
fn child_processes_helper(pid: Pid) -> Result<AHashSet<Pid>, ProcError> {
    let mut pids = AHashSet::default();
    child_processes_recursive(pid, &mut pids)?;

    // We don't expect to include parent Pid
    debug_assert!(!pids.contains(&pid));

    Ok(pids)
}

/// Recursively collects all children of a process, specified by `pid`.
fn child_processes_recursive(pid: Pid, pids: &mut AHashSet<Pid>) -> Result<(), ProcError> {
    for child_pid in child_processes(pid) {
        let child_pid = child_pid?;

        let is_new = pids.insert(child_pid);

        // If PID is new we recurse into it
        if is_new {
            child_processes_recursive(child_pid, pids)?;
        }
    }

    Ok(())
}

/// Returns all new child processes of a parent process, specified by `parent_pid`.
fn child_processes(parent_pid: Pid) -> Box<dyn Iterator<Item = Result<Pid, ProcError>>> {
    let process = match Process::new(parent_pid) {
        Ok(proc) => proc,
        // Ignore children that don't exist anymore.
        Err(ProcError::NotFound(_)) => return Box::new(std::iter::empty()),
        Err(err) => return Box::new(std::iter::once(Err(err))),
    };

    let tasks = match process.tasks() {
        Ok(tasks) => tasks,
        Err(err) => return Box::new(std::iter::once(Err(err))),
    };

    let pids = tasks
        .filter_map(|task| {
            let children = task.and_then(|task| task.children());
            match children {
                Ok(children) => Some(Ok(children)),
                // Filter out tasks/children that might not exist anymore.
                Err(ProcError::NotFound(_)) => None,
                Err(err) => Some(Err(err)),
            }
        })
        .flatten_ok()
        .map_ok(|i| i as Pid);

    Box::new(pids)
}

#[test]
fn test_child_processes() {
    use nix::sys::wait::waitpid;
    use nix::unistd::{ForkResult, fork};

    let my_pid = procfs::process::Process::myself().unwrap().pid;

    // We have no children now
    assert_eq!(child_processes_helper(my_pid).unwrap(), AHashSet::new(),);

    // Spawn two child processes
    let mut child_1 = std::process::Command::new("sleep")
        .arg("3s")
        .spawn()
        .unwrap();
    let mut child_2 = std::process::Command::new("sleep")
        .arg("3s")
        .spawn()
        .unwrap();
    let child_1_pid = child_1.id() as Pid;
    let child_2_pid = child_2.id() as Pid;

    // Recursively fork process to create a grandchild
    let fork_1 = match unsafe { fork().unwrap() } {
        ForkResult::Parent { child } => child,
        ForkResult::Child => {
            let fork_2 = match unsafe { fork().unwrap() } {
                ForkResult::Parent { child } => child,
                ForkResult::Child => {
                    std::thread::sleep(std::time::Duration::from_secs(3));
                    std::process::exit(0);
                }
            };
            waitpid(fork_2, None).unwrap();
            std::process::exit(0);
        }
    };

    // Give forks a second to spawn
    std::thread::sleep(std::time::Duration::from_secs(1));

    // We expect exactly 4 children (3 direct, 1 grandchild)
    // We don't know the PID of the grandchild here, so we expect three children and one extra
    let child_pids = child_processes_helper(my_pid).unwrap();
    assert_eq!(child_pids.len(), 4);
    assert_eq!(
        child_pids
            .difference(&AHashSet::from([child_1_pid, child_2_pid, fork_1.into()]))
            .count(),
        1,
        "expect exactly one extra child",
    );

    // Cleanup fork
    waitpid(fork_1, None).unwrap();

    // Cleanup processes
    child_1.kill().unwrap();
    child_2.kill().unwrap();
    let _ = child_1.wait();
    let _ = child_2.wait();
}
