use std::collections::VecDeque;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::Duration;
use std::{io, thread};

use super::Event;
use super::clock::now;

const INTERVAL: Duration = Duration::from_millis(10);
const LIMIT: usize = 60_000; // 10 minutes

/// Background thread that samples CPU usage, for [`Event::Cpu`].
pub struct CpuSampler {
    stop: mpsc::Sender<()>,
    thread: thread::JoinHandle<Event>,
}

impl CpuSampler {
    /// Start the background thread.
    pub fn start() -> io::Result<Self> {
        let (stop, stopped) = mpsc::channel();
        let thread = thread::Builder::new()
            .name("uio-trace-cpu".to_owned())
            .spawn(move || run_thread(&stopped))?;
        Ok(Self { stop, thread })
    }

    /// Stop the background thread and return the collected samples.
    pub fn finish(self) -> Event {
        let Self { stop, thread } = self;
        drop(stop);
        thread
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    }
}

fn run_thread(stop: &Receiver<()>) -> Event {
    let mut timestamps = VecDeque::new();
    let mut cpu_ns = VecDeque::new();
    loop {
        if timestamps.len() == LIMIT {
            timestamps.pop_front();
            cpu_ns.pop_front();
        }
        timestamps.push_back(now());
        cpu_ns.push_back(process_cpu_ns());
        if !matches!(stop.recv_timeout(INTERVAL), Err(RecvTimeoutError::Timeout)) {
            return Event::Cpu { timestamps, cpu_ns };
        }
    }
}

#[cfg(target_os = "linux")]
fn process_cpu_ns() -> u64 {
    let mut ts = nix::libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime with CLOCK_PROCESS_CPUTIME_ID is always valid.
    let ret = unsafe { nix::libc::clock_gettime(nix::libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts) };
    if ret == 0 {
        ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
    } else {
        0
    }
}

#[cfg(not(target_os = "linux"))]
fn process_cpu_ns() -> u64 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finish_returns_samples() {
        let sampler = CpuSampler::start().unwrap();
        thread::sleep(INTERVAL * 3);
        let Event::Cpu { timestamps, cpu_ns } = sampler.finish() else {
            panic!("not a cpu event");
        };
        assert!(!timestamps.is_empty());
        assert_eq!(timestamps.len(), cpu_ns.len());
        assert!(timestamps.iter().is_sorted());
        assert!(cpu_ns.iter().is_sorted());
    }
}
