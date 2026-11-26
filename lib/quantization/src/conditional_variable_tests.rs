use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use crate::ConditionalVariable;

/// How long a waiter may take to return before the test treats it as a lost wakeup.
const WAKEUP_TIMEOUT: Duration = Duration::from_secs(5);

#[test]
fn wait_returns_on_notify() {
    let waiter = ConditionalVariable::default();
    let notifier = waiter.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let handle = thread::spawn(move || done_tx.send(waiter.wait()).unwrap());

    notifier.notify();
    let disconnected = done_rx
        .recv_timeout(WAKEUP_TIMEOUT)
        .expect("waiter was not woken");
    assert!(!disconnected, "the notifier clone is still alive");
    handle.join().unwrap();
}

#[test]
fn wait_returns_immediately_without_other_clones() {
    let alone = ConditionalVariable::default();
    assert!(
        alone.wait(),
        "a variable without other clones is disconnected"
    );
}

/// Dropping the last other clone must wake a waiter.
///
/// PQ encoding relies on this: a job that stops early returns without notifying the next job and
/// only drops its handle. `Drop` used to notify before releasing its `Arc`, so a waiter woken by
/// that notification could still see two strong references, go back to sleep, and never be
/// woken again. That hung the optimization, and with it `wait_workers_stops`.
#[test]
fn wait_returns_when_last_other_clone_is_dropped() {
    const THREADS: usize = 8;
    const ITERATIONS: usize = 2_000;

    let workers: Vec<_> = (0..THREADS)
        .map(|_| {
            thread::spawn(|| {
                for iteration in 0..ITERATIONS {
                    let waiter = ConditionalVariable::default();
                    let other = waiter.clone();
                    let (done_tx, done_rx) = mpsc::channel();
                    let handle = thread::spawn(move || done_tx.send(waiter.wait()).unwrap());

                    // Give the waiter time to block in `wait` most of the time
                    thread::sleep(Duration::from_micros(50));
                    drop(other);

                    let disconnected = done_rx.recv_timeout(WAKEUP_TIMEOUT).unwrap_or_else(|_| {
                        panic!("waiter not woken after the last other clone was dropped (iteration {iteration})")
                    });
                    assert!(disconnected);
                    handle.join().unwrap();
                }
            })
        })
        .collect();

    for worker in workers {
        worker.join().unwrap();
    }
}
