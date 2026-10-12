use std::panic::Location;

/// You probably came here from the panic below.
///
/// Qdrant counts CPU and IO spent on each request. Where the counts go is
/// tracked per thread, by entering an "ambient scope".
///
/// If you see this panic, that means the code did measurable work on a thread
/// that is inside no scope, so the counts have nowhere to go.
///
/// # Fix A: if this code path shouldn't be measured at all
///
/// 1) In tests and benchmarks:
///
///    ```ignore
///    use common::ambient;
///
///    #[test]
///    fn my_test() {
///        let _scope = ambient::test_guard(); // ← add this
///        /* … */
///    }
///    ```
///
/// 2) In internal operation that shouldn't be billed to any request
///    (e.g., loading, optimization, …). Opt out explicitly, stating why:
///
///    ```ignore
///    use common::reason::reason;
///
///    // ↓ Add this to the top-level entry point of the operation.
///    let _scope = ambient::unmeasured_guard(reason("<your reason here>"));
///    ```
///
///
/// # Fix B: if the scope didn't survive a hop to another thread/task
///
/// Scopes are per-thread. Spawned work starts without one.
/// Re-run with `RUST_BACKTRACE=1` and find the nearest frame where execution
/// crossed to another thread or task. Carry the scope across it:
///
/// 3) Spawned thread or blocking task (sync), e.g. [`std::thread::spawn`] or
///    [`tokio::task::spawn_blocking`]:
///
///    ```ignore
///    let handoff = ambient::current();      // ← add this
///    tokio::task::spawn_blocking(move || {
///        let _scope = handoff.enter_guard(); // ← add this
///        /* … */
///    })
///    .await
///    ```
///
/// 4) Spawned async task, e.g. [`tokio::spawn`]:
///
///    ```ignore
///    use common::ambient::{self, AmbientFutureExt}; // ← add this
///
///    tokio::spawn(
///        async move { /* … */ }
///            .in_current_ambient(), // ← add this
///    )
///    .await
///    ```
///
/// 5) Rayon or another work-stealing pool:
///
///    ```ignore
///    ambient::parallel(|handoff| {               // ← wrap inside `ambient::parallel`
///        items.par_iter().for_each(|item| {
///            let _scope = handoff.enter_guard(); // ← add this
///            /* … */
///        })
///    })
///    ```
pub(super) fn panic_outside_of_any_scope(what: &str, caller: &'static Location<'static>) -> ! {
    panic!("{what:?} outside of any ambient scope. Caller: {caller}");
}
