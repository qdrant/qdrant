//! Dirty one-shot profiling to find which optimizer lock window blocks search.
//!
//! Uses global atomics — not for production, remove after profiling.
//!
//! Only logs when a new worst-case search spike is observed (monotonically
//! increasing threshold). Optimizer-side events always log since they are
//! infrequent.
//!
//! There are TWO write-lock windows that block search readers:
//!
//!  Window 1 ("proxy_swap"): optimize() upgrades to write lock to replace
//!     segments with proxies. `replicate_field_indexes` runs here and may
//!     call `create_field_index` on the cow segment — potentially slow.
//!
//!  Window 2 ("finish"): finish_optimization() upgrades to write lock for
//!     the final swap_new. Should be fast (pointer swap only).
//!
//!  Note: finish_optimization's `upgradable_read` (where create_field_index
//!  runs on the optimized segment) does NOT block readers — parking_lot
//!  upgradable_read is compatible with read().

use std::fmt::Display;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::Instant;

/// Epoch reference point for compact timestamps (millis since first use).
static EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

fn now_ms() -> u64 {
    let epoch = EPOCH.get_or_init(Instant::now);
    epoch.elapsed().as_millis() as u64
}

// ── Optimizer-side state ──────────────────────────────────────────────

/// Which phase currently holds a WRITE lock (actually blocks readers).
static IN_WRITE_LOCK_PHASE: AtomicU8 = AtomicU8::new(0);
static WRITE_LOCK_ACQUIRED_MS: AtomicU64 = AtomicU64::new(0);

/// Phase constants for IN_WRITE_LOCK_PHASE
const PHASE_NONE: u8 = 0;
const PHASE_PROXY_SWAP: u8 = 1; // optimize(): write lock for proxy replacement
const PHASE_FINISH_SWAP: u8 = 2; // finish_optimization(): write lock for final swap

/// True while `finish_optimization` holds the upgradable_read lock
/// (does NOT block readers, but useful to see correlation).
static IN_FINISH_UPGRADABLE: AtomicBool = AtomicBool::new(false);

/// True while `create_field_index` runs inside finish_optimization's
/// upgradable_read (does NOT block readers).
static IN_CREATE_FIELD_INDEX: AtomicBool = AtomicBool::new(false);

// ── Search-side spike tracking ────────────────────────────────────────

/// Greatest search lock wait observed so far (in ms). Only log when beaten.
static MAX_SEARCH_WAIT_MS: AtomicU64 = AtomicU64::new(0);

/// Try to update the max. Returns true if `new_ms` is a new record.
fn try_update_max(new_ms: u64) -> bool {
    let mut current = MAX_SEARCH_WAIT_MS.load(Ordering::Relaxed);
    loop {
        if new_ms <= current {
            return false;
        }
        match MAX_SEARCH_WAIT_MS.compare_exchange_weak(
            current,
            new_ms,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(actual) => current = actual,
        }
    }
}

// ── Window 1: proxy swap write lock (optimize() lines 723-761) ───────

pub fn on_proxy_swap_write_lock_acquired() {
    WRITE_LOCK_ACQUIRED_MS.store(now_ms(), Ordering::Relaxed);
    IN_WRITE_LOCK_PHASE.store(PHASE_PROXY_SWAP, Ordering::Release);
    log::warn!(
        "[OPT-PROFILE] proxy_swap: WRITE LOCK ACQUIRED at {}ms",
        now_ms()
    );
}

pub fn on_proxy_swap_replicate_start(field_count: usize) {
    log::warn!(
        "[OPT-PROFILE] proxy_swap: replicate_field_indexes START ({field_count} indexes) at {}ms",
        now_ms()
    );
}

pub fn on_proxy_swap_replicate_end() {
    log::warn!(
        "[OPT-PROFILE] proxy_swap: replicate_field_indexes END at {}ms",
        now_ms()
    );
}

pub fn on_proxy_swap_write_lock_released() {
    let held_ms = now_ms() - WRITE_LOCK_ACQUIRED_MS.load(Ordering::Relaxed);
    IN_WRITE_LOCK_PHASE.store(PHASE_NONE, Ordering::Release);
    log::warn!("[OPT-PROFILE] proxy_swap: WRITE LOCK RELEASED after {held_ms}ms",);
}

// ── Window 2: finish_optimization ────────────────────────────────────

pub fn on_finish_upgradable_acquired() {
    IN_FINISH_UPGRADABLE.store(true, Ordering::Release);
    log::warn!(
        "[OPT-PROFILE] finish: upgradable_read acquired at {}ms (does NOT block readers)",
        now_ms()
    );
}

pub fn on_create_field_index_start(field_name: &impl Display) {
    IN_CREATE_FIELD_INDEX.store(true, Ordering::Release);
    log::warn!(
        "[OPT-PROFILE] finish: create_field_index START field={field_name} at {}ms",
        now_ms()
    );
}

pub fn on_create_field_index_end(field_name: &impl Display) {
    IN_CREATE_FIELD_INDEX.store(false, Ordering::Release);
    log::warn!(
        "[OPT-PROFILE] finish: create_field_index END field={field_name} at {}ms",
        now_ms()
    );
}

pub fn on_delete_points_start(count: usize) {
    log::warn!(
        "[OPT-PROFILE] finish: delete_points START count={count} at {}ms",
        now_ms()
    );
}

pub fn on_delete_points_end() {
    log::warn!("[OPT-PROFILE] finish: delete_points END at {}ms", now_ms());
}

pub fn on_finish_write_lock_acquired() {
    WRITE_LOCK_ACQUIRED_MS.store(now_ms(), Ordering::Relaxed);
    IN_WRITE_LOCK_PHASE.store(PHASE_FINISH_SWAP, Ordering::Release);
    log::warn!(
        "[OPT-PROFILE] finish: WRITE LOCK ACQUIRED (upgrade) at {}ms",
        now_ms()
    );
}

pub fn on_finish_write_lock_released() {
    let held_ms = now_ms() - WRITE_LOCK_ACQUIRED_MS.load(Ordering::Relaxed);
    IN_WRITE_LOCK_PHASE.store(PHASE_NONE, Ordering::Release);
    IN_FINISH_UPGRADABLE.store(false, Ordering::Release);
    log::warn!("[OPT-PROFILE] finish: WRITE LOCK RELEASED after {held_ms}ms",);
}

// ── Search-side instrumentation ───────────────────────────────────────

/// Call before `try_read_for` in search path. Returns `Instant` for timing.
pub fn on_search_lock_wait_start() -> Instant {
    Instant::now()
}

/// Call after `try_read_for` returns in search path.
/// Only logs when a new worst-case spike is observed.
pub fn on_search_lock_wait_end(started: Instant, caller: &str) {
    let waited_ms = started.elapsed().as_millis() as u64;
    if try_update_max(waited_ms) {
        let write_phase = IN_WRITE_LOCK_PHASE.load(Ordering::Acquire);
        let finish_upgradable = IN_FINISH_UPGRADABLE.load(Ordering::Acquire);
        let index_building = IN_CREATE_FIELD_INDEX.load(Ordering::Acquire);
        let phase_name = match write_phase {
            PHASE_PROXY_SWAP => "proxy_swap",
            PHASE_FINISH_SWAP => "finish_swap",
            _ => "none",
        };
        log::warn!(
            "[OPT-PROFILE] {caller}: NEW WORST search wait {waited_ms}ms | \
             write_lock_phase={phase_name} finish_upgradable={finish_upgradable} \
             create_field_index={index_building}",
        );
    }
}
