//! Measures how many sequential cache-line accesses it takes before the
//! hardware prefetcher engages — the stream detector's training threshold.
//!
//! Method: a dependent pointer chase over short runs of K consecutive cache
//! lines. One run per 4 KiB page (so runs never collide, never cross a page
//! boundary, and no page carries prefetcher training left over from another
//! run), pages visited in shuffled order, every run visited exactly once.
//! Each hop depends on the previous load, so the chase has zero memory-level
//! parallelism — any cheapening of a run's later lines can only come from
//! the hardware prefetcher running ahead of the chain. A streaming pass over
//! the whole buffer between chain build and timing evicts the build's writes.
//!
//! Reading the output: the marginal cost of extending a run by one line is
//! ~full DRAM latency while the prefetcher is asleep, and collapses once it
//! has trained and runs ahead. The knee position is the training threshold
//! (plus the prefetcher's first-fetch lead).
//!
//! Usage:
//!   cargo build -p segment --profile perf --example hwpf_threshold_probe
//!   ./target/perf/examples/hwpf_threshold_probe
//! Env: PROBE_MB (512) buffer size, PROBE_ROUNDS (3),
//!      PROBE_LINE (64) cache line size in bytes (128 for Apple L2),
//!      PROBE_MAX_K (32) longest run in lines,
//!      PROBE_SW_PREFIX (0) issue software prefetches (prefetcht0) for the
//!      first N lines of each run right before chasing it. Compare K > N
//!      rows against PROBE_SW_PREFIX=0 and =K: if the tail lines stay cheap
//!      the hardware prefetcher continues past a software-prefetched prefix;
//!      if the staircase reappears after line N it starts cold there.
//!
//! Install-level mode (PROBE_INSTALL=1): instead of the threshold probe,
//! measure which cache level each prefetch hint actually fills. A dependent
//! chase over single random lines (one per page), where each line is hinted
//! W hops ahead of its demand load; by arrival the fill is long complete, so
//! the per-hop cost is the hit latency at whatever level the hint installed
//! into. Modes: no hint (DRAM baseline), T0, T1, T2. If T1 matches T0 the
//! core fills L1 for both (documented Zen 3 behavior); a T1 a few ns above
//! T0 but far below baseline means the L1-exclusion is honored (documented
//! Zen 4+/Intel). Env: PROBE_INSTALL_W (64) hint lead in hops,
//! PROBE_INSTALL_MULS (16) dependent multiplies pacing each hop so
//! outstanding fills stay well under the MSHR count.

use std::hint::black_box;
use std::time::Instant;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// xorshift64* — deterministic, dependency-free
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// Non-faulting prefetch into all cache levels; no-op off x86_64/aarch64.
#[inline(always)]
fn prefetch(ptr: *const u64) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: prefetch is a non-faulting hint for any address.
    unsafe {
        std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(ptr.cast::<i8>())
    }
    #[cfg(target_arch = "aarch64")]
    // SAFETY: `prfm` is a non-faulting hint; no memory access, stack use,
    // or flag clobber.
    unsafe {
        core::arch::asm!(
            "prfm pldl1keep, [{ptr}]",
            ptr = in(reg) ptr,
            options(nostack, preserves_flags),
        )
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = ptr;
}

/// Non-faulting prefetch into L2 and higher (`prefetcht1` / `pldl2keep`).
#[inline(always)]
fn prefetch_l2(ptr: *const u64) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: prefetch is a non-faulting hint for any address.
    unsafe {
        std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T1 }>(ptr.cast::<i8>())
    }
    #[cfg(target_arch = "aarch64")]
    // SAFETY: `prfm` is a non-faulting hint; no memory access, stack use,
    // or flag clobber.
    unsafe {
        core::arch::asm!(
            "prfm pldl2keep, [{ptr}]",
            ptr = in(reg) ptr,
            options(nostack, preserves_flags),
        )
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = ptr;
}

/// Non-faulting prefetch into L3 and higher (`prefetcht2` / `pldl3keep`).
#[inline(always)]
fn prefetch_l3(ptr: *const u64) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: prefetch is a non-faulting hint for any address.
    unsafe {
        std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T2 }>(ptr.cast::<i8>())
    }
    #[cfg(target_arch = "aarch64")]
    // SAFETY: `prfm` is a non-faulting hint; no memory access, stack use,
    // or flag clobber.
    unsafe {
        core::arch::asm!(
            "prfm pldl3keep, [{ptr}]",
            ptr = in(reg) ptr,
            options(nostack, preserves_flags),
        )
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = ptr;
}

/// One timed chase pass for the install-level probe. `hint` is issued for
/// the line `w` hops ahead of its demand load; `muls` dependent multiplies
/// gate every hop's address so hops never overlap and outstanding fills stay
/// low. Returns ns per hop.
fn install_chase(
    buf: &[u64],
    starts: &[u32],
    w: usize,
    muls: usize,
    hint: impl Fn(*const u64),
) -> f64 {
    let pages = starts.len();
    let ptr = buf.as_ptr();
    // Opaque runtime zero: `j & zero` below cannot be constant-folded by the
    // compiler (value unknown) and is no dependency-breaking idiom for the
    // hardware, so the delay chain genuinely gates every hop's address.
    let zero = black_box(0u64);
    let mut idx = starts[0] as u64;
    let t = Instant::now();
    for i in 0..pages {
        if i + w < pages {
            // SAFETY: starts holds in-bounds word indices.
            hint(unsafe { ptr.add(*starts.get_unchecked(i + w) as usize) });
        }
        let mut j = idx;
        for _ in 0..muls {
            // Multiply mixed with xor-shift: unlike a pure affine chain,
            // consecutive iterations cannot be composed into one constant
            // operation, so all `muls` steps stay on the dependency path.
            j = j.wrapping_mul(0x2545_F491_4F6C_DD1D) ^ (j >> 32);
        }
        let adj = j & zero;
        idx = unsafe { *buf.get_unchecked((idx.wrapping_add(adj)) as usize) };
    }
    black_box(idx);
    t.elapsed().as_nanos() as f64 / pages as f64
}

fn install_probe(
    buf: &mut [u64],
    pages: usize,
    lines_per_page: usize,
    words_per_line: usize,
    rounds: usize,
) {
    let w = env_usize("PROBE_INSTALL_W", 64);
    let muls = env_usize("PROBE_INSTALL_MULS", 16);
    let words = buf.len();
    println!(
        "== prefetch install-level probe: {pages} random lines (one per page), \
         hint {w} hops ahead, {muls} pacing multiplies per hop, {rounds} rounds =="
    );

    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    let mut order: Vec<u32> = (0..pages as u32).collect();
    const MODES: [&str; 4] = ["none", "t0", "t1", "t2"];
    let mut per_mode: [Vec<f64>; 4] = [const { Vec::new() }; 4];

    for round in 0..rounds {
        for i in (1..order.len()).rev() {
            order.swap(i, rng.below(i + 1));
        }
        let mut starts: Vec<u32> = Vec::with_capacity(pages);
        for &page in &order {
            let off = rng.below(lines_per_page);
            starts.push(((page as usize * lines_per_page + off) * words_per_line) as u32);
        }
        for r in 0..pages {
            buf[starts[r] as usize] = starts[(r + 1) % pages] as u64;
        }
        // Rotate mode order per round: within-round drift (clock ramp, DRAM
        // state) otherwise biases whichever mode always runs last.
        for m in (0..MODES.len()).map(|i| (i + round) % MODES.len()) {
            // evict whatever the build or the previous mode left cached
            let mut acc = 0u64;
            for i in (0..words).step_by(words_per_line) {
                acc = acc.wrapping_add(unsafe { *buf.get_unchecked(i) });
            }
            black_box(acc);

            let ns = match m {
                1 => install_chase(buf, &starts, w, muls, prefetch),
                2 => install_chase(buf, &starts, w, muls, prefetch_l2),
                3 => install_chase(buf, &starts, w, muls, prefetch_l3),
                _ => install_chase(buf, &starts, w, muls, |p| {
                    black_box(p);
                }),
            };
            per_mode[m].push(ns);
        }
    }

    let mut med = [0.0f64; 4];
    for (m, v) in per_mode.iter_mut().enumerate() {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        med[m] = v[v.len() / 2];
    }
    let base = med[0];
    let floor = med[1]; // T0 documented "all levels" everywhere = the L1 floor
    let dram_gap = base - floor;

    println!(
        "\n{:>6} {:>12} {:>12}  verdict (heuristic)",
        "hint", "ns/hop", "vs t0"
    );
    println!(
        "{:>6} {:>12.1} {:>12}  DRAM baseline (no hint)",
        "none", base, "-"
    );
    for m in 1..4 {
        let d = med[m] - floor;
        let verdict = if m == 1 {
            "L1 floor by definition (docs: fills all levels)"
        } else if d <= 1.2 {
            "fills L1 (behaves like T0)"
        } else if d <= 10.0 {
            "skips L1, installs to L2"
        } else if d < 0.6 * dram_gap {
            "installs to L3 only"
        } else {
            "not installed (= no hint)"
        };
        println!("{:>6} {:>12.1} {:>+11.1}  {verdict}", MODES[m], med[m], d);
    }
    println!(
        "\nnote: thresholds are heuristic (L1-vs-L2 gap is ~2-5 ns depending on \
         clock); trust the raw deltas. DRAM gap here: {dram_gap:.1} ns."
    );
    // ~3 cycles per dependent multiply; even at 6 GHz that is 0.5 ns each.
    if floor < muls as f64 * 0.5 {
        println!(
            "WARNING: t0 floor {floor:.1} ns < expected pacing (~{} ns): the \
             delay chain got compiled out — outstanding fills may exceed small \
             MSHR counts and inflate the t0 floor on Intel-class cores.",
            muls as f64 * 0.5
        );
    }
}

fn main() {
    let mb = env_usize("PROBE_MB", 512);
    let rounds = env_usize("PROBE_ROUNDS", 3);
    let line = env_usize("PROBE_LINE", 64);
    let max_k = env_usize("PROBE_MAX_K", 32);
    let sw_prefix = env_usize("PROBE_SW_PREFIX", 0);
    assert!(line % 8 == 0 && line >= 64);
    let words = mb * 1024 * 1024 / 8;
    let words_per_line = line / 8;
    let lines_per_page = 4096 / line;
    let pages = mb * 1024 * 1024 / 4096;

    let install = env_usize("PROBE_INSTALL", 0) == 1;
    if !install {
        println!(
            "== hw-prefetcher training threshold probe: buffer {mb} MiB, \
             line {line} B, one run per page ({pages} runs), {rounds} rounds, \
             sw prefix {sw_prefix} lines =="
        );
    }

    let mut buf: Vec<u64> = vec![0u64; words];
    for i in (0..words).step_by(4096 / 8) {
        buf[i] = 1; // fault every page in before timing
    }

    if install {
        install_probe(&mut buf, pages, lines_per_page, words_per_line, rounds);
        return;
    }

    let ks: Vec<usize> = [1usize, 2, 3, 4, 5, 6, 8, 10, 12, 16, 20, 24, 28, 32]
        .into_iter()
        .filter(|&k| k <= max_k && k <= lines_per_page)
        .collect();

    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut med_run_ns: Vec<(usize, f64)> = Vec::new();

    // reusable shuffled page order (reshuffled per round)
    let mut order: Vec<u32> = (0..pages as u32).collect();

    for &k in &ks {
        let mut per_round = Vec::new();
        for _ in 0..rounds {
            for i in (1..order.len()).rev() {
                order.swap(i, rng.below(i + 1));
            }
            // one run per page at a random in-page offset that fits K lines
            let start_of =
                |page: u32, off: usize| (page as usize * lines_per_page + off) * words_per_line;
            let mut starts: Vec<u32> = Vec::with_capacity(pages);
            for &page in &order {
                let off = rng.below(lines_per_page - k + 1);
                starts.push(start_of(page, off) as u32);
            }
            for r in 0..pages {
                let s = starts[r] as usize;
                for j in 0..k - 1 {
                    buf[s + j * words_per_line] = (s + (j + 1) * words_per_line) as u64;
                }
                buf[s + (k - 1) * words_per_line] = starts[(r + 1) % pages] as u64;
            }
            // evict the build's writes: stream-read every line once
            let mut acc = 0u64;
            for i in (0..words).step_by(words_per_line) {
                acc = acc.wrapping_add(unsafe { *buf.get_unchecked(i) });
            }
            black_box(acc);

            // timed per-run chase: every run visited exactly once. The next
            // run's start comes from the chain's own last hop (not starts[]),
            // so runs stay fully serialized like the original flat chain;
            // the software-prefetch prefix is issued the moment a run's start
            // address becomes known, i.e. with zero lead.
            let ptr = buf.as_ptr();
            let p = sw_prefix.min(k);
            let mut idx = starts[0] as u64;
            let t = Instant::now();
            for _ in 0..pages {
                let s = idx as usize;
                for j in 0..p {
                    // SAFETY: address lies within the buffer; prefetch never
                    // faults regardless.
                    prefetch(unsafe { ptr.add(s + j * words_per_line) });
                }
                for _ in 0..k {
                    idx = unsafe { *buf.get_unchecked(idx as usize) };
                }
            }
            black_box(idx);
            per_round.push(t.elapsed().as_nanos() as f64 / pages as f64);
        }
        per_round.sort_by(|a, b| a.partial_cmp(b).unwrap());
        med_run_ns.push((k, per_round[per_round.len() / 2]));
    }

    let base = med_run_ns[0].1;
    println!("\nbase random-line latency (K=1): {base:.1} ns");
    println!(
        "\n{:>3} {:>12} {:>16} {:>10}",
        "K", "ns/run", "marginal ns/line", "vs base"
    );
    let mut knee: Option<usize> = None;
    for w in med_run_ns.windows(2) {
        let (k0, t0) = w[0];
        let (k1, t1) = w[1];
        let marginal = (t1 - t0) / (k1 - k0) as f64;
        let ratio = marginal / base;
        println!("{k1:>3} {t1:>12.1} {marginal:>16.1} {ratio:>9.2}x");
        if knee.is_none() && ratio < 0.5 {
            knee = Some(k0);
        }
    }
    match knee {
        Some(t) => println!(
            "\n=> marginal line cost first drops below 0.5x base after line ~{t}: \
             the hw prefetcher trains on about {t} sequential misses"
        ),
        None => println!(
            "\n=> no marginal collapse up to K={max_k}: no stream prefetcher \
             engaged in this pattern (or its lead never catches a dependent chase)"
        ),
    }
    println!(
        "note: dependent chase gives the prefetcher ~1 latency per line to run \
         ahead — this measures the training threshold, not its steady-state lead"
    );
}
