//! Optional AMX backend for a prepared batch of scalar-quantized queries.
//!
//! The codes are in `0..=127`, so unsigned-by-signed AMX dot products need no
//! correction. This module returns integer dots; the caller applies the same
//! floating-point postprocessing as the single-query scorer.

use crate::encoded_vectors_u8::EncodedQueryU8;

const TILE: usize = 16;
const DEPTH: usize = 64;
const TILE_BYTES: usize = TILE * DEPTH;
const QUERY_TILES: usize = 4;
const QUERY_BLOCK: usize = TILE * QUERY_TILES;

pub(super) struct AmxQueries {
    /// `[query block][depth block][query tile][depth / 4][query lane][depth % 4]`.
    packed: Vec<u8>,
    query_count: usize,
    dimension: usize,
    depth_blocks: usize,
}

impl AmxQueries {
    /// Pack queries once, independently of the thread which will score them.
    /// Returns `None` for fewer than 16 queries, zero dimensions, or unsupported
    /// CPU/OS tile state. Linux permission is checked later on the scoring worker.
    pub(super) fn new(queries: &[EncodedQueryU8], dimension: usize) -> Option<Self> {
        if queries.len() < TILE || dimension == 0 || !hardware_supported() {
            return None;
        }
        Some(Self::pack(queries, dimension))
    }

    /// Arrange query panels as `[query block][depth block][tile][depth / 4]
    /// [query lane][depth % 4]`, zero-filling missing queries and depth bytes.
    ///
    /// # Panics
    /// Panics if a query length differs from `dimension` or the buffer size overflows.
    fn pack(queries: &[EncodedQueryU8], dimension: usize) -> Self {
        let depth_blocks = dimension.div_ceil(DEPTH);
        let size = queries
            .len()
            .div_ceil(QUERY_BLOCK)
            .checked_mul(depth_blocks)
            .and_then(|n| n.checked_mul(QUERY_TILES * TILE_BYTES))
            .expect("AMX query buffer size overflow");
        let mut packed = vec![0; size];
        for (q, query) in queries.iter().enumerate() {
            assert_eq!(query.encoded_query.len(), dimension);
            let group = q / QUERY_BLOCK;
            let tile = q % QUERY_BLOCK / TILE;
            let lane = q % TILE;
            for (k, &code) in query.encoded_query.iter().enumerate() {
                let block = k / DEPTH;
                let within = k % DEPTH;
                let index = (group * depth_blocks + block) * QUERY_TILES * TILE_BYTES
                    + tile * TILE_BYTES
                    + (within / 4) * DEPTH
                    + lane * 4
                    + within % 4;
                packed[index] = code;
            }
        }
        Self {
            packed,
            query_count: queries.len(),
            dimension,
            depth_blocks,
        }
    }

    /// Score row-major vector codes into query-major dots. Returns `false`,
    /// without touching `scores`, when this worker must use the SIMD fallback.
    ///
    /// # Panics
    /// Panics unless `vectors` contains complete rows of the prepared dimension
    /// and `scores` has one entry per query and vector.
    pub(super) fn score(&self, vectors: &[u8], scores: &mut [i32]) -> bool {
        self.score_with_permission(vectors, scores, thread_ready)
    }

    /// Validate buffer shapes and use AMX only for at least 16 queries and points.
    /// Small blocks skip `ready`; a rejected readiness check leaves output intact.
    /// The callback must verify CPU, OS state, and permission on the current thread
    /// before returning `true`; tests can return `false` to exercise fallback.
    fn score_with_permission(
        &self,
        vectors: &[u8],
        scores: &mut [i32],
        ready: impl FnOnce() -> bool,
    ) -> bool {
        assert!(vectors.len().is_multiple_of(self.dimension));
        let points = vectors.len() / self.dimension;
        assert_eq!(scores.len(), self.query_count * points);
        if points < TILE || self.query_count < TILE || !ready() {
            return false;
        }
        #[cfg(all(target_arch = "x86_64", target_os = "linux"))]
        {
            // SAFETY: thread_ready checks the CPU, OS state and Linux permission.
            // All buffers are validated above; the kernel pads its edge tiles.
            unsafe { self.score_ready(vectors, points, scores) };
            true
        }
        #[cfg(not(all(target_arch = "x86_64", target_os = "linux")))]
        false
    }

    /// Score complete or padded gallery tiles, then transpose live results into
    /// query-major output. Tile configuration is loaded and released for this call.
    ///
    /// # Safety
    /// The current thread must have passed the AMX capability and permission checks.
    /// The prepared dimension must be nonzero, `vectors.len()` must equal
    /// `points * self.dimension`, and `scores.len()` must equal
    /// `self.query_count * points`. Query panels must retain the layout from `pack`.
    #[cfg(all(target_arch = "x86_64", target_os = "linux"))]
    unsafe fn score_ready(&self, vectors: &[u8], points: usize, scores: &mut [i32]) {
        use std::borrow::Cow;

        let padded_dimension = self.depth_blocks * DEPTH;
        // Full tiles read the existing row-major buffer directly. Edge rows and
        // depth tails are zero-filled: tileloadd never reads past a vector.
        let gallery = if self.dimension == padded_dimension && points.is_multiple_of(TILE) {
            Cow::Borrowed(vectors)
        } else {
            let mut padded = vec![0; points.next_multiple_of(TILE) * padded_dimension];
            for (src, dst) in vectors
                .chunks_exact(self.dimension)
                .zip(padded.chunks_exact_mut(padded_dimension))
            {
                dst[..self.dimension].copy_from_slice(src);
            }
            Cow::Owned(padded)
        };
        let mut dots = [0i32; QUERY_TILES * TILE * TILE];
        // Tile configuration is not cached across calls: another AMX user on
        // this worker may have changed it. Release also on an unwinding exit.
        // SAFETY: the caller checked all AMX prerequisites on this OS thread.
        let _tiles = unsafe { Tiles::load() };
        for first_point in (0..points).step_by(TILE) {
            for first_query in (0..self.query_count).step_by(QUERY_BLOCK) {
                let live_queries = (self.query_count - first_query).min(QUERY_BLOCK);
                let tiles = live_queries.div_ceil(TILE);
                // SAFETY: gallery has complete 16-row, 64-byte depth tiles;
                // packed has four B tiles for every depth block, including
                // zero-filled missing queries. dots holds four output tiles.
                unsafe {
                    let a = gallery.as_ptr().add(first_point * padded_dimension);
                    let b = self.packed.as_ptr().add(
                        first_query / QUERY_BLOCK * self.depth_blocks * QUERY_TILES * TILE_BYTES,
                    );
                    match tiles {
                        1 => dot_tiles::<1>(a, padded_dimension, b, self.depth_blocks, &mut dots),
                        2 => dot_tiles::<2>(a, padded_dimension, b, self.depth_blocks, &mut dots),
                        3 => dot_tiles::<3>(a, padded_dimension, b, self.depth_blocks, &mut dots),
                        4 => dot_tiles::<4>(a, padded_dimension, b, self.depth_blocks, &mut dots),
                        _ => unreachable!(),
                    }
                }
                for query in 0..live_queries {
                    let tile = query / TILE;
                    let column = query % TILE;
                    let dst = (first_query + query) * points + first_point;
                    for point in 0..(points - first_point).min(TILE) {
                        scores[dst + point] = dots[tile * TILE * TILE + point * TILE + column];
                    }
                }
            }
        }
    }
}

/// Cache CPU and XCR0 tile-state support without requesting Linux permission.
/// XGETBV is reached only after CPUID confirms XSAVE and OSXSAVE support.
#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
fn hardware_supported() -> bool {
    use std::arch::x86_64::{__cpuid, __cpuid_count, _xgetbv};
    use std::sync::OnceLock;

    static SUPPORTED: OnceLock<bool> = OnceLock::new();
    *SUPPORTED.get_or_init(|| {
        // SAFETY: CPUID is available on x86_64. XGETBV is executed only after
        // the CPU and OS advertise XSAVE and OSXSAVE support.
        unsafe {
            if __cpuid(0).eax < 7 {
                return false;
            }
            let xsave = (1 << 26) | (1 << 27);
            if __cpuid(1).ecx & xsave != xsave {
                return false;
            }
            let amx = (1 << 24) | (1 << 25); // AMX-TILE and AMX-INT8
            if __cpuid_count(7, 0).edx & amx != amx {
                return false;
            }
            _xgetbv(0) & TILE_STATE == TILE_STATE
        }
    })
}

/// Report AMX unavailable outside Linux x86_64 so callers retain their fallback.
#[cfg(not(all(target_arch = "x86_64", target_os = "linux")))]
fn hardware_supported() -> bool {
    false
}

#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
const TILE_STATE: u64 = (1 << 17) | (1 << 18);

/// Check process tile-state permission, requesting XTILEDATA only when needed.
/// Cache success or failure on this worker; unsupported state or syscall failure
/// returns `false` before tile instructions. Linux grants permission process-wide,
/// while each worker allocates its extended state on first tile use.
#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
fn thread_ready() -> bool {
    use std::cell::Cell;

    use nix::libc::{SYS_arch_prctl, syscall};

    thread_local! {
        static READY: Cell<Option<bool>> = const { Cell::new(None) };
    }
    if !hardware_supported() {
        return false;
    }
    READY.with(|ready| {
        if let Some(value) = ready.get() {
            return value;
        }
        const ARCH_GET_XCOMP_SUPP: i32 = 0x1021;
        const ARCH_GET_XCOMP_PERM: i32 = 0x1022;
        const ARCH_REQ_XCOMP_PERM: i32 = 0x1023;
        const XFEATURE_XTILEDATA: usize = 18;
        // Linux grants permission to the process, but each task allocates its
        // extended state on first use. Check lazily on each search-pool thread;
        // denied permission must select the normal blocked-scoring fallback.
        // SAFETY: GET arguments point to writable u64 values. REQ takes the
        // highest required xstate component number, not a pointer or mask.
        let allowed = unsafe {
            let mut supported = 0u64;
            let mut permitted = 0u64;
            syscall(SYS_arch_prctl, ARCH_GET_XCOMP_SUPP, &mut supported) == 0
                && supported & TILE_STATE == TILE_STATE
                && syscall(SYS_arch_prctl, ARCH_GET_XCOMP_PERM, &mut permitted) == 0
                && (permitted & TILE_STATE == TILE_STATE
                    || syscall(SYS_arch_prctl, ARCH_REQ_XCOMP_PERM, XFEATURE_XTILEDATA) == 0)
        };
        ready.set(Some(allowed));
        allowed
    })
}

/// Keep workers on the fallback path when Linux AMX permission checks are unavailable.
#[cfg(not(all(target_arch = "x86_64", target_os = "linux")))]
fn thread_ready() -> bool {
    false
}

#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
#[repr(C, align(64))]
struct TileConfig {
    palette: u8,
    start_row: u8,
    reserved0: [u8; 14],
    column_bytes: [u16; 8],
    reserved1: [u8; 16],
    rows: [u8; 8],
    reserved2: [u8; 8],
}

#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
struct Tiles;

#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
impl Tiles {
    /// Load the palette-1 configuration with 16 rows and 64 bytes per tile row.
    /// The returned guard releases tile state when it leaves scope.
    ///
    /// # Safety
    /// AMX hardware/OS support and Linux permission must be established on the
    /// current thread. Loading this configuration replaces its previous tile state.
    unsafe fn load() -> Self {
        const CONFIG: TileConfig = TileConfig {
            palette: 1,
            start_row: 0,
            reserved0: [0; 14],
            column_bytes: [64; 8],
            reserved1: [0; 16],
            rows: [16; 8],
            reserved2: [0; 8],
        };
        // SAFETY: caller checked AMX availability and permission. CONFIG is a
        // valid 64-byte palette-1 configuration; all reserved bytes are zero.
        unsafe {
            std::arch::asm!("ldtilecfg [{config}]", config = in(reg) &CONFIG, options(nostack));
        }
        Self
    }
}

#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
impl Drop for Tiles {
    /// Release the scoring guard's tile configuration and data on the current thread.
    fn drop(&mut self) {
        // SAFETY: Tiles is constructed only after the runtime checks succeed.
        unsafe { std::arch::asm!("tilerelease", options(nostack)) };
    }
}

/// One gallery tile against 1..=4 query tiles, keeping every accumulator live
/// across the depth loop. The A tile is loaded once for each group of queries.
/// Writes `N` consecutive 16-by-16 i32 tiles in gallery-row/query-column order.
///
/// # Safety
/// `N` must be in `1..=4` and `depth_blocks` must be nonzero. The current thread
/// must have AMX permission and the configuration installed by `Tiles::load`.
/// `gallery` must expose 16 rows, each holding `depth_blocks * 64` readable bytes
/// with the supplied byte stride. `queries` must expose four packed 1024-byte
/// tiles per depth block in `pack` order. Inputs must not alias `output`.
#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
unsafe fn dot_tiles<const N: usize>(
    gallery: *const u8,
    gallery_stride: usize,
    queries: *const u8,
    depth_blocks: usize,
    output: &mut [i32; QUERY_TILES * TILE * TILE],
) {
    // SAFETY: the caller provides padded full tiles and the active config.
    // All tile operations for this product are in one assembly block; no
    // external call can change the tile state between zeroing and storing.
    unsafe {
        std::arch::asm!(
            "tilezero tmm4",
            ".if {tiles} > 1", "tilezero tmm5", ".endif",
            ".if {tiles} > 2", "tilezero tmm6", ".endif",
            ".if {tiles} > 3", "tilezero tmm7", ".endif",
            "2:",
            "tileloadd tmm0, [{a} + {a_stride}*1]",
            "tileloadd tmm1, [{b} + {tile_stride}*1]",
            "tdpbusd tmm4, tmm0, tmm1",
            ".if {tiles} > 1",
            "tileloadd tmm1, [{b} + {tile_stride}*1 + 1024]",
            "tdpbusd tmm5, tmm0, tmm1",
            ".endif",
            ".if {tiles} > 2",
            "tileloadd tmm1, [{b} + {tile_stride}*1 + 2048]",
            "tdpbusd tmm6, tmm0, tmm1",
            ".endif",
            ".if {tiles} > 3",
            "tileloadd tmm1, [{b} + {tile_stride}*1 + 3072]",
            "tdpbusd tmm7, tmm0, tmm1",
            ".endif",
            "add {a}, 64",
            "add {b}, 4096",
            "dec {blocks}",
            "jnz 2b",
            "tilestored [{out} + {tile_stride}*1], tmm4",
            ".if {tiles} > 1",
            "tilestored [{out} + {tile_stride}*1 + 1024], tmm5",
            ".endif",
            ".if {tiles} > 2",
            "tilestored [{out} + {tile_stride}*1 + 2048], tmm6",
            ".endif",
            ".if {tiles} > 3",
            "tilestored [{out} + {tile_stride}*1 + 3072], tmm7",
            ".endif",
            tiles = const N,
            a = inout(reg) gallery => _,
            b = inout(reg) queries => _,
            blocks = inout(reg) depth_blocks => _,
            a_stride = in(reg) gallery_stride,
            tile_stride = in(reg) DEPTH,
            out = in(reg) output.as_mut_ptr(),
            options(nostack),
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    /// Generate deterministic 0..=127 codes varying across queries and depth positions.
    fn queries(count: usize, dimension: usize) -> Vec<EncodedQueryU8> {
        (0..count)
            .map(|q| EncodedQueryU8 {
                offset: 0.0,
                encoded_query: (0..dimension)
                    .map(|k| ((q * 37 + k * 17 + k / 5) % 128) as u8)
                    .collect(),
            })
            .collect()
    }

    /// Compare one shape with an i64 scalar oracle, checking canaries and repeated
    /// calls after tile release. Skip when AMX hardware or permission is unavailable.
    fn check_shape(count: usize, dimension: usize, points: usize) {
        let queries = queries(count, dimension);
        let Some(prepared) = AmxQueries::new(&queries, dimension) else {
            return;
        };
        let vectors: Vec<u8> = (0..points * dimension)
            .map(|i| ((i * 43 + i / dimension * 11) % 128) as u8)
            .collect();
        let mut guarded = vec![i32::MIN; count * points + 2];
        if !prepared.score(&vectors, &mut guarded[1..1 + count * points]) {
            return;
        }
        assert_eq!(guarded[0], i32::MIN);
        assert_eq!(guarded[count * points + 1], i32::MIN);
        for (q, query) in queries.iter().enumerate() {
            for (p, vector) in vectors.chunks_exact(dimension).enumerate() {
                let expected: i64 = query
                    .encoded_query
                    .iter()
                    .zip(vector)
                    .map(|(&a, &b)| i64::from(a) * i64::from(b))
                    .sum();
                assert_eq!(guarded[1 + q * points + p], expected as i32);
            }
        }
        // A second call must reload its configuration after the first call's
        // TILERELEASE; prepared queries may outlive an AMX hardware context.
        let mut again = vec![0; count * points];
        assert!(prepared.score(&vectors, &mut again));
        assert_eq!(again, guarded[1..1 + count * points]);
    }

    /// Verify full and partial query/point tiles and depth tails against the oracle.
    #[test]
    fn amx_dots_match_integer_oracle_with_edge_tiles() {
        for dimension in [16, 48, 64, 80, 128, 512, 528, 1024] {
            for (queries, points) in [(16, 16), (17, 31), (31, 17), (48, 63), (63, 64), (65, 65)] {
                check_shape(queries, dimension, points);
            }
        }
    }

    /// Verify denied permission preserves output and small blocks never consult
    /// the permission callback, without requiring AMX hardware.
    #[test]
    fn amx_denied_permission_and_small_batches_leave_output_untouched() {
        let queries = queries(16, 64);
        let prepared = AmxQueries::pack(&queries, 64);
        let mut scores = vec![123; 16 * 16];
        assert!(!prepared.score_with_permission(&vec![1; 16 * 64], &mut scores, || false));
        assert!(scores.iter().all(|&score| score == 123));
        assert!(
            !prepared.score_with_permission(&[1; 64], &mut scores[..16], || {
                panic!("small point batches must not request AMX permission")
            })
        );
        let small = AmxQueries::pack(&queries[..1], 64);
        assert!(
            !small.score_with_permission(&vec![1; 16 * 64], &mut scores[..16], || {
                panic!("small query batches must not request AMX permission")
            })
        );
        assert!(scores.iter().all(|&score| score == 123));
    }

    /// Share packed queries across four fresh workers and verify their dot products.
    /// Skip on unavailable AMX; otherwise each worker must pass its readiness check.
    #[test]
    fn amx_prepared_queries_work_on_multiple_threads() {
        if !thread_ready() {
            return;
        }
        let queries = queries(17, 80);
        let Some(prepared) = AmxQueries::new(&queries, 80) else {
            return;
        };
        let prepared = Arc::new(prepared);
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let prepared = Arc::clone(&prepared);
                std::thread::spawn(move || {
                    let mut scores = vec![0; 17 * 17];
                    assert!(prepared.score(&vec![1; 17 * 80], &mut scores));
                    scores
                })
            })
            .collect();
        for worker in workers {
            let scores = worker.join().unwrap();
            for (q, query) in queries.iter().enumerate() {
                let expected: i32 = query.encoded_query.iter().map(|&v| i32::from(v)).sum();
                assert!(scores[q * 17..(q + 1) * 17].iter().all(|&v| v == expected));
            }
        }
    }
}
