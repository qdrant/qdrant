//! Measures the two hardware inputs of the prefetch-window formula
//! `look-ahead bytes ≈ memory latency × kernel consumption rate`, so
//! per-machine prefetch look-ahead constants can be checked instead of
//! trusted from one calibration box.
//!
//! Part A — memory latency: dependent pointer chase over a single random
//! cycle of cache lines in a buffer far larger than L3 (default 512 MiB).
//! Each hop's address depends on the previous load, so nothing overlaps and
//! ns/hop is the full load-to-use latency, including TLB effects — the same
//! conditions a scoring loop's first-touch loads see. A small L3-resident
//! control buffer sanity-checks the methodology.
//!
//! Part B — kernel consumption rates: the production scoring kernels
//! (TurboQuant 4-bit and 1-bit, binary 1-bit and 2-bit with u128 words,
//! int8 scalar) over a cache-resident working set, in production batch shape. Warm data means no
//! memory stalls: the measured ns/vector is pure compute + loop glue, i.e.
//! the consumption rate the formula needs.
//!
//! Part C — the same two measurements under load: background aggressor
//! threads (default: all remaining cores) each issue batches of independent
//! random cache-line reads over their own DRAM-sized buffers — the scattered,
//! high-MLP traffic shape of contended candidate fills, deliberately not
//! streaming (which would measure the bandwidth wall instead of queueing).
//! Loaded latency × loaded kernel rate gives the look-ahead required under
//! contention; idle inputs alone can under-size it.
//!
//! The final table multiplies latency and rate into a predicted look-ahead
//! per code size — idle and loaded — next to a reference two-level window
//! schedule (1 KiB near / 4 KiB far).
//!
//! Usage:
//!   cargo run -p segment --profile perf --example latency_kernel_probe
//! Env: PROBE_LAT_MB (512), PROBE_HOPS (16777216), PROBE_ROUNDS (3),
//!      PROBE_KERNEL_SECS (2), PROBE_DIMS (64,128,512,768,1024,2048,4096),
//!      PROBE_TYPES (turbo4,turbo1,bq,bq2,int8),
//!      PROBE_LOAD_THREADS (cores - 1; 0 skips part C), PROBE_LOAD_MB (128)

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use common::counter::hardware_counter::HardwareCounterCell;
use quantization::encoded_storage::{EncodedStorage, TestEncodedStorageBuilder};
use quantization::encoded_vectors_binary::{self, EncodedVectorsBin, Encoding, QueryEncoding};
use quantization::encoded_vectors_u8::{self, EncodedVectorsU8, ScalarQuantizationMethod};
use quantization::turboquant::quantization::TurboQuantizer;
use quantization::turboquant::{TQBits, TQMode, TQRotation};
use quantization::{DistanceType, EncodedVectors, VectorParameters};
use rand::prelude::*;
use rand::rngs::SmallRng;

const SEED: u64 = 42;
const CACHE_LINE: usize = 64;
/// Mirrors `VECTOR_READ_BATCH_SIZE`: production scores candidates in batches.
const BATCH: usize = 64;

/// Reference two-level prefetch schedule the final table compares against:
/// a near (L1) window covering `NEAR_BYTES` and a far (L2) window covering
/// `FAR_BYTES` of look-ahead, both expressed in whole vectors, with no far
/// window for sub-cache-line codes. Kept local so this tool stands alone;
/// update it if the production schedule changes.
const NEAR_BYTES: usize = 1024;
const FAR_BYTES: usize = 4096;

fn reference_windows(vector_size_bytes: usize) -> (usize, usize) {
    let near = (NEAR_BYTES / vector_size_bytes.max(1)).clamp(1, 8);
    if vector_size_bytes < CACHE_LINE {
        return (near, 0);
    }
    let far = (FAR_BYTES / vector_size_bytes).clamp(near + 2, 16);
    (near, far)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be an integer, got {value:?}"))
    })
}

/// One random cycle through all cache lines of a `mb` MiB buffer: buf holds
/// one u64 next-line index at the start of each 64-B line.
fn build_chase(mb: usize, rng: &mut SmallRng) -> Vec<u64> {
    let lines = mb * 1024 * 1024 / CACHE_LINE;
    let mut order: Vec<u32> = (0..lines as u32).collect();
    order.shuffle(rng);
    let mut buf = vec![0u64; lines * (CACHE_LINE / 8)];
    for k in 0..lines {
        let from = order[k] as usize;
        let to = order[(k + 1) % lines];
        buf[from * (CACHE_LINE / 8)] = u64::from(to);
    }
    buf
}

/// ns per dependent load. The chain forces one full memory round trip per hop.
fn chase_ns_per_hop(buf: &[u64], hops: usize) -> f64 {
    let mut cur: u64 = 0;
    let start = Instant::now();
    for _ in 0..hops {
        cur = buf[cur as usize * (CACHE_LINE / 8)];
    }
    let elapsed = start.elapsed();
    black_box(cur);
    elapsed.as_secs_f64() * 1e9 / hops as f64
}

fn measure_chase(buf: &[u64], label: &str, hops: usize, rounds: usize) -> f64 {
    chase_ns_per_hop(buf, hops / 4); // warm-up: page-in + TLB/predictor settle
    let mut samples: Vec<f64> = (0..rounds).map(|_| chase_ns_per_hop(buf, hops)).collect();
    samples.sort_by(f64::total_cmp);
    for (idx, ns) in samples.iter().enumerate() {
        println!("latency {label} round{idx}: {ns:.1} ns/hop");
    }
    samples[samples.len() / 2]
}

fn measure_latency(mb: usize, hops: usize, rounds: usize, rng: &mut SmallRng) -> f64 {
    let buf = build_chase(mb, rng);
    measure_chase(&buf, &format!("{mb}MiB"), hops, rounds)
}

/// Background memory aggressors: each thread issues batches of 8 independent
/// random cache-line reads over its own private buffer — many misses in
/// flight per thread (scattered candidate-fill shape), unlike a dependent
/// chase (1 miss) or streaming (HW-prefetched bandwidth).
struct LoadGen {
    stop: Arc<AtomicBool>,
    handles: Vec<std::thread::JoinHandle<u64>>,
    started: Instant,
}

fn start_load(threads: usize, mb: usize) -> LoadGen {
    let stop = Arc::new(AtomicBool::new(false));
    // +1: the main thread also waits for all buffers to be paged in before
    // any loaded measurement starts.
    let barrier = Arc::new(Barrier::new(threads + 1));
    let handles = (0..threads)
        .map(|tid| {
            let stop = Arc::clone(&stop);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let words = mb * 1024 * 1024 / 8;
                let lines = words / (CACHE_LINE / 8);
                assert!(
                    lines.is_power_of_two(),
                    "PROBE_LOAD_MB must be a power of two"
                );
                // Distinct written values force real physical pages (an
                // untouched zeroed allocation would alias the kernel's shared
                // zero page and turn every "miss" into a cache hit).
                let buf: Vec<u64> = (0..words).map(|w| w as u64).collect();
                // 8 xorshift streams -> 8 independent loads per batch.
                let mut states: [u64; 8] = std::array::from_fn(|lane| {
                    SEED ^ ((tid * 8 + lane) as u64).wrapping_mul(0x9e3779b97f4a7c15) | 1
                });
                let mut acc = 0u64;
                let mut touched = 0u64;
                barrier.wait();
                while !stop.load(Ordering::Relaxed) {
                    for _ in 0..64 {
                        for state in &mut states {
                            *state ^= *state << 13;
                            *state ^= *state >> 7;
                            *state ^= *state << 17;
                            let line = (*state as usize) & (lines - 1);
                            acc = acc.wrapping_add(buf[line * (CACHE_LINE / 8)]);
                        }
                        touched += 8;
                    }
                }
                black_box(acc);
                touched
            })
        })
        .collect();
    barrier.wait();
    LoadGen {
        stop,
        handles,
        started: Instant::now(),
    }
}

impl LoadGen {
    /// Stops the aggressors and returns their aggregate read bandwidth in
    /// GB/s — context for how close to the wall the loaded numbers were taken.
    fn stop(self) -> f64 {
        self.stop.store(true, Ordering::Relaxed);
        let elapsed = self.started.elapsed().as_secs_f64();
        let touched: u64 = self.handles.into_iter().map(|h| h.join().unwrap()).sum();
        touched as f64 * CACHE_LINE as f64 / elapsed / 1e9
    }
}

#[derive(Clone, Copy)]
enum Datatype {
    Turbo4,
    Turbo1,
    Bq,
    Bq2,
    Int8,
}

impl Datatype {
    fn label(self) -> &'static str {
        match self {
            Datatype::Turbo4 => "turbo4",
            Datatype::Turbo1 => "turbo1",
            Datatype::Bq => "bq",
            Datatype::Bq2 => "bq2",
            Datatype::Int8 => "int8",
        }
    }

    fn parse(name: &str) -> Self {
        match name {
            "turbo4" => Datatype::Turbo4,
            "turbo1" => Datatype::Turbo1,
            "bq" => Datatype::Bq,
            "bq2" => Datatype::Bq2,
            "int8" => Datatype::Int8,
            other => panic!("PROBE_TYPES entry {other:?}, expected turbo4|turbo1|bq|bq2|int8"),
        }
    }
}

struct KernelCell {
    datatype: Datatype,
    dim: usize,
    vec_bytes: usize,
    /// Size of the code buffer the timed loop cycles over — printed so
    /// cache residency is auditable against the machine's private L1+L2.
    ws_bytes: usize,
    ns_per_vec: f64,
    bytes_per_ns: f64,
}

fn random_vec(rng: &mut SmallRng, dim: usize) -> Vec<f32> {
    (0..dim).map(|_| rng.random::<f32>() - 0.5).collect()
}

/// Working set capped at 256 KiB so it settles into L1/L2 after warm-up;
/// power-of-two count for mask indexing.
fn working_set_count(vec_bytes: usize) -> usize {
    (256 * 1024 / vec_bytes).clamp(64, 1024).next_power_of_two() / 2
}

/// The shared timing loop: score random ids from a flat cache-resident code
/// buffer in production batch shape, return ns per scored vector.
fn run_rate(
    buf: &[u8],
    vec_bytes: usize,
    count: usize,
    secs: u64,
    rng: &mut SmallRng,
    score: impl Fn(&[u8]) -> f32,
) -> f64 {
    let ids: Vec<u32> = (0..1 << 16)
        .map(|_| (rng.random::<u64>() as usize & (count - 1)) as u32)
        .collect();
    let run = |deadline: Instant| -> (u64, f64) {
        let start = Instant::now();
        let mut scored: u64 = 0;
        let mut acc: f32 = 0.0;
        'outer: loop {
            for (batch_idx, batch) in ids.chunks(BATCH).enumerate() {
                for &id in batch {
                    let at = id as usize * vec_bytes;
                    acc += score(&buf[at..at + vec_bytes]);
                }
                scored += batch.len() as u64;
                if batch_idx % 64 == 0 && Instant::now() >= deadline {
                    break 'outer;
                }
            }
        }
        black_box(acc);
        (scored, start.elapsed().as_secs_f64())
    };

    run(Instant::now() + Duration::from_millis(200)); // warm caches + branch predictors
    let (scored, elapsed) = run(Instant::now() + Duration::from_secs(secs));
    elapsed * 1e9 / scored as f64
}

/// Pure consumption rate of one production scoring kernel: cache-resident
/// codes, batch loop shape, no prefetch hints (hints on resident data are
/// near-free and would only blur the compute measurement).
fn measure_kernel(datatype: Datatype, dim: usize, secs: u64, rng: &mut SmallRng) -> KernelCell {
    let params = VectorParameters {
        dim,
        deprecated_count: None,
        distance_type: DistanceType::Dot,
        invert: false,
    };
    let stopped = AtomicBool::new(false);
    let hw = HardwareCounterCell::new();

    let (vec_bytes, ns_per_vec, ws_bytes) = match datatype {
        Datatype::Turbo4 | Datatype::Turbo1 => {
            let bits = match datatype {
                Datatype::Turbo4 => TQBits::Bits4,
                Datatype::Turbo1 => TQBits::Bits1,
                Datatype::Bq | Datatype::Bq2 | Datatype::Int8 => unreachable!(),
            };
            let quantizer = TurboQuantizer::new(
                dim,
                bits,
                TQMode::Normal,
                DistanceType::Dot,
                TQRotation::Unpadded,
                None,
            );
            let mut pad_buf = vec![0.0f64; quantizer.get_padded_dim()];
            let vec_bytes = quantizer
                .quantize(&random_vec(rng, dim), &mut pad_buf)
                .len();
            let count = working_set_count(vec_bytes);
            let mut buf: Vec<u8> = Vec::with_capacity(count * vec_bytes);
            for _ in 0..count {
                buf.extend_from_slice(&quantizer.quantize(&random_vec(rng, dim), &mut pad_buf));
            }
            let query = quantizer.precompute_query(&random_vec(rng, dim));
            let ns = run_rate(&buf, vec_bytes, count, secs, rng, |bytes| {
                quantizer.score_precomputed(&query, bytes)
            });
            (vec_bytes, ns, buf.len())
        }
        Datatype::Bq | Datatype::Bq2 => {
            let encoding = match datatype {
                Datatype::Bq => Encoding::OneBit,
                Datatype::Bq2 => Encoding::TwoBits,
                Datatype::Turbo4 | Datatype::Turbo1 | Datatype::Int8 => unreachable!(),
            };
            let qsize = encoded_vectors_binary::get_quantized_vector_size_from_params::<u128>(
                dim, encoding,
            );
            let count = working_set_count(qsize);
            let samples: Vec<Vec<f32>> = (0..count).map(|_| random_vec(rng, dim)).collect();
            let encoded = EncodedVectorsBin::<u128, _>::encode(
                samples.iter(),
                TestEncodedStorageBuilder::new(None, qsize),
                &params,
                encoding,
                QueryEncoding::SameAsStorage,
                None,
                &stopped,
            )
            .unwrap();
            let vec_bytes = encoded.quantized_vector_size();
            let mut buf: Vec<u8> = Vec::with_capacity(count * vec_bytes);
            for i in 0..count {
                buf.extend_from_slice(&encoded.storage().get_vector_data(i as u32));
            }
            let query = encoded.encode_query(&random_vec(rng, dim));
            let ns = run_rate(&buf, vec_bytes, count, secs, rng, |bytes| {
                encoded.score(&query, bytes, &hw)
            });
            (vec_bytes, ns, buf.len())
        }
        Datatype::Int8 => {
            let qsize = encoded_vectors_u8::get_quantized_vector_size(&params);
            let count = working_set_count(qsize);
            let samples: Vec<Vec<f32>> = (0..count).map(|_| random_vec(rng, dim)).collect();
            let encoded = EncodedVectorsU8::encode(
                samples.iter(),
                TestEncodedStorageBuilder::new(None, qsize),
                &params,
                count,
                None,
                ScalarQuantizationMethod::Int8,
                None,
                &stopped,
            )
            .unwrap();
            let vec_bytes = encoded.quantized_vector_size();
            let mut buf: Vec<u8> = Vec::with_capacity(count * vec_bytes);
            for i in 0..count {
                buf.extend_from_slice(&encoded.storage().get_vector_data(i as u32));
            }
            let query = encoded.encode_query(&random_vec(rng, dim));
            let ns = run_rate(&buf, vec_bytes, count, secs, rng, |bytes| {
                encoded.score(&query, bytes, &hw)
            });
            (vec_bytes, ns, buf.len())
        }
    };

    KernelCell {
        datatype,
        dim,
        vec_bytes,
        ws_bytes,
        ns_per_vec,
        bytes_per_ns: vec_bytes as f64 / ns_per_vec,
    }
}

fn measure_kernels(
    types: &[Datatype],
    dims: &[usize],
    secs: u64,
    rng: &mut SmallRng,
    prefix: &str,
) -> Vec<KernelCell> {
    types
        .iter()
        .flat_map(|&datatype| dims.iter().map(move |&dim| (datatype, dim)))
        .map(|(datatype, dim)| {
            let cell = measure_kernel(datatype, dim, secs, rng);
            println!(
                "{prefix}{:7} dim {:4}  code {:4} B  ws {:3} KiB  {:7.2} ns/vec  {:5.1} B/ns",
                cell.datatype.label(),
                cell.dim,
                cell.vec_bytes,
                cell.ws_bytes / 1024,
                cell.ns_per_vec,
                cell.bytes_per_ns
            );
            cell
        })
        .collect()
}

fn main() {
    let lat_mb = env_usize("PROBE_LAT_MB", 512);
    let hops = env_usize("PROBE_HOPS", 16 * 1024 * 1024);
    let rounds = env_usize("PROBE_ROUNDS", 3);
    let kernel_secs = env_usize("PROBE_KERNEL_SECS", 2) as u64;
    let dims: Vec<usize> = std::env::var("PROBE_DIMS")
        .unwrap_or_else(|_| "64,128,512,768,1024,2048,4096".to_string())
        .split(',')
        .map(|d| d.trim().parse().expect("PROBE_DIMS must be integers"))
        .collect();
    let types: Vec<Datatype> = std::env::var("PROBE_TYPES")
        .unwrap_or_else(|_| "turbo4,turbo1,bq,bq2,int8".to_string())
        .split(',')
        .map(|name| Datatype::parse(name.trim()))
        .collect();
    let mut rng = SmallRng::seed_from_u64(SEED);

    println!("== part A: dependent-load memory latency ==");
    let l3_control = measure_latency(4, hops, rounds, &mut rng);
    let dram_buf = build_chase(lat_mb, &mut rng);
    let dram = measure_chase(&dram_buf, &format!("{lat_mb}MiB"), hops, rounds);
    println!("control(4MiB, ~L3): {l3_control:.1} ns   DRAM({lat_mb}MiB): {dram:.1} ns");

    println!("\n== part B: kernel consumption rates (cache-resident) ==");
    let cells = measure_kernels(&types, &dims, kernel_secs, &mut rng, "");

    let load_threads = env_usize(
        "PROBE_LOAD_THREADS",
        std::thread::available_parallelism().map_or(0, |n| n.get().saturating_sub(1)),
    );
    let load_mb = env_usize("PROBE_LOAD_MB", 128);
    let loaded = (load_threads > 0).then(|| {
        println!(
            "\n== part C: loaded latency + kernel rates ({load_threads} aggressor threads x {load_mb} MiB, scattered reads) =="
        );
        let load = start_load(load_threads, load_mb);
        let dram_loaded = measure_chase(&dram_buf, "loaded", hops, rounds);
        let loaded_cells = measure_kernels(&types, &dims, kernel_secs, &mut rng, "loaded ");
        let gbps = load.stop();
        println!(
            "loaded DRAM: {dram_loaded:.1} ns (idle {dram:.1} ns, x{:.2});  aggressor read bandwidth {gbps:.1} GB/s aggregate",
            dram_loaded / dram,
        );
        (dram_loaded, loaded_cells)
    });

    println!("\n== formula check: predicted look-ahead = latency x rate ==");
    println!(
        "reference schedule: NEAR_BYTES={NEAR_BYTES} FAR_BYTES={FAR_BYTES}; coverage = eff_near x ns_per_vec / latency (<1 -> near alone too shallow)"
    );
    println!(
        "type,dim,code_bytes,eff_near,eff_far,idle_need_bytes,idle_coverage,loaded_need_bytes,loaded_coverage"
    );
    for (idx, cell) in cells.iter().enumerate() {
        let (near, far) = reference_windows(cell.vec_bytes);
        let coverage =
            |lat: f64, c: &KernelCell| (near as f64 * c.ns_per_vec / lat, lat * c.bytes_per_ns);
        let (idle_cov, idle_need) = coverage(dram, cell);
        let (loaded_need, loaded_cov) = match &loaded {
            Some((lat, loaded_cells)) => {
                let (cov, need) = coverage(*lat, &loaded_cells[idx]);
                (format!("{need:.0}"), format!("{cov:.2}"))
            }
            None => ("-".to_string(), "-".to_string()),
        };
        println!(
            "{},{},{},{},{},{:.0},{:.2},{},{}",
            cell.datatype.label(),
            cell.dim,
            cell.vec_bytes,
            near,
            far,
            idle_need,
            idle_cov,
            loaded_need,
            loaded_cov,
        );
    }
}
