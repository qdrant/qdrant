use std::cell::LazyCell;
use std::hint::black_box;
use std::rc::Rc;

use common::bitpacking::{BitReader, BitWriter};
use common::bitpacking_links::iterate_packed_links;
use common::bitpacking_ordered;
use itertools::Itertools as _;
use rand::rngs::StdRng;
use rand::{Rng as _, SeedableRng as _};
use tango_bench::{
    Bencher, Benchmark, ErasedSampler, IntoBenchmarks, benchmark_fn, tango_benchmarks, tango_main,
};
use zerocopy::IntoBytes;

pub fn benchmarks_bitpacking() -> impl IntoBenchmarks {
    let data8 = StateBencher::new(move || {
        let mut rng = StdRng::seed_from_u64(42);
        (0..64_000_000).map(|_| rng.random()).collect::<Vec<u8>>()
    });
    let data32 = StateBencher::new(move || {
        let mut rng = StdRng::seed_from_u64(42);
        (0..4_000_000).map(|_| rng.random()).collect::<Vec<u32>>()
    });

    [
        data8.benchmark_fn("bitpacking/read", move |b, data8| {
            let mut rng = StdRng::seed_from_u64(42);
            b.iter(move || {
                let bits = rng.random_range(1..=32);
                let bytes = rng.random_range(0..=16);
                let start = rng.random_range(0..data8.len() - bytes);
                let data = &data8[start..start + bytes];

                let mut r = BitReader::new(data);
                r.set_bits(bits);
                for _ in 0..(data.len() * u8::BITS as usize / bits as usize) {
                    black_box(r.read::<u32>());
                }
            })
        }),
        data32.benchmark_fn("bitpacking/write", move |b, data32| {
            let mut rng = StdRng::seed_from_u64(42);
            let mut out = Vec::new();
            b.iter(move || {
                let bits = rng.random_range(1..=32);
                let values = rng.random_range(0..=16);
                let start = rng.random_range(0..data32.len() - values);
                let data = &data32[start..start + values];

                out.clear();
                let mut w = BitWriter::new(&mut out);
                for &x in data {
                    w.write(x, bits);
                }
                w.finish();
                black_box(&mut out);
            })
        }),
    ]
}

fn benchmarks_bitpacking_links() -> impl IntoBenchmarks {
    struct Item {
        offset: usize,
        bits_per_unsorted: u8,
        sorted_count: usize,
    }
    struct State {
        links: Vec<u8>,
        items: Vec<Item>,
    }

    let b = StateBencher::new(move || {
        Rc::new({
            let mut rng = StdRng::seed_from_u64(42);
            let mut links = Vec::new();
            let mut pos = vec![Item {
                offset: 0,
                bits_per_unsorted: 0,
                sorted_count: 0,
            }];
            while links.len() <= 64_000_000 {
                let bits_per_unsorted = rng.random_range(7..=32);
                let sorted_count = rng.random_range(0..100);
                let unsorted_count = rng.random_range(0..100);
                if 1 << bits_per_unsorted < sorted_count + unsorted_count {
                    continue;
                }

                common::bitpacking_links::pack_links(
                    &mut links,
                    std::iter::repeat_with(|| {
                        rng.random_range(0..1u64 << bits_per_unsorted) as u32
                    })
                    .unique()
                    .take(sorted_count + unsorted_count)
                    .collect(),
                    bits_per_unsorted,
                    sorted_count,
                );
                pos.push(Item {
                    offset: links.len(),
                    bits_per_unsorted,
                    sorted_count,
                });
            }

            State { links, items: pos }
        })
    });

    [b.benchmark_fn("bitpacking_links/read", move |b, state| {
        let mut rng = rand::rng();
        b.iter(move || {
            let idx = rng.random_range(1..state.items.len());
            iterate_packed_links(
                &state.links[state.items[idx - 1].offset..state.items[idx].offset],
                state.items[idx].bits_per_unsorted,
                state.items[idx].sorted_count,
            )
            .for_each(|x| {
                black_box(x);
            });
        })
    })]
}

fn benchmarks_ordered() -> impl IntoBenchmarks {
    struct StateOwner {
        values: Vec<u64>,
        compressed: Vec<u8>,
    }

    struct StateDependent<'a> {
        decompressor: bitpacking_ordered::Reader<'a>,
    }

    self_cell::self_cell! {
        struct State {
            owner: StateOwner,
            #[covariant]
            dependent: StateDependent,
        }
    }

    let b = StateBencher::new(move || {
        let values =
            bitpacking_ordered::gen_test_sequence(&mut StdRng::seed_from_u64(42), 32, 1 << 22);

        let (compressed, parameters) = bitpacking_ordered::compress(&values);

        State::new(StateOwner { values, compressed }, |owner| {
            let (decompressor, _) =
                bitpacking_ordered::Reader::new(parameters, &owner.compressed).unwrap();
            println!(
                "Original size: {:.1} MB, compressed size: {:.1} MB, {:?}",
                owner.values.as_bytes().len() as f64 / 1e6,
                owner.compressed.len() as f64 / 1e6,
                decompressor.parameters(),
            );
            StateDependent { decompressor }
        })
    });

    [
        b.benchmark_fn("ordered/get", {
            move |b, state| {
                let mut rng = rand::rng();
                let len = state.borrow_owner().values.len() - 1;
                b.iter(move || {
                    let i = rng.random_range(0..len);
                    black_box(state.borrow_dependent().decompressor.get(i));
                })
            }
        }),
        b.benchmark_fn("ordered/get2", {
            move |b, state| {
                let mut rng = rand::rng();
                let len = state.borrow_owner().values.len() - 1;
                b.iter(move || {
                    let i = rng.random_range(0..len);
                    let a = state.borrow_dependent().decompressor.get(i);
                    let b = state.borrow_dependent().decompressor.get(i + 1);
                    black_box((a, b));
                })
            }
        }),
    ]
}

#[expect(clippy::type_complexity)]
struct StateBencher<T>(Rc<LazyCell<Rc<T>, Box<dyn FnOnce() -> Rc<T>>>>);

impl<T: 'static> StateBencher<T> {
    fn new<F: FnOnce() -> T + 'static>(f: F) -> Self {
        Self(Rc::new(LazyCell::new(Box::new(move || Rc::new(f())))))
    }

    pub fn benchmark_fn<F: FnMut(Bencher, Rc<T>) -> Box<dyn ErasedSampler> + 'static>(
        &self,
        name: impl Into<String>,
        mut sampler_factory: F,
    ) -> Benchmark {
        let state = Rc::clone(&self.0);
        benchmark_fn(name, move |b| {
            let state = Rc::clone(LazyCell::force(&state));
            sampler_factory(b, state)
        })
    }
}

tango_benchmarks!(
    benchmarks_bitpacking(),
    benchmarks_bitpacking_links(),
    benchmarks_ordered()
);
tango_main!();
