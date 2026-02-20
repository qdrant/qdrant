use log::info;
use rand::Rng;
use std::env;
use std::fmt;

use rand;
use rand::SeedableRng;
use rand_distr::{Distribution, StandardUniform};

use crate::segment;

pub struct EntryGenerator {
    seed: usize,

    /// Random number generator.
    rng: rand::rngs::StdRng,

    /// Gamma(shape=1.25, scale=25.6) distribution for generating entry sizes.
    /// Produces values with a mean of 32 bytes and a median of 24 bytes. See
    /// http://wolfr.am/8r3vGRkZ.
    dist: rand_distr::Gamma<f32>,

    remaining_size: usize,
}

impl EntryGenerator {
    pub fn new() -> EntryGenerator {
        EntryGenerator::with_segment_capacity(usize::MAX)
    }

    pub fn with_seed(seed: usize) -> EntryGenerator {
        EntryGenerator::with_seed_and_segment_capacity(seed, usize::MAX)
    }

    pub fn with_segment_capacity(size: usize) -> EntryGenerator {
        let seed: usize = env::var("WAL_TEST_SEED")
            .map(|seed| seed.parse::<usize>().unwrap())
            .unwrap_or_else(|_| rand::rng().random_range(0..usize::MAX));
        EntryGenerator::with_seed_and_segment_capacity(seed, size)
    }

    pub fn with_seed_and_segment_capacity(seed: usize, size: usize) -> EntryGenerator {
        info!("Creating EntryGenerator with seed {seed}");
        EntryGenerator {
            seed,
            rng: rand::rngs::StdRng::seed_from_u64(seed as u64),
            dist: rand_distr::Gamma::new(1.25, 25.6).unwrap(),
            remaining_size: size.saturating_sub(segment::segment_overhead()),
        }
    }

    pub fn seed(&self) -> usize {
        self.seed
    }
}

impl Default for EntryGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl Iterator for EntryGenerator {
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Vec<u8>> {
        let size = self.dist.sample(&mut self.rng) as usize;
        let padded_size = size + segment::entry_overhead(size);
        if self.remaining_size >= padded_size {
            self.remaining_size -= padded_size;
            Some(
                StandardUniform
                    .sample_iter(&mut self.rng)
                    .take(size)
                    .collect(),
            )
        } else {
            self.remaining_size = 0;
            None
        }
    }
}

impl fmt::Debug for EntryGenerator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "EntryGenerator {{ seed: {}, remaining_size: {} }}",
            self.seed, self.remaining_size
        )
    }
}

#[cfg(test)]
mod test {

    use super::EntryGenerator;
    use crate::segment;

    #[test]
    fn entry_generator_distribution() {
        let generator = EntryGenerator::new();
        let mut sizes: Vec<usize> = generator
            .into_iter()
            .take(1000)
            .map(|entry| entry.len())
            .collect();

        sizes.sort();

        let sum: usize = sizes.iter().sum();
        let mean = sum as f64 / sizes.len() as f64;
        let median = sizes[sizes.len() / 2];

        println!("median: {median}, mean: {mean}");

        assert!(median >= 18);
        assert!(median <= 30);

        assert!(mean >= 26.0);
        assert!(mean <= 38.0);
    }

    #[test]
    fn entry_generator_size() {
        let generator = EntryGenerator::with_segment_capacity(1023);
        let len = generator
            .into_iter()
            .fold(segment::segment_overhead(), |acc, entry| {
                let len = entry.len();
                acc + len + segment::entry_overhead(len)
            });

        assert!(len <= 1023);
    }

    #[test]
    fn test_seed() {
        let gen1 = EntryGenerator::with_seed(42);
        let gen2 = EntryGenerator::with_seed(42);

        assert_eq!(
            gen1.into_iter().take(100).collect::<Vec<_>>(),
            gen2.into_iter().take(100).collect::<Vec<_>>()
        );
    }
}
