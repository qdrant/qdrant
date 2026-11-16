//! HashPrune: a bounded reservoir of candidate neighbors per point.
//!
//! - A candidate's bucket is the hash of its residual against the point, from [`super::sketches`].
//! - Within a bucket, the better scoring candidate stays. With the reservoir full, a candidate in
//!   a new bucket replaces the worst one, if it scores better.
//! - Ties are broken by point id, so the reservoirs don't depend on the order of insertion, and
//!   with it on the order leaves are processed in.
//!
//! Reservoirs are the largest memory cost of the build, so they live in one flat allocation with a
//! fixed capacity per point, shared by all threads.
//!
//! # Order Independence
//!
//! Candidates are ordered by score, then by the lower id. Without NaN scores, and with both zeros
//! made one, that's a total order. A reservoir then always holds the best candidate of each of the
//! `capacity` buckets whose best candidates are best (Theorem 3.1 of the paper), by induction over
//! the inserts: a candidate of a held bucket replaces the one held if it's better. A bucket that
//! isn't held is new if the reservoir isn't full. If it's full, everything the bucket had so far is
//! below the worst candidate held, so its new candidate belongs in exactly if it beats that one,
//! which is the eviction rule. Hence the same inserts in any order, from any number of threads,
//! give the same reservoirs. A candidate's hash is a function of the pair, so a repeated
//! candidate lands in its own bucket again, which keeps its best score.
//!
//! # Layout
//!
//! Every point has `capacity` slots of 10 bytes, a 2-byte hash and an 8-byte [`Key`] with the
//! 4-byte id and the f32 score, plus 12 bytes for its lock and its number of used slots: 652 bytes
//! per point at a capacity of 64. The paper packs a slot into 8 bytes with a bf16 score, but bf16
//! keeps only 8 significant bits. Candidates within 0.4% of each other would tie and go to the
//! lower id instead of the closer one, and the final prune would compare rounded scores with exact
//! ones.
//!
//! The slots of a point aren't sorted: a bucket is found by scanning its hashes, 128 bytes at a
//! capacity of 64. The paper sorts them by hash for a binary search, which shifts slots for every
//! new bucket. A full reservoir remembers its worst slot, so a candidate that doesn't make it in
//! costs one comparison. The keys are only scanned again after the worst slot changed.
//!
//! # Concurrency
//!
//! Every point has its own lock, so threads only wait for each other when they insert into the
//! same point at the same time. The slots are atomics only so that one flat allocation can be
//! written through a shared reference without `unsafe`: they're read and written with relaxed
//! ordering while holding the point's lock, which orders all accesses. Relaxed loads and stores
//! are plain moves on x86_64 and aarch64.

use std::ops::Range;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};

use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};
use parking_lot::Mutex;
use rayon::prelude::*;

/// Candidate reservoirs of the points of one level, written by all threads of the build.
pub(super) struct Reservoirs {
    /// Slots per point.
    capacity: usize,
    /// Position of every point among the points, which indexes `heads` and the slots.
    positions: Positions,
    /// Lock and used slots of every point.
    heads: Box<[Mutex<Head>]>,
    /// Bucket of every slot, `capacity` consecutive slots per point.
    hashes: Box<[AtomicU16]>,
    /// [`Key`] of every slot, laid out like `hashes`.
    keys: Box<[AtomicU64]>,
}

impl Reservoirs {
    /// Reservoirs for `points` (sorted, distinct: the points of one level), `capacity` each.
    ///
    /// Allocates and initializes all slots up front, in parallel on the current rayon pool.
    pub fn new(points: &[PointOffsetType], capacity: usize) -> Self {
        debug_assert!(
            points.is_sorted_by(|a, b| a < b),
            "points must be sorted and distinct",
        );
        // `Head` counts slots in u32, more than any reservoir needs
        let capacity = capacity.min(u32::MAX as usize);
        let slots = points.len() * capacity;
        Self {
            capacity,
            positions: Positions::new(points),
            heads: (0..points.len())
                .into_par_iter()
                .map(|_| Mutex::default())
                .collect(),
            hashes: (0..slots)
                .into_par_iter()
                .map(|_| AtomicU16::new(0))
                .collect(),
            keys: (0..slots)
                .into_par_iter()
                .map(|_| AtomicU64::new(0))
                .collect(),
        }
    }

    /// Offers `candidate` with `score` and its bucket `hash` to the reservoir of `point`.
    /// Thread-safe; called concurrently.
    ///
    /// `hash` must be a function of `point` and `candidate`, as the residual hash is. Debug builds
    /// check that a candidate doesn't come with two hashes. Ignored are:
    /// - a point as its own candidate,
    /// - NaN scores, which have no place in the order of candidates,
    /// - a `point` without a reservoir, which fails a debug assertion.
    ///
    /// `candidate` must be one of the points too, which only debug builds check.
    pub fn insert(
        &self,
        point: PointOffsetType,
        candidate: PointOffsetType,
        hash: u16,
        score: ScoreType,
    ) {
        if candidate == point || score.is_nan() {
            return;
        }
        let Some(position) = self.position(point) else {
            return;
        };
        debug_assert!(
            self.positions.get(candidate).is_some(),
            "candidate {candidate} is not a point of this level",
        );
        if self.capacity == 0 {
            return;
        }

        let slots = self.slots(position);
        let mut head = self.heads[position].lock();
        Reservoir {
            head: &mut head,
            hashes: &self.hashes[slots.clone()],
            keys: &self.keys[slots],
        }
        .insert(hash, Key::new(candidate, score));
    }

    /// The candidates of `point`, best first (ties to the lower id). Callable concurrently for
    /// different points, for a parallel final prune.
    ///
    /// Empty for a point without a reservoir, which fails a debug assertion.
    pub fn candidates(&self, point: PointOffsetType) -> Vec<ScoredPointOffset> {
        let Some(position) = self.position(point) else {
            return Vec::new();
        };
        let mut keys: Vec<_> = {
            let head = self.heads[position].lock();
            self.keys[self.slots(position)][..head.len as usize]
                .iter()
                .map(|key| Key(key.load(Ordering::Relaxed)))
                .collect()
        };
        keys.sort_unstable_by(|a, b| b.cmp(a));
        keys.into_iter().map(Key::scored).collect()
    }

    /// Position of `point` among the points, `None` if it has no reservoir.
    fn position(&self, point: PointOffsetType) -> Option<usize> {
        let position = self.positions.get(point);
        debug_assert!(position.is_some(), "point {point} has no reservoir");
        position
    }

    /// Slots of the point at `position`.
    fn slots(&self, position: usize) -> Range<usize> {
        position * self.capacity..(position + 1) * self.capacity
    }
}

/// The part of a reservoir its lock protects: which of the point's slots are used.
#[derive(Default)]
struct Head {
    /// Number of used slots, the first ones of the point.
    len: u32,
    /// The slot with the worst candidate, only kept while all slots are used.
    worst: u32,
}

/// The reservoir of one point, borrowed while holding its lock.
struct Reservoir<'a> {
    head: &'a mut Head,
    hashes: &'a [AtomicU16],
    keys: &'a [AtomicU64],
}

impl Reservoir<'_> {
    /// Applies the bucket rule, then the eviction rule.
    fn insert(&mut self, hash: u16, key: Key) {
        let len = self.head.len as usize;
        let full = len == self.keys.len();
        debug_assert!(
            (0..len).all(|slot| {
                self.key(slot).candidate() != key.candidate() || self.hash(slot) == hash
            }),
            "candidate {} comes with two hashes",
            key.candidate(),
        );

        let bucket = self.hashes[..len]
            .iter()
            .position(|slot_hash| slot_hash.load(Ordering::Relaxed) == hash);
        if let Some(slot) = bucket {
            // The bucket keeps its better candidate
            if key > self.key(slot) {
                self.keys[slot].store(key.0, Ordering::Relaxed);
                if full && slot == self.head.worst as usize {
                    self.update_worst();
                }
            }
        } else if !full {
            self.set(len, hash, key);
            self.head.len += 1;
            if len + 1 == self.keys.len() {
                self.update_worst();
            }
        } else {
            // A new bucket replaces the worst one, if its candidate is better
            let worst = self.head.worst as usize;
            if key > self.key(worst) {
                self.set(worst, hash, key);
                self.update_worst();
            }
        }
    }

    fn hash(&self, slot: usize) -> u16 {
        self.hashes[slot].load(Ordering::Relaxed)
    }

    fn key(&self, slot: usize) -> Key {
        Key(self.keys[slot].load(Ordering::Relaxed))
    }

    fn set(&self, slot: usize, hash: u16, key: Key) {
        self.hashes[slot].store(hash, Ordering::Relaxed);
        self.keys[slot].store(key.0, Ordering::Relaxed);
    }

    /// Finds the worst slot of the full reservoir.
    fn update_worst(&mut self) {
        // Slots hold distinct keys, since a candidate has only one bucket
        let worst = (0..self.keys.len())
            .min_by_key(|&slot| self.key(slot))
            .unwrap_or_default();
        self.head.worst = worst as u32;
    }
}

/// Sign bit of an f32.
const SIGN: u32 = 1 << 31;

/// A candidate with its score, packed so that a larger key is a better candidate: a higher score,
/// then a lower id.
///
/// The upper half is the score, mapped to an unsigned integer of the same order. The lower half is
/// the inverted id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Key(u64);

impl Key {
    /// `score` must not be NaN. Both zeros give the same key, the one of `0.0`.
    fn new(candidate: PointOffsetType, score: ScoreType) -> Self {
        debug_assert!(!score.is_nan(), "NaN has no key");
        // `-0.0 == 0.0`, so both are one score
        let bits = if score == 0.0 { 0 } else { score.to_bits() };
        // Positive floats order like their bits and negative ones in reverse, so mark the positive
        // ones with the sign bit and invert the negative ones
        let ordered = if bits & SIGN == 0 { bits | SIGN } else { !bits };
        Self((u64::from(ordered) << 32) | u64::from(!candidate))
    }

    fn candidate(self) -> PointOffsetType {
        // Truncates to the lower half
        !(self.0 as PointOffsetType)
    }

    fn score(self) -> ScoreType {
        let ordered = (self.0 >> 32) as u32;
        let bits = if ordered & SIGN == 0 {
            !ordered
        } else {
            ordered & !SIGN
        };
        ScoreType::from_bits(bits)
    }

    fn scored(self) -> ScoredPointOffset {
        ScoredPointOffset {
            idx: self.candidate(),
            score: self.score(),
        }
    }
}

/// Positions of the points of one level among them, by id in constant time.
///
/// A bitmap of the points with a running count every 64 ids: 16 bytes per 64 ids up to the largest
/// point, a quarter byte per id: 250 KB for a million ids, which mostly stays in cache. A binary
/// search over the points would cost several cache misses per insert, and a table of positions by
/// id 4 bytes per id.
struct Positions {
    blocks: Vec<PositionBlock>,
}

/// 64 consecutive ids.
#[derive(Clone, Copy, Default)]
struct PositionBlock {
    /// Bit `i` is set if the block's `i`-th id is a point.
    points: u64,
    /// Number of points with a lower id than the block's first.
    before: u32,
}

impl Positions {
    /// `points` must be distinct.
    fn new(points: &[PointOffsetType]) -> Self {
        let len = points.iter().max().map_or(0, |&max| max as usize / 64 + 1);
        let mut blocks = vec![PositionBlock::default(); len];
        for &point in points {
            blocks[point as usize / 64].points |= 1 << (point % 64);
        }
        let mut before = 0;
        for block in &mut blocks {
            // Below the number of points, which are distinct u32
            block.before = before as u32;
            before += block.points.count_ones() as usize;
        }
        Self { blocks }
    }

    /// Position of `point` among the points in the order of ids, `None` if it isn't one.
    fn get(&self, point: PointOffsetType) -> Option<usize> {
        let block = self.blocks.get(point as usize / 64)?;
        let bit = 1 << (point % 64);
        (block.points & bit != 0)
            .then(|| block.before as usize + (block.points & (bit - 1)).count_ones() as usize)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering as CmpOrdering;
    use std::collections::{BTreeSet, HashMap};

    use rand::rngs::StdRng;
    use rand::seq::SliceRandom as _;
    use rand::{RngExt as _, SeedableRng as _};

    use super::*;

    /// One call of [`Reservoirs::insert`].
    #[derive(Clone, Copy, Debug)]
    struct Insert {
        point: PointOffsetType,
        candidate: PointOffsetType,
        hash: u16,
        score: ScoreType,
    }

    fn scored(idx: PointOffsetType, score: ScoreType) -> ScoredPointOffset {
        ScoredPointOffset { idx, score }
    }

    fn insert_all(reservoirs: &Reservoirs, inserts: &[Insert]) {
        for &Insert {
            point,
            candidate,
            hash,
            score,
        } in inserts
        {
            reservoirs.insert(point, candidate, hash, score);
        }
    }

    fn all_candidates(
        reservoirs: &Reservoirs,
        points: &[PointOffsetType],
    ) -> Vec<Vec<ScoredPointOffset>> {
        points
            .iter()
            .map(|&point| reservoirs.candidates(point))
            .collect()
    }

    /// Order of candidates, the better one greater: a higher score, then a lower id.
    fn cmp_candidates(a: &ScoredPointOffset, b: &ScoredPointOffset) -> CmpOrdering {
        a.score
            .partial_cmp(&b.score)
            .expect("no NaN in reservoirs")
            .then(b.idx.cmp(&a.idx))
    }

    /// The reservoirs of `points` after `inserts` by their definition, independent of any order:
    /// the best candidate of every bucket, then the best `capacity` of those, best first.
    fn expected(
        inserts: &[Insert],
        points: &[PointOffsetType],
        capacity: usize,
    ) -> Vec<Vec<ScoredPointOffset>> {
        let mut buckets: HashMap<(PointOffsetType, u16), ScoredPointOffset> = HashMap::new();
        for insert in inserts {
            if insert.candidate == insert.point || insert.score.is_nan() {
                continue;
            }
            let new = scored(insert.candidate, insert.score);
            buckets
                .entry((insert.point, insert.hash))
                .and_modify(|best| {
                    if cmp_candidates(&new, best).is_gt() {
                        *best = new;
                    }
                })
                .or_insert(new);
        }

        let mut reservoirs: HashMap<PointOffsetType, Vec<ScoredPointOffset>> = HashMap::new();
        for ((point, _), best) in buckets {
            reservoirs.entry(point).or_default().push(best);
        }
        points
            .iter()
            .map(|point| {
                let mut best = reservoirs.remove(point).unwrap_or_default();
                best.sort_by(|a, b| cmp_candidates(b, a));
                best.truncate(capacity);
                best
            })
            .collect()
    }

    /// A hash into `buckets` buckets that is a function of the pair, as the residual hash is.
    fn pair_hash(point: PointOffsetType, candidate: PointOffsetType, buckets: u16) -> u16 {
        let pair = (u64::from(point) << 32) | u64::from(candidate);
        (pair.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 48) as u16 % buckets
    }

    /// A few distinct values, for many ties, and the special ones.
    fn random_score(rng: &mut StdRng) -> ScoreType {
        match rng.random_range(0..20) {
            0 => ScoreType::NAN,
            1 => -0.0,
            2 => 0.0,
            3 => ScoreType::INFINITY,
            4 => ScoreType::NEG_INFINITY,
            _ => rng.random_range(-4..4) as ScoreType / 2.0,
        }
    }

    /// `count` inserts among `points` with many collisions: few buckets and distinct scores,
    /// repeated inserts, repeated pairs with another score, and self candidates.
    fn random_inserts(
        rng: &mut StdRng,
        points: &[PointOffsetType],
        count: usize,
        buckets: u16,
    ) -> Vec<Insert> {
        let mut inserts: Vec<Insert> = Vec::with_capacity(count);
        while inserts.len() < count {
            let insert = if !inserts.is_empty() && rng.random_bool(0.2) {
                let mut repeated = inserts[rng.random_range(0..inserts.len())];
                if rng.random_bool(0.5) {
                    repeated.score = random_score(rng);
                }
                repeated
            } else {
                let point = points[rng.random_range(0..points.len())];
                let candidate = points[rng.random_range(0..points.len())];
                Insert {
                    point,
                    candidate,
                    hash: pair_hash(point, candidate, buckets),
                    score: random_score(rng),
                }
            };
            inserts.push(insert);
        }
        inserts
    }

    /// `len` distinct random ids below `ids`, sorted.
    fn random_points(rng: &mut StdRng, len: usize, ids: PointOffsetType) -> Vec<PointOffsetType> {
        let mut points = BTreeSet::new();
        while points.len() < len {
            points.insert(rng.random_range(0..ids));
        }
        points.into_iter().collect()
    }

    #[test]
    fn keys_order_by_score_then_lower_id() {
        let scores = [
            ScoreType::NEG_INFINITY,
            ScoreType::MIN,
            -1.0,
            -ScoreType::MIN_POSITIVE,
            -ScoreType::from_bits(1),
            0.0,
            ScoreType::from_bits(1),
            ScoreType::MIN_POSITIVE,
            0.5,
            1.0,
            ScoreType::MAX,
            ScoreType::INFINITY,
        ];
        let ids = [0, 1, 7, 1 << 31, PointOffsetType::MAX];
        let candidates: Vec<_> = scores
            .iter()
            .flat_map(|&score| ids.iter().map(move |&idx| scored(idx, score)))
            .collect();

        for a in &candidates {
            let key = Key::new(a.idx, a.score);
            assert_eq!(key.candidate(), a.idx);
            assert_eq!(key.score().to_bits(), a.score.to_bits());
            for b in &candidates {
                assert_eq!(
                    key.cmp(&Key::new(b.idx, b.score)),
                    cmp_candidates(a, b),
                    "{a:?} against {b:?}",
                );
            }
        }

        // Both zeros are one score, and come back as 0.0
        assert_eq!(Key::new(3, -0.0), Key::new(3, 0.0));
        assert_eq!(Key::new(3, -0.0).score().to_bits(), 0.0f32.to_bits());
    }

    #[test]
    fn bucket_keeps_its_best_candidate() {
        let reservoirs = Reservoirs::new(&[0, 1, 2, 3, 4], 4);
        reservoirs.insert(0, 2, 7, 1.0);
        reservoirs.insert(0, 3, 7, 2.0); // Better, replaces
        reservoirs.insert(0, 4, 7, 1.5); // Worse, ignored
        assert_eq!(reservoirs.candidates(0), [scored(3, 2.0)]);

        reservoirs.insert(0, 1, 7, 2.0); // Same score, lower id: replaces
        reservoirs.insert(0, 4, 7, 2.0); // Same score, higher id: ignored
        assert_eq!(reservoirs.candidates(0), [scored(1, 2.0)]);

        // Other buckets and points are separate
        reservoirs.insert(0, 4, 8, 0.5);
        reservoirs.insert(1, 0, 7, 3.0);
        assert_eq!(reservoirs.candidates(0), [scored(1, 2.0), scored(4, 0.5)]);
        assert_eq!(reservoirs.candidates(1), [scored(0, 3.0)]);
        assert!(reservoirs.candidates(2).is_empty());
    }

    #[test]
    fn full_reservoir_replaces_its_worst_candidate() {
        let points: Vec<_> = (0..10).collect();
        let reservoirs = Reservoirs::new(&points, 3);
        // The worst one in the middle slot
        reservoirs.insert(0, 2, 2, 2.0);
        reservoirs.insert(0, 1, 1, 1.0);
        reservoirs.insert(0, 3, 3, 3.0);
        reservoirs.insert(0, 4, 4, 1.5); // Beats the worst, 1.0
        let after_eviction = [scored(3, 3.0), scored(2, 2.0), scored(4, 1.5)];
        assert_eq!(reservoirs.candidates(0), after_eviction);

        reservoirs.insert(0, 5, 5, 1.2); // Below the worst, 1.5
        reservoirs.insert(0, 6, 6, 1.5); // Ties the worst with a higher id
        assert_eq!(reservoirs.candidates(0), after_eviction);

        // Improving the worst slot's bucket makes another one the worst
        reservoirs.insert(0, 7, 4, 2.5);
        assert_eq!(
            reservoirs.candidates(0),
            [scored(3, 3.0), scored(7, 2.5), scored(2, 2.0)],
        );
        reservoirs.insert(0, 8, 8, 2.2); // Beats the new worst, 2.0
        assert_eq!(
            reservoirs.candidates(0),
            [scored(3, 3.0), scored(7, 2.5), scored(8, 2.2)],
        );

        reservoirs.insert(0, 9, 9, 2.2); // Ties the worst with a higher id
        reservoirs.insert(0, 1, 1, 2.2); // Ties the worst with a lower id
        assert_eq!(
            reservoirs.candidates(0),
            [scored(3, 3.0), scored(7, 2.5), scored(1, 2.2)],
        );
    }

    #[test]
    fn evicted_bucket_has_to_beat_the_worst_candidate() {
        let points: Vec<_> = (0..6).collect();
        let reservoirs = Reservoirs::new(&points, 2);
        reservoirs.insert(0, 1, 1, 5.0);
        reservoirs.insert(0, 2, 2, 6.0);
        reservoirs.insert(0, 3, 3, 7.0); // Evicts bucket 1
        reservoirs.insert(0, 4, 1, 4.0); // Below the evicted one
        reservoirs.insert(0, 5, 1, 5.5); // Above the evicted one, below the worst held
        assert_eq!(reservoirs.candidates(0), [scored(3, 7.0), scored(2, 6.0)]);

        reservoirs.insert(0, 4, 1, 6.5); // Beats the worst held
        assert_eq!(reservoirs.candidates(0), [scored(3, 7.0), scored(4, 6.5)]);
    }

    #[test]
    fn capacity_zero_keeps_nothing() {
        let points = [0, 1, 2];
        let reservoirs = Reservoirs::new(&points, 0);
        reservoirs.insert(0, 1, 0, 1.0);
        reservoirs.insert(1, 2, 0, 1.0);
        reservoirs.insert(2, 0, 5, 1.0);
        assert!(
            all_candidates(&reservoirs, &points)
                .iter()
                .all(Vec::is_empty)
        );
    }

    #[test]
    fn capacity_one_keeps_the_best_candidate() {
        let points: Vec<_> = (0..10).collect();
        let reservoirs = Reservoirs::new(&points, 1);
        reservoirs.insert(0, 5, 1, 1.0);
        reservoirs.insert(0, 6, 2, 3.0);
        reservoirs.insert(0, 7, 2, 2.0);
        reservoirs.insert(0, 8, 3, 3.0);
        reservoirs.insert(0, 4, 4, 3.0);
        reservoirs.insert(0, 9, 5, -1.0);
        assert_eq!(reservoirs.candidates(0), [scored(4, 3.0)]);

        let mut rng = StdRng::seed_from_u64(1);
        let inserts = random_inserts(&mut rng, &points, 500, 7);
        let reservoirs = Reservoirs::new(&points, 1);
        insert_all(&reservoirs, &inserts);
        for (point, candidates) in all_candidates(&reservoirs, &points).iter().enumerate() {
            let best = inserts
                .iter()
                .filter(|i| i.point == point as PointOffsetType)
                .filter(|i| i.candidate != i.point && !i.score.is_nan())
                .map(|i| scored(i.candidate, i.score))
                .max_by(cmp_candidates);
            assert_eq!(candidates.first(), best.as_ref());
        }
    }

    #[test]
    fn repeated_candidate_takes_one_slot() {
        let reservoirs = Reservoirs::new(&[0, 1, 2, 3], 3);
        for _ in 0..10 {
            reservoirs.insert(0, 1, 4, 1.0);
        }
        assert_eq!(reservoirs.candidates(0), [scored(1, 1.0)]);

        // With other scores, its best one stays
        reservoirs.insert(0, 1, 4, 3.0);
        reservoirs.insert(0, 1, 4, 2.0);
        assert_eq!(reservoirs.candidates(0), [scored(1, 3.0)]);

        // Repeats don't take the slots of other candidates
        reservoirs.insert(0, 2, 5, 0.5);
        reservoirs.insert(0, 3, 6, 0.25);
        for _ in 0..10 {
            reservoirs.insert(0, 3, 6, 0.25);
            reservoirs.insert(0, 2, 5, 0.5);
        }
        assert_eq!(
            reservoirs.candidates(0),
            [scored(1, 3.0), scored(2, 0.5), scored(3, 0.25)],
        );
    }

    #[test]
    fn self_candidates_are_ignored() {
        let reservoirs = Reservoirs::new(&[0, 1, 2], 1);
        reservoirs.insert(1, 1, 0, 100.0);
        assert!(reservoirs.candidates(1).is_empty());

        reservoirs.insert(1, 2, 3, 1.0);
        reservoirs.insert(1, 1, 0, 100.0); // Would beat the worst
        reservoirs.insert(1, 1, 3, 100.0); // Would beat the bucket's candidate
        assert_eq!(reservoirs.candidates(1), [scored(2, 1.0)]);
    }

    #[test]
    fn nan_scores_are_ignored() {
        let reservoirs = Reservoirs::new(&[0, 1, 2, 3], 2);
        reservoirs.insert(0, 1, 5, ScoreType::NAN);
        assert!(reservoirs.candidates(0).is_empty());

        reservoirs.insert(0, 1, 5, 1.0);
        reservoirs.insert(0, 1, 5, ScoreType::NAN); // Doesn't replace in its bucket
        reservoirs.insert(0, 2, 6, -ScoreType::NAN);
        assert_eq!(reservoirs.candidates(0), [scored(1, 1.0)]);

        reservoirs.insert(0, 2, 6, ScoreType::NEG_INFINITY);
        reservoirs.insert(0, 3, 7, ScoreType::NAN); // Doesn't evict
        reservoirs.insert(0, 3, 7, -ScoreType::NAN);
        assert_eq!(
            reservoirs.candidates(0),
            [scored(1, 1.0), scored(2, ScoreType::NEG_INFINITY)],
        );
    }

    #[test]
    fn zeros_of_both_signs_tie() {
        let reservoirs = Reservoirs::new(&[0, 1, 2, 3, 4], 4);
        // Both orders go to the lower id
        reservoirs.insert(0, 2, 1, -0.0);
        reservoirs.insert(0, 1, 1, 0.0);
        reservoirs.insert(3, 1, 1, 0.0);
        reservoirs.insert(3, 2, 1, -0.0);
        assert_eq!(reservoirs.candidates(0), [scored(1, 0.0)]);
        assert_eq!(reservoirs.candidates(3), [scored(1, 0.0)]);

        // Zeros sort between positive and negative scores, and come back as 0.0
        reservoirs.insert(4, 2, 2, -0.0);
        reservoirs.insert(4, 3, 3, -ScoreType::MIN_POSITIVE);
        reservoirs.insert(4, 1, 1, ScoreType::MIN_POSITIVE);
        let candidates = reservoirs.candidates(4);
        assert_eq!(
            candidates,
            [
                scored(1, ScoreType::MIN_POSITIVE),
                scored(2, 0.0),
                scored(3, -ScoreType::MIN_POSITIVE),
            ],
        );
        assert_eq!(candidates[1].score.to_bits(), 0.0f32.to_bits());
    }

    #[test]
    fn candidates_are_sorted_best_first() {
        let points: Vec<_> = (0..100).collect();
        let mut rng = StdRng::seed_from_u64(2);
        let inserts: Vec<_> = (1..100)
            .map(|candidate| Insert {
                point: 0,
                candidate,
                hash: candidate as u16,
                score: rng.random_range(0..10) as ScoreType,
            })
            .collect();
        let reservoirs = Reservoirs::new(&points, 64);
        insert_all(&reservoirs, &inserts);

        let candidates = reservoirs.candidates(0);
        assert_eq!(candidates.len(), 64);
        assert!(candidates.is_sorted_by(|a, b| cmp_candidates(a, b).is_gt()));
        assert_eq!(candidates, expected(&inserts, &[0], 64)[0]);
    }

    #[test]
    fn order_of_inserts_does_not_matter() {
        let mut rng = StdRng::seed_from_u64(42);
        for case in 0..60 {
            let len = rng.random_range(1..=6);
            let points = random_points(&mut rng, len, 200);
            let capacity = case % 7;
            let buckets = rng.random_range(1..=12);
            let mut inserts = random_inserts(&mut rng, &points, 150, buckets);
            let expected = expected(&inserts, &points, capacity);
            for _ in 0..50 {
                inserts.shuffle(&mut rng);
                let reservoirs = Reservoirs::new(&points, capacity);
                insert_all(&reservoirs, &inserts);
                assert_eq!(
                    all_candidates(&reservoirs, &points),
                    expected,
                    "case {case}: {capacity} slots, {buckets} buckets, points {points:?}",
                );
            }
        }
    }

    #[test]
    fn concurrent_inserts_match_sequential_ones() {
        const THREADS: usize = 8;
        let mut rng = StdRng::seed_from_u64(7);
        // Few points, so threads insert into the same ones at the same time, and many points
        for (len, ids, capacity, buckets, count) in
            [(4, 10, 3, 8, 20_000), (2_000, 1_000_000, 16, 4096, 100_000)]
        {
            let points = random_points(&mut rng, len, ids);
            let mut inserts = random_inserts(&mut rng, &points, count, buckets);

            let sequential = Reservoirs::new(&points, capacity);
            insert_all(&sequential, &inserts);
            let sequential = all_candidates(&sequential, &points);
            assert_eq!(sequential, expected(&inserts, &points, capacity));

            for _ in 0..3 {
                inserts.shuffle(&mut rng);
                let reservoirs = Reservoirs::new(&points, capacity);
                std::thread::scope(|scope| {
                    for thread in 0..THREADS {
                        let (reservoirs, inserts) = (&reservoirs, &inserts);
                        scope.spawn(move || {
                            let own: Vec<_> = inserts
                                .iter()
                                .skip(thread)
                                .step_by(THREADS)
                                .copied()
                                .collect();
                            insert_all(reservoirs, &own);
                        });
                    }
                });
                assert_eq!(all_candidates(&reservoirs, &points), sequential);
            }
        }
    }

    #[test]
    fn positions_of_sparse_points() {
        let mut rng = StdRng::seed_from_u64(3);
        let mut points = random_points(&mut rng, 20_000, 5_000_000);
        points.extend([0, 1, 63, 64, 127, 128, 4_999_999]);
        points.sort_unstable();
        points.dedup();

        let positions = Positions::new(&points);
        for (position, &point) in points.iter().enumerate() {
            assert_eq!(positions.get(point), Some(position));
        }
        for _ in 0..20_000 {
            let id = rng.random_range(0..5_100_000);
            assert_eq!(positions.get(id), points.binary_search(&id).ok());
        }
        assert_eq!(positions.get(5_000_000), None);
        assert_eq!(positions.get(PointOffsetType::MAX), None);

        assert_eq!(Positions::new(&[]).get(0), None);
        let single = Positions::new(&[64]);
        assert_eq!(single.get(63), None);
        assert_eq!(single.get(64), Some(0));
        assert_eq!(single.get(65), None);
    }

    #[test]
    fn sparse_points_of_a_large_id_space() {
        let mut rng = StdRng::seed_from_u64(4);
        let points = random_points(&mut rng, 3_000, 10_000_000);
        let inserts = random_inserts(&mut rng, &points, 60_000, 4096);
        for capacity in [1, 16, 64] {
            let reservoirs = Reservoirs::new(&points, capacity);
            insert_all(&reservoirs, &inserts);
            assert_eq!(
                all_candidates(&reservoirs, &points),
                expected(&inserts, &points, capacity),
            );
        }
    }

    /// The bytes per point the module documentation states.
    #[test]
    fn memory_per_point() {
        assert_eq!(size_of::<AtomicU16>() + size_of::<AtomicU64>(), 10);
        assert_eq!(size_of::<Mutex<Head>>(), 12);
        assert_eq!(size_of::<PositionBlock>(), 16);
    }

    #[test]
    fn no_points() {
        let reservoirs = Reservoirs::new(&[], 8);
        assert!(reservoirs.heads.is_empty());
        assert!(reservoirs.keys.is_empty());
    }

    #[test]
    #[cfg_attr(debug_assertions, should_panic(expected = "point 3 has no reservoir"))]
    fn inserts_for_other_points_are_ignored() {
        let reservoirs = Reservoirs::new(&[2, 5], 4);
        reservoirs.insert(3, 5, 0, 1.0);
        reservoirs.insert(7, 2, 0, 1.0);
        assert!(reservoirs.candidates(2).is_empty());
        assert!(reservoirs.candidates(5).is_empty());
    }

    #[test]
    #[cfg_attr(debug_assertions, should_panic(expected = "point 1 has no reservoir"))]
    fn other_points_have_no_candidates() {
        let reservoirs = Reservoirs::new(&[0, 2], 4);
        reservoirs.insert(0, 2, 0, 1.0);
        assert!(reservoirs.candidates(1).is_empty());
    }

    #[test]
    #[cfg_attr(
        debug_assertions,
        should_panic(expected = "candidate 9 is not a point of this level")
    )]
    fn candidates_of_other_levels_are_checked_in_debug_builds() {
        let reservoirs = Reservoirs::new(&[0, 2], 4);
        reservoirs.insert(0, 9, 0, 1.0);
        assert_eq!(reservoirs.candidates(0), [scored(9, 1.0)]);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "candidate 1 comes with two hashes")]
    fn candidate_with_two_hashes_fails_in_debug_builds() {
        let reservoirs = Reservoirs::new(&[0, 1], 4);
        reservoirs.insert(0, 1, 3, 1.0);
        reservoirs.insert(0, 1, 4, 2.0);
    }
}
