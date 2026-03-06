use std::cmp::{max, min};
use std::hash::{Hash, Hasher};

use common::types::{PointOffsetType, ScoreType};
use seahash::SeaHasher;

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct PointPair {
    a: PointOffsetType,
    b: PointOffsetType,
}

impl PointPair {
    pub fn new(a: PointOffsetType, b: PointOffsetType) -> Self {
        PointPair {
            a: min(a, b),
            b: max(a, b),
        }
    }
}

#[derive(Clone, Debug)]
struct CacheObj {
    points: PointPair,
    value: ScoreType,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct DistanceCache {
    cache: Vec<Option<CacheObj>>,
    pub hits: usize,
    pub misses: usize,
}

#[allow(dead_code)]
impl DistanceCache {
    fn hasher() -> impl Hasher {
        SeaHasher::new()
    }

    pub fn new(size: usize) -> Self {
        let mut cache = Vec::with_capacity(size);
        cache.resize(size, None);
        DistanceCache {
            cache,
            hits: 0,
            misses: 0,
        }
    }

    pub fn get(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> Option<ScoreType> {
        let points = PointPair::new(point_a, point_b);
        let mut s = DistanceCache::hasher();
        points.hash(&mut s);
        let idx = s.finish() as usize % self.cache.len();

        self.cache[idx].as_ref().and_then(|x| {
            if x.points == points {
                Some(x.value)
            } else {
                None
            }
        })
    }

    pub fn put(&mut self, point_a: PointOffsetType, point_b: PointOffsetType, value: ScoreType) {
        let points = PointPair::new(point_a, point_b);
        let mut s = DistanceCache::hasher();
        points.hash(&mut s);
        let idx = s.finish() as usize % self.cache.len();
        self.cache[idx] = Some(CacheObj { points, value });
    }
}

impl Default for DistanceCache {
    fn default() -> Self {
        DistanceCache::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache() {
        let mut cache = DistanceCache::new(1000);

        cache.put(100, 10, 0.8);
        cache.put(10, 101, 0.7);
        cache.put(10, 110, 0.1);

        assert_eq!(cache.get(12, 99), None);
        assert_eq!(cache.get(10, 100), Some(0.8));
        assert_eq!(cache.get(10, 101), Some(0.7));
    }

    #[test]
    fn test_collision() {
        let mut cache = DistanceCache::new(1);

        cache.put(1, 2, 0.8);
        cache.put(3, 4, 0.7);

        assert_eq!(cache.get(1, 2), None);
        assert_eq!(cache.get(2, 1), None);
        assert_eq!(cache.get(4, 3), Some(0.7));
    }
}
