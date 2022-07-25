use std::cmp::{max, min, Ordering};
use std::collections::BTreeMap;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::ops::Bound;

use itertools::Itertools;

const MIN_BUCKET_SIZE: usize = 10;

#[derive(Debug, Clone)]
struct Counts {
    pub left: usize,
    pub right: usize,
}

#[derive(PartialEq, PartialOrd, Debug, Clone)]
pub struct Point {
    pub val: f64,
    pub idx: usize,
}

impl Eq for Point {}

#[allow(clippy::derive_ord_xor_partial_ord)]
impl Ord for Point {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.val, self.idx)
            .partial_cmp(&(other.val, other.idx))
            .unwrap()
    }
}

#[derive(Debug)]
pub struct Histogram {
    max_bucket_size: usize,
    precision: f64,
    total_count: usize,
    borders: BTreeMap<Point, Counts>,
}

impl Histogram {
    pub fn new(max_bucket_size: usize, precision: f64) -> Self {
        assert!(precision < 1.0);
        assert!(precision > 0.0);
        Self {
            max_bucket_size,
            precision,
            total_count: 0,
            borders: BTreeMap::default(),
        }
    }

    #[allow(dead_code)]
    fn validate(&self) -> Result<(), String> {
        // Iterate over chunks of borders
        for (left, right) in self.borders.values().tuple_windows() {
            assert_eq!(left.right, right.left);
        }
        Ok(())
    }

    fn current_bucket_size(&self) -> usize {
        let bucket_size = (self.total_count as f64 * self.precision) as usize;
        min(max(MIN_BUCKET_SIZE, bucket_size), self.max_bucket_size)
    }

    /// Infers boundaries for bucket of given size and staring point.
    /// Returns `to` range of values starting provided `from`value which is expected to contain
    /// `range_size` values
    ///
    /// Returns None if there are no points stored
    pub fn get_range_by_size(&self, from: Bound<f64>, range_size: usize) -> Bound<f64> {
        // bound_map is unstable, but can be used here
        // let from_ = from.map(|val| Point { val, idx: usize::MIN });

        let from_ = match from {
            Included(val) => Included(Point {
                val,
                idx: usize::MIN,
            }),
            Excluded(val) => Excluded(Point {
                val,
                idx: usize::MAX,
            }),
            Unbounded => Unbounded,
        };

        let mut reached_count = 0;
        for (border, counts) in self.borders.range((from_, Unbounded)) {
            if reached_count + counts.left > range_size {
                // required size reached
                return Included(border.val);
            } else {
                // Size not yet reached
                reached_count += counts.left;
            }
        }

        Unbounded
    }

    pub fn estimate(&self, from: Bound<f64>, to: Bound<f64>) -> (usize, usize, usize) {
        let from_ = match &from {
            Included(val) => Included(Point {
                val: *val,
                idx: usize::MIN,
            }),
            Excluded(val) => Excluded(Point {
                val: *val,
                idx: usize::MAX,
            }),
            Unbounded => Unbounded,
        };

        let to_ = match &to {
            Included(val) => Included(Point {
                val: *val,
                idx: usize::MAX,
            }),
            Excluded(val) => Excluded(Point {
                val: *val,
                idx: usize::MIN,
            }),
            Unbounded => Unbounded,
        };

        // Value for range fraction estimation
        let from_val = match from {
            Included(val) => val,
            Excluded(val) => val,
            Unbounded => f64::MIN,
        };

        let to_val = match to {
            Included(val) => val,
            Excluded(val) => val,
            Unbounded => f64::MAX,
        };

        if from_val > to_val {
            return (0, 0, 0);
        }

        let left_border = {
            if matches!(from_, Unbounded) {
                None
            } else {
                self.borders.range((Unbounded, from_.clone())).next_back()
            }
        };

        let right_border = {
            if matches!(to_, Unbounded) {
                None
            } else {
                self.borders.range((to_.clone(), Unbounded)).next()
            }
        };

        let estimation = left_border
            .into_iter()
            .chain(self.borders.range((from_, to_)))
            .chain(right_border.into_iter())
            .tuple_windows()
            .map(
                |((a, a_count), (b, _b_count)): ((&Point, &Counts), (&Point, &Counts))| {
                    let val_range = b.val - a.val;

                    if val_range == 0. {
                        // Zero-length range is always covered
                        let estimates = a_count.right + 1;
                        return (estimates, estimates, estimates);
                    }

                    if a_count.right == 0 {
                        // Range covers most-right border
                        return (1, 1, 1);
                    }

                    let cover_range = to_val.min(b.val) - from_val.max(a.val);

                    let covered_frac = cover_range / val_range;
                    let estimate = (a_count.right as f64 * covered_frac).round() as usize + 1;

                    let min_estimate = if cover_range == val_range {
                        a_count.right + 1
                    } else {
                        0
                    };
                    let max_estimate = a_count.right + 1;
                    (min_estimate, estimate, max_estimate)
                },
            )
            .reduce(|a, b| (a.0 + b.0, a.1 + b.1, a.2 + b.2))
            .unwrap_or((0, 0, 0));

        estimation
    }

    pub fn remove<F, G>(&mut self, val: &Point, left_neighbour: F, right_neighbour: G)
    where
        F: Fn(&Point) -> Option<Point>,
        G: Fn(&Point) -> Option<Point>,
    {
        let (mut close_neighbors, (mut far_left_neighbor, mut far_right_neighbor)) = {
            let mut left_iterator = self
                .borders
                .range((Unbounded, Included(val.clone())))
                .map(|(k, v)| (k.clone(), v.clone()));
            let mut right_iterator = self
                .borders
                .range((Excluded(val.clone()), Unbounded))
                .map(|(k, v)| (k.clone(), v.clone()));
            (
                (left_iterator.next_back(), right_iterator.next()),
                (left_iterator.next_back(), right_iterator.next()),
            )
        };

        let (to_remove, to_create, removed) = match &mut close_neighbors {
            (None, None) => (None, None, false), // histogram is empty
            (Some((left_border, ref mut left_border_count)), None) => {
                if left_border == val {
                    // ....|
                    // ...|
                    if left_border_count.left == 0 {
                        // ...||
                        // ...|
                        (Some(left_border.clone()), None, true)
                    } else {
                        // ...|..|
                        // ...|.|
                        match &mut far_left_neighbor {
                            Some((_fln, ref mut fln_count)) => fln_count.right -= 1,
                            None => {}
                        }
                        let (new_border, new_border_count) = (
                            left_neighbour(left_border).unwrap(),
                            Counts {
                                left: left_border_count.left - 1,
                                right: 0,
                            },
                        );
                        (
                            Some(left_border.clone()),
                            Some((new_border, new_border_count)),
                            true,
                        )
                    }
                } else {
                    (None, None, false)
                }
            }
            (None, Some((right_border, ref mut right_border_count))) => {
                if right_border == val {
                    // |...
                    //  |..
                    if right_border_count.right == 0 {
                        // ||...
                        //  |...
                        (Some(right_border.clone()), None, true)
                    } else {
                        // |..|...
                        //  |.|...
                        match &mut far_right_neighbor {
                            Some((_frn, ref mut frn_count)) => frn_count.left -= 1,
                            None => {}
                        }
                        let (new_border, new_border_count) = (
                            right_neighbour(right_border).unwrap(),
                            Counts {
                                left: 0,
                                right: right_border_count.right - 1,
                            },
                        );
                        (
                            Some(right_border.clone()),
                            Some((new_border, new_border_count)),
                            true,
                        )
                    }
                } else {
                    (None, None, false)
                }
            }
            (
                Some((left_border, ref mut left_border_count)),
                Some((right_border, ref mut right_border_count)),
            ) => {
                // ...|...x.|...
                if left_border == val {
                    // ...|....|...
                    // ... |...|...
                    if left_border_count.right == 0 {
                        // ...||...
                        // ... |...
                        right_border_count.left = left_border_count.left;
                        (Some(left_border.clone()), None, true)
                    } else if right_border_count.left + left_border_count.left
                        <= self.current_bucket_size()
                        && far_left_neighbor.is_some()
                    {
                        // ...|.l..r...
                        // ...|. ..r...
                        match &mut far_left_neighbor {
                            Some((_fln, ref mut fln_count)) => {
                                fln_count.right += right_border_count.left;
                                right_border_count.left = fln_count.right;
                            }
                            None => {}
                        }
                        (Some(left_border.clone()), None, true)
                    } else {
                        // ...|..|...
                        // ... |.|...
                        right_border_count.left -= 1;
                        let (new_border, new_border_count) = (
                            right_neighbour(left_border).unwrap(),
                            Counts {
                                left: left_border_count.left,
                                right: left_border_count.right - 1,
                            },
                        );
                        (
                            Some(left_border.clone()),
                            Some((new_border, new_border_count)),
                            true,
                        )
                    }
                } else if right_border == val {
                    // ...|....|...
                    // ...|...| ...
                    if right_border_count.left == 0 {
                        // ...||...
                        // ...| ...
                        left_border_count.right = right_border_count.left;
                        (Some(right_border.clone()), None, true)
                    } else if left_border_count.right + right_border_count.right
                        <= self.current_bucket_size()
                        && far_right_neighbor.is_some()
                    {
                        // ...l..r.|...
                        // ...l.. .|...
                        match &mut far_right_neighbor {
                            Some((_frn, ref mut frn_count)) => {
                                frn_count.left += left_border_count.right;
                                left_border_count.right = frn_count.left;
                            }
                            None => {}
                        }
                        (Some(right_border.clone()), None, true)
                    } else {
                        // ...|..|...
                        // ...|.| ...
                        left_border_count.right -= 1;
                        let (new_border, new_border_count) = (
                            left_neighbour(right_border).unwrap(),
                            Counts {
                                left: right_border_count.right,
                                right: right_border_count.left - 1,
                            },
                        );
                        (
                            Some(right_border.clone()),
                            Some((new_border, new_border_count)),
                            true,
                        )
                    }
                } else if right_border_count.left == 0 {
                    // ...||...
                    // ...||...
                    (None, None, false)
                } else {
                    // ...|...|...
                    // ...|. .|...
                    right_border_count.left -= 1;
                    left_border_count.right -= 1;
                    (None, None, true)
                }
            }
        };

        if removed {
            self.total_count -= 1;
        }

        let (left_border_opt, right_border_opt) = close_neighbors;

        if let Some((k, v)) = left_border_opt {
            self.borders.insert(k, v);
        }

        if let Some((k, v)) = right_border_opt {
            self.borders.insert(k, v);
        }

        if let Some((k, v)) = far_left_neighbor {
            self.borders.insert(k, v);
        }

        if let Some((k, v)) = far_right_neighbor {
            self.borders.insert(k, v);
        }

        if let Some(remove_border) = to_remove {
            self.borders.remove(&remove_border);
        }

        if let Some((new_border, new_border_count)) = to_create {
            self.borders.insert(new_border, new_border_count);
        }
    }

    /// Warn: `val` should be unique
    pub fn insert<F, G>(&mut self, val: Point, left_neighbour: F, right_neighbour: G)
    where
        F: Fn(&Point) -> Option<Point>,
        G: Fn(&Point) -> Option<Point>,
    {
        self.total_count += 1;

        if self.borders.len() < 2 {
            self.borders.insert(val, Counts { left: 0, right: 0 });
            return;
        }

        let (mut close_neighbors, (mut far_left_neighbor, mut far_right_neighbor)) = {
            let mut left_iterator = self
                .borders
                .range((Unbounded, Included(val.clone())))
                .map(|(k, v)| (k.clone(), v.clone()));
            let mut right_iterator = self
                .borders
                .range((Excluded(val.clone()), Unbounded))
                .map(|(k, v)| (k.clone(), v.clone()));
            (
                (left_iterator.next_back(), right_iterator.next()),
                (left_iterator.next_back(), right_iterator.next()),
            )
        };

        let (to_remove, to_create) = match &mut close_neighbors {
            (None, Some((right_border, right_border_count))) => {
                // x|.....|...
                let new_count = right_border_count.right + 1;
                let (new_border, mut new_border_count) = (
                    val,
                    Counts {
                        left: 0,
                        right: new_count,
                    },
                );

                if new_count > self.current_bucket_size() {
                    // Too many values, can't move the border
                    // x|.....|...
                    // ||.....|...
                    new_border_count.right = 0;
                    (None, Some((new_border, new_border_count)))
                } else {
                    // x|.....|...
                    // |......|...
                    match &mut far_right_neighbor {
                        Some((_frn, frn_count)) => {
                            frn_count.left = new_count;
                        }
                        None => {}
                    }
                    (
                        Some(right_border.clone()),
                        Some((new_border, new_border_count)),
                    )
                }
            }
            (Some((left_border, left_border_count)), None) => {
                // ...|.....|x
                let new_count = left_border_count.left + 1;
                let (new_border, mut new_border_count) = (
                    val,
                    Counts {
                        left: new_count,
                        right: 0,
                    },
                );
                if new_count > self.current_bucket_size() {
                    // Too many values, can't move the border
                    // ...|.....|x
                    // ...|.....||
                    new_border_count.left = 0;
                    (None, Some((new_border, new_border_count)))
                } else {
                    // ...|.....|x
                    // ...|......|
                    match &mut far_left_neighbor {
                        Some((_fln, ref mut fln_count)) => fln_count.right = new_count,
                        None => {}
                    }
                    (
                        Some(left_border.clone()),
                        Some((new_border, new_border_count)),
                    )
                }
            }
            (Some((left_border, left_border_count)), Some((right_border, right_border_count))) => {
                if left_border_count.right != right_border_count.left {
                    eprintln!("error");
                }
                assert_eq!(left_border_count.right, right_border_count.left);
                let new_count = left_border_count.right + 1;

                if new_count > self.current_bucket_size() {
                    // Too many values, let's adjust
                    // Decide which border to move

                    let left_dist = (val.val - left_border.val).abs();
                    let right_dist = (val.val - right_border.val).abs();
                    if left_dist < right_dist {
                        // left border closer:
                        //  ...|..x.........|...
                        let (new_border, mut new_border_count) = (
                            right_neighbour(left_border).unwrap(),
                            Counts {
                                left: left_border_count.left + 1,
                                right: left_border_count.right,
                            },
                        );

                        if left_border_count.left < self.current_bucket_size()
                            && far_left_neighbor.is_some()
                        {
                            //we can move
                            //  ...|..x.........|...
                            //  ....|.x.........|...
                            match &mut far_left_neighbor {
                                Some((_fln, ref mut fln_count)) => {
                                    fln_count.right = new_border_count.left
                                }
                                None => {}
                            }

                            (
                                Some(left_border.clone()),
                                Some((new_border, new_border_count)),
                            )
                        } else {
                            // Can't be moved anymore, create an additional one
                            //  ...|..x.........|...
                            //  ...||.x.........|...
                            new_border_count.left = 0;
                            left_border_count.right = 0;
                            (None, Some((new_border, new_border_count)))
                        }
                    } else {
                        // right border closer
                        //  ...|........x...|...
                        let (new_border, mut new_border_count) = (
                            left_neighbour(right_border).unwrap(),
                            Counts {
                                left: right_border_count.left,
                                right: right_border_count.right + 1,
                            },
                        );

                        if right_border_count.right < self.current_bucket_size()
                            && far_right_neighbor.is_some()
                        {
                            // it's ok, we can move
                            //  1: ...|........x...|...
                            //  2: ...|........x..|....
                            match &mut far_right_neighbor {
                                Some((_frn, frn_count)) => frn_count.left = new_border_count.right,
                                None => {}
                            }
                            (
                                Some(right_border.clone()),
                                Some((new_border, new_border_count)),
                            )
                        } else {
                            // Can't be moved anymore, create a new one
                            //  1: ...|........x...|...
                            //  2: ...|........x..||...
                            new_border_count.right = 0;
                            right_border_count.left = 0;
                            (None, Some((new_border, new_border_count)))
                        }
                    }
                } else {
                    left_border_count.right = new_count;
                    right_border_count.left = new_count;
                    (None, None)
                }
            }
            (None, None) => unreachable!(),
        };

        let (left_border_opt, right_border_opt) = close_neighbors;

        if let Some((k, v)) = left_border_opt {
            self.borders.insert(k, v);
        }

        if let Some((k, v)) = right_border_opt {
            self.borders.insert(k, v);
        }

        if let Some((k, v)) = far_left_neighbor {
            self.borders.insert(k, v);
        }

        if let Some((k, v)) = far_right_neighbor {
            self.borders.insert(k, v);
        }

        if let Some(remove_border) = to_remove {
            self.borders.remove(&remove_border);
        }

        if let Some((new_border, new_border_count)) = to_create {
            self.borders.insert(new_border, new_border_count);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::BTreeSet;
    use std::io;
    use std::io::Write;

    use itertools::Itertools;
    use rand::prelude::StdRng;
    use rand::{Rng, SeedableRng};
    use rand_distr::StandardNormal;

    use super::*;

    #[allow(dead_code)]
    fn print_results(points_index: &BTreeSet<Point>, histogram: &Histogram, pnt: Option<Point>) {
        for point in points_index.iter() {
            if let Some(border_count) = histogram.borders.get(point) {
                if pnt.is_some() && pnt.as_ref().unwrap().idx == point.idx {
                    eprint!(" {}x{} ", border_count.left, border_count.right);
                } else {
                    eprint!(" {}|{} ", border_count.left, border_count.right);
                }
            } else if pnt.is_some() && pnt.as_ref().unwrap().idx == point.idx {
                eprint!("x");
            } else {
                eprint!(".");
            }
        }
        eprintln!("[{}]", histogram.total_count);
        io::stdout().flush().unwrap();
    }

    fn count_range(points_index: &BTreeSet<Point>, a: f64, b: f64) -> usize {
        points_index
            .iter()
            .filter(|x| a <= x.val && x.val <= b)
            .count()
    }

    #[test]
    fn test_build_histogram_small() {
        let max_bucket_size = 10;
        let precision = 0.01;
        let num_samples = 1000;
        let mut rnd = StdRng::seed_from_u64(42);

        // let points = (0..100000).map(|i| Point { val: rnd.gen_range(-10.0..10.0), idx: i }).collect_vec();
        let points = (0..num_samples)
            .map(|i| Point {
                val: f64::round(rnd.sample::<f64, _>(StandardNormal) * 10.0),
                idx: i % num_samples / 2,
            })
            .collect_vec();

        let mut points_index: BTreeSet<Point> = Default::default();

        let mut histogram = Histogram::new(max_bucket_size, precision);

        for point in &points {
            points_index.insert(point.clone());
            // print_results(&points_index, &histogram, Some(point.clone()));
            histogram.insert(
                point.clone(),
                |x| {
                    points_index
                        .range((Unbounded, Excluded(x)))
                        .next_back()
                        .cloned()
                },
                |x| points_index.range((Excluded(x), Unbounded)).next().cloned(),
            );
        }

        for point in &points {
            print_results(&points_index, &histogram, Some(point.clone()));
            points_index.remove(point);
            histogram.remove(
                point,
                |x| {
                    points_index
                        .range((Unbounded, Excluded(x)))
                        .next_back()
                        .cloned()
                },
                |x| points_index.range((Excluded(x), Unbounded)).next().cloned(),
            );
        }
    }

    fn test_range_by_cardinality(histogram: &Histogram) {
        let from = Unbounded;
        let range_size = 100;
        let to = histogram.get_range_by_size(from, range_size);
        let estimation = histogram.estimate(from, to);
        eprintln!("({from:?} - {to:?}) -> {estimation:?} / {range_size}");
        assert!(
            (estimation.1 as i64 - range_size as i64).abs()
                < 2 * histogram.current_bucket_size() as i64
        );

        let from = Unbounded;
        let range_size = 1000;
        let to = histogram.get_range_by_size(from, range_size);
        let estimation = histogram.estimate(from, to);
        eprintln!("({from:?} - {to:?}) -> {estimation:?} / {range_size}");
        assert!(
            (estimation.1 as i64 - range_size as i64).abs()
                < 2 * histogram.current_bucket_size() as i64
        );

        let from = Excluded(0.1);
        let range_size = 100;
        let to = histogram.get_range_by_size(from, range_size);
        let estimation = histogram.estimate(from, to);
        eprintln!("({from:?} - {to:?}) -> {estimation:?} / {range_size}");
        assert!(
            (estimation.1 as i64 - range_size as i64).abs()
                < 2 * histogram.current_bucket_size() as i64
        );

        let from = Excluded(0.1);
        let range_size = 1000;
        let to = histogram.get_range_by_size(from, range_size);
        let estimation = histogram.estimate(from, to);
        eprintln!("({from:?} - {to:?}) -> {estimation:?} / {range_size}");
        assert!(
            (estimation.1 as i64 - range_size as i64).abs()
                < 2 * histogram.current_bucket_size() as i64
        );

        let from = Excluded(0.1);
        let range_size = 100_000;
        let to = histogram.get_range_by_size(from, range_size);
        let estimation = histogram.estimate(from, to);
        eprintln!("({from:?} - {to:?}) -> {estimation:?} / {range_size}");
        assert!(matches!(to, Unbounded));
    }

    fn request_histogram(histogram: &Histogram, points_index: &BTreeSet<Point>) {
        let (est_min, estimation, est_max) = histogram.estimate(Included(0.0), Included(0.0));
        let real = count_range(points_index, 0., 0.);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < 2 * histogram.current_bucket_size());

        let (est_min, estimation, est_max) = histogram.estimate(Included(0.0), Included(0.0001));
        let real = count_range(points_index, 0., 0.0001);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < 2 * histogram.current_bucket_size());

        let (est_min, estimation, est_max) = histogram.estimate(Included(0.0), Included(0.01));
        let real = count_range(points_index, 0., 0.01);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < 2 * histogram.current_bucket_size());

        let (est_min, estimation, est_max) = histogram.estimate(Included(0.), Included(1.));
        let real = count_range(points_index, 0., 1.);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < 2 * histogram.current_bucket_size());

        let (est_min, estimation, est_max) = histogram.estimate(Included(0.), Included(100.));
        let real = count_range(points_index, 0., 100.);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < 2 * histogram.current_bucket_size());

        let (est_min, estimation, est_max) = histogram.estimate(Included(-100.), Included(100.));
        let real = count_range(points_index, -100., 100.);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < 2 * histogram.current_bucket_size());

        let (est_min, estimation, est_max) = histogram.estimate(Included(20.), Included(100.));
        let real = count_range(points_index, 20., 100.);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < 2 * histogram.current_bucket_size());
    }

    fn build_histogram(
        max_bucket_size: usize,
        precision: f64,
        points: Vec<Point>,
    ) -> (Histogram, BTreeSet<Point>) {
        let mut points_index: BTreeSet<Point> = Default::default();
        let mut histogram = Histogram::new(max_bucket_size, precision);

        let read_counter = Cell::new(0);
        for point in points {
            points_index.insert(point.clone());
            // print_results(&points_index, &histogram, Some(point.clone()));
            histogram.insert(
                point,
                |x| {
                    read_counter.set(read_counter.get() + 1);
                    points_index
                        .range((Unbounded, Excluded(x)))
                        .next_back()
                        .cloned()
                },
                |x| {
                    read_counter.set(read_counter.get() + 1);
                    points_index.range((Excluded(x), Unbounded)).next().cloned()
                },
            );
        }
        eprintln!("read_counter.get() = {:#?}", read_counter.get());
        eprintln!("histogram.borders.len() = {:#?}", histogram.borders.len());
        for border in histogram.borders.iter().take(5) {
            eprintln!("border = {:?}", border);
        }
        (histogram, points_index)
    }

    #[test]
    fn test_build_histogram_round() {
        let max_bucket_size = 100;
        let precision = 0.01;
        let num_samples = 100_000;
        let mut rnd = StdRng::seed_from_u64(42);

        // let points = (0..100000).map(|i| Point { val: rnd.gen_range(-10.0..10.0), idx: i }).collect_vec();
        let points = (0..num_samples).map(|i| Point {
            val: f64::round(rnd.sample::<f64, _>(StandardNormal) * 100.0),
            idx: i,
        });
        let (histogram, points_index) =
            build_histogram(max_bucket_size, precision, points.collect());

        request_histogram(&histogram, &points_index);
    }

    #[test]
    fn test_build_histogram() {
        let max_bucket_size = 1000;
        let precision = 0.01;
        let num_samples = 100_000;
        let mut rnd = StdRng::seed_from_u64(42);

        // let points = (0..100000).map(|i| Point { val: rnd.gen_range(-10.0..10.0), idx: i }).collect_vec();
        let points = (0..num_samples)
            .map(|i| Point {
                val: rnd.sample(StandardNormal),
                idx: i,
            })
            .collect_vec();

        let (histogram, points_index) = build_histogram(max_bucket_size, precision, points);
        request_histogram(&histogram, &points_index);
        test_range_by_cardinality(&histogram);
    }
}
