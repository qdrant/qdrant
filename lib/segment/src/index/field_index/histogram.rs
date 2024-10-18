use std::collections::BTreeMap;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::ops::Bound;
use std::path::{Path, PathBuf};

use common::types::PointOffsetType;
use io::file_operations::{atomic_save_bin, atomic_save_json, read_bin, read_json};
use itertools::Itertools;
use num_traits::Num;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::common::operation_error::OperationResult;
use crate::index::field_index::utils::check_boundaries;

const MIN_BUCKET_SIZE: usize = 10;
const CONFIG_PATH: &str = "histogram_config.json";
const BORDERS_PATH: &str = "histogram_borders.bin";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Counts {
    pub left: usize,
    pub right: usize,
}

#[derive(PartialEq, PartialOrd, Debug, Clone, Serialize, Deserialize)]
#[repr(C)]
pub struct Point<T> {
    pub val: T,
    pub idx: PointOffsetType,
}

impl<T> Point<T> {
    pub fn new(val: T, idx: PointOffsetType) -> Self {
        Self { val, idx }
    }
}

impl<T: PartialEq> Eq for Point<T> {}

#[allow(clippy::derive_ord_xor_partial_ord)]
impl<T: PartialOrd + Copy> Ord for Point<T> {
    fn cmp(&self, other: &Point<T>) -> std::cmp::Ordering {
        (self.val, self.idx)
            .partial_cmp(&(other.val, other.idx))
            .unwrap()
    }
}

/// A trait that should represent common properties of integer and floating point types.
/// In particular, i64 and f64.
pub trait Numericable: Num + PartialEq + PartialOrd + Copy {
    fn min_value() -> Self;
    fn max_value() -> Self;
    fn to_f64(self) -> f64;
    fn from_f64(x: f64) -> Self;
    fn from_u128(x: u128) -> Self;
    fn min(self, b: Self) -> Self {
        if self < b {
            self
        } else {
            b
        }
    }
    fn max(self, b: Self) -> Self {
        if self > b {
            self
        } else {
            b
        }
    }
    fn abs_diff(self, b: Self) -> Self {
        if self > b {
            self - b
        } else {
            b - self
        }
    }
}

impl Numericable for i64 {
    fn min_value() -> Self {
        i64::MIN
    }
    fn max_value() -> Self {
        i64::MAX
    }
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn from_f64(x: f64) -> Self {
        x as Self
    }
    fn from_u128(x: u128) -> Self {
        x as i64
    }
    fn abs_diff(self, b: Self) -> Self {
        i64::abs_diff(self, b) as i64
    }
}

impl Numericable for f64 {
    fn min_value() -> Self {
        f64::MIN
    }
    fn max_value() -> Self {
        f64::MAX
    }
    fn to_f64(self) -> f64 {
        self
    }
    fn from_f64(x: f64) -> Self {
        x
    }
    fn from_u128(x: u128) -> Self {
        x as Self
    }
}

impl Numericable for u128 {
    fn min_value() -> Self {
        u128::MIN
    }

    fn max_value() -> Self {
        u128::MAX
    }

    fn to_f64(self) -> f64 {
        self as f64
    }

    fn from_f64(x: f64) -> Self {
        x as u128
    }

    fn from_u128(x: u128) -> Self {
        x
    }

    fn abs_diff(self, b: Self) -> Self {
        u128::abs_diff(self, b)
    }
}

#[derive(Debug, PartialEq)]
pub struct Histogram<T: Numericable + Serialize + DeserializeOwned> {
    max_bucket_size: usize,
    precision: f64,
    total_count: usize,
    borders: BTreeMap<Point<T>, Counts>,
}

#[derive(Debug, Serialize, Deserialize)]
struct HistogramConfig {
    max_bucket_size: usize,
    precision: f64,
    total_count: usize,
}

impl<T: Numericable + Serialize + DeserializeOwned> Histogram<T> {
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

    pub fn load(path: &Path) -> OperationResult<Self> {
        let config_path = path.join(CONFIG_PATH);
        let borders_path = path.join(BORDERS_PATH);

        let histogram_config: HistogramConfig = read_json(&config_path)?;
        let histogram_buckets: Vec<(Point<T>, Counts)> = read_bin(&borders_path)?;

        Ok(Self {
            max_bucket_size: histogram_config.max_bucket_size,
            precision: histogram_config.precision,
            total_count: histogram_config.total_count,
            borders: histogram_buckets.into_iter().collect(),
        })
    }

    pub fn save(&self, path: &Path) -> OperationResult<()> {
        let config_path = path.join(CONFIG_PATH);
        let borders_path = path.join(BORDERS_PATH);

        atomic_save_json(
            &config_path,
            &HistogramConfig {
                max_bucket_size: self.max_bucket_size,
                precision: self.precision,
                total_count: self.total_count,
            },
        )?;

        let borders: Vec<(Point<T>, Counts)> = self
            .borders
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        atomic_save_bin(&borders_path, &borders)?;
        Ok(())
    }

    pub fn files(path: &Path) -> Vec<PathBuf> {
        vec![path.join(CONFIG_PATH), path.join(BORDERS_PATH)]
    }

    #[cfg(test)]
    pub fn total_count(&self) -> usize {
        self.total_count
    }

    #[cfg(test)]
    pub fn borders(&self) -> &BTreeMap<Point<T>, Counts> {
        &self.borders
    }

    pub fn current_bucket_size(&self) -> usize {
        let bucket_size = (self.total_count as f64 * self.precision) as usize;
        bucket_size.clamp(MIN_BUCKET_SIZE, self.max_bucket_size)
    }

    pub fn get_total_count(&self) -> usize {
        self.total_count
    }

    /// Infers boundaries for bucket of given size and starting point.
    /// Returns `to` range of values starting provided `from`value which is expected to contain
    /// `range_size` values
    ///
    /// Returns `Unbounded` if there are no points stored
    pub fn get_range_by_size(&self, from: Bound<T>, range_size: usize) -> Bound<T> {
        let from_ = match from {
            Included(val) => Included(Point {
                val,
                idx: PointOffsetType::MIN,
            }),
            Excluded(val) => Excluded(Point {
                val,
                idx: PointOffsetType::MAX,
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

    pub fn estimate(&self, from: Bound<T>, to: Bound<T>) -> (usize, usize, usize) {
        let from_ = match &from {
            Included(val) => Included(Point {
                val: *val,
                idx: PointOffsetType::MIN,
            }),
            Excluded(val) => Excluded(Point {
                val: *val,
                idx: PointOffsetType::MAX,
            }),
            Unbounded => Unbounded,
        };

        let to_ = match &to {
            Included(val) => Included(Point {
                val: *val,
                idx: PointOffsetType::MAX,
            }),
            Excluded(val) => Excluded(Point {
                val: *val,
                idx: PointOffsetType::MIN,
            }),
            Unbounded => Unbounded,
        };

        // Value for range fraction estimation
        let from_val = match from {
            Included(val) => val,
            Excluded(val) => val,
            Unbounded => T::min_value(),
        };

        let to_val = match to {
            Included(val) => val,
            Excluded(val) => val,
            Unbounded => T::max_value(),
        };

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

        if !check_boundaries(&from_, &to_) {
            return (0, 0, 0);
        }

        let estimation = left_border
            .into_iter()
            .chain(self.borders.range((from_, to_)))
            .chain(right_border)
            .tuple_windows()
            .map(
                |((a, a_count), (b, _b_count)): ((&Point<T>, &Counts), (&Point<T>, _))| {
                    let val_range = (b.val - a.val).to_f64();

                    if val_range == 0. {
                        // Zero-length range is always covered
                        let estimates = a_count.right + 1;
                        return (estimates, estimates, estimates);
                    }

                    if a_count.right == 0 {
                        // Range covers most-right border
                        return (1, 1, 1);
                    }

                    let cover_range = (to_val.min(b.val) - from_val.max(a.val)).to_f64();

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

    pub fn remove<F, G>(&mut self, val: &Point<T>, left_neighbour: F, right_neighbour: G)
    where
        F: Fn(&Point<T>) -> Option<Point<T>>,
        G: Fn(&Point<T>) -> Option<Point<T>>,
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
                        if let Some((_fln, ref mut fln_count)) = &mut far_left_neighbor {
                            fln_count.right -= 1
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
                        if let Some((_frn, ref mut frn_count)) = &mut far_right_neighbor {
                            frn_count.left -= 1
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
                        if let Some((_fln, ref mut fln_count)) = &mut far_left_neighbor {
                            fln_count.right += right_border_count.left;
                            right_border_count.left = fln_count.right;
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
                        if let Some((_frn, ref mut frn_count)) = &mut far_right_neighbor {
                            frn_count.left += left_border_count.right;
                            left_border_count.right = frn_count.left;
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
    pub fn insert<F, G>(&mut self, val: Point<T>, left_neighbour: F, right_neighbour: G)
    where
        F: Fn(&Point<T>) -> Option<Point<T>>,
        G: Fn(&Point<T>) -> Option<Point<T>>,
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
                    if let Some((_frn, frn_count)) = &mut far_right_neighbor {
                        frn_count.left = new_count;
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
                    if let Some((_fln, ref mut fln_count)) = &mut far_left_neighbor {
                        fln_count.right = new_count
                    }
                    (
                        Some(left_border.clone()),
                        Some((new_border, new_border_count)),
                    )
                }
            }
            (Some((left_border, left_border_count)), Some((right_border, right_border_count))) => {
                assert_eq!(left_border_count.right, right_border_count.left);
                let new_count = left_border_count.right + 1;

                if new_count > self.current_bucket_size() {
                    // Too many values, let's adjust
                    // Decide which border to move
                    let left_dist = val.val.abs_diff(left_border.val);
                    let right_dist = val.val.abs_diff(right_border.val);
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
                            if let Some((_fln, ref mut fln_count)) = &mut far_left_neighbor {
                                fln_count.right = new_border_count.left
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
                            if let Some((_frn, frn_count)) = &mut far_right_neighbor {
                                frn_count.left = new_border_count.right
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
