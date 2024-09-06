use std::collections::BTreeSet;
use std::fmt::Display;
use std::io;
use std::io::Write;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::index::field_index::histogram::{Histogram, Numericable, Point};

pub fn print_results<T: Numericable + Serialize + DeserializeOwned + Display>(
    points_index: &BTreeSet<Point<T>>,
    histogram: &Histogram<T>,
    pnt: Option<Point<T>>,
) {
    for point in points_index.iter() {
        if let Some(border_count) = histogram.borders().get(point) {
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
    eprintln!("[{}]", histogram.total_count());
    io::stdout().flush().unwrap();
}
