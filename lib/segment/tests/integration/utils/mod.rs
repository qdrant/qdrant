use segment::json_path::JsonPath;

pub mod scored_point_ties;

pub fn path(p: &str) -> JsonPath {
    p.parse().unwrap()
}
