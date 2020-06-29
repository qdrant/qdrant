use crate::payload_storage::payload_storage::{ConditionChecker, PayloadStorage, TheMap};
use crate::types::{Filter, PayloadKeyType, PayloadType, Condition, GeoBoundingBox, Range, Match};


fn match_payload(payload: &PayloadType, condition_match: &Match) -> bool {
    match payload {
        PayloadType::Keyword(payload_kw) => condition_match.keyword
            .as_ref().map(|x| x == payload_kw).unwrap_or(false),
        &PayloadType::Integer(payload_int) => condition_match.integer
            .map(|x| x == payload_int).unwrap_or(false),
        _ => false
    }
}

fn match_range(
    payload: &PayloadType,
    num_range: &Range
) -> bool {
    let number: Option<f64> = match payload {
        &PayloadType::Float(num) => Some(num),
        &PayloadType::Integer(num) => Some(num as f64),
        _ => None
    };

    match number {
        Some(number) => num_range.lt.map_or(true, |x| number < x)
            && num_range.gt.map_or(true, |x| number > x)
            && num_range.lte.map_or(true, |x| number <= x)
            && num_range.gte.map_or(true, |x| number >= x),
        None => false
    }
}

fn match_geo(
    payload: &PayloadType,
    geo_bounding_box: &GeoBoundingBox
) -> bool {
    return match payload {
        PayloadType::Geo(geo_point) => {
            (geo_bounding_box.top_left.lon < geo_point.lon) && (geo_point.lon < geo_bounding_box.bottom_right.lon)
            && (geo_bounding_box.bottom_right.lat < geo_point.lat) && (geo_point.lat < geo_bounding_box.top_left.lat)
        },
        _ => false,
    }
}


fn check_condition(payload: &TheMap<PayloadKeyType, PayloadType>, condition: &Condition) -> bool {
    match condition {
        Condition::Filter(filter) => check_filter(payload, filter),
        Condition::Match (condition_match) => {
            payload.get(&condition_match.key)
                .map(|p| match_payload(p, condition_match))
                .unwrap_or(false)
        },
        Condition::Range (range) => {
            payload.get(&range.key)
                .map(|p| match_range(p, range))
                .unwrap_or(false)
        },
        Condition::GeoBoundingBox (geo_bounding_box) => {
            payload.get(&geo_bounding_box.key)
                .map(|p| match_geo(p, geo_bounding_box))
                .unwrap_or(false)
        }
    }
}

fn check_filter(payload: &TheMap<PayloadKeyType, PayloadType>, filter: &Filter) -> bool {
    return check_must(payload, &filter.must) && check_must_not(payload, &filter.must_not);
}

fn check_must(payload: &TheMap<PayloadKeyType, PayloadType>, must: &Option<Vec<Condition>>) -> bool {
    let check = |x| check_condition(payload, x);
    match must {
        None => true,
        Some(conditions) => conditions.iter().all(check)
    }
}

fn check_must_not(payload: &TheMap<PayloadKeyType, PayloadType>, must: &Option<Vec<Condition>>) -> bool {
    let check = |x| !check_condition(payload, x);
    match must {
        None => true,
        Some(conditions) => conditions.iter().all(check)
    }
}

impl<T> ConditionChecker for T
    where T: PayloadStorage
{
    fn check(&self, point_id: usize, query: &Filter) -> bool {
        let payload = self.payload(point_id);
        return check_filter(&payload, query);
    }
}