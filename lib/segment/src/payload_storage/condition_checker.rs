//! Contains functions for interpreting filter queries and defining if given points pass the conditions

use crate::types::{GeoBoundingBox, GeoRadius, Match, PayloadType, Range};
use geo::algorithm::haversine_distance::HaversineDistance;
use geo::Point;

pub fn match_payload(payload: &PayloadType, condition_match: &Match) -> bool {
    match payload {
        PayloadType::Keyword(payload_kws) => payload_kws.iter().any(|payload_kw| {
            condition_match
                .keyword
                .as_ref()
                .map_or(false, |x| x == payload_kw)
        }),
        PayloadType::Integer(payload_ints) => payload_ints
            .iter()
            .copied()
            .any(|payload_int| condition_match.integer.map_or(false, |x| x == payload_int)),
        _ => false,
    }
}

pub fn match_range(payload: &PayloadType, num_range: &Range) -> bool {
    let condition = |number| {
        num_range.lt.map_or(true, |x| number < x)
            && num_range.gt.map_or(true, |x| number > x)
            && num_range.lte.map_or(true, |x| number <= x)
            && num_range.gte.map_or(true, |x| number >= x)
    };

    match payload {
        PayloadType::Float(num) => num.iter().copied().any(condition),
        PayloadType::Integer(num) => num.iter().copied().any(|x| condition(x as f64)),
        _ => false,
    }
}

pub fn match_geo(payload: &PayloadType, geo_bounding_box: &GeoBoundingBox) -> bool {
    return match payload {
        PayloadType::Geo(geo_points) => geo_points.iter().any(|geo_point| {
            (geo_bounding_box.top_left.lon < geo_point.lon)
                && (geo_point.lon < geo_bounding_box.bottom_right.lon)
                && (geo_bounding_box.bottom_right.lat < geo_point.lat)
                && (geo_point.lat < geo_bounding_box.top_left.lat)
        }),
        _ => false,
    };
}

pub fn match_geo_radius(payload: &PayloadType, geo_radius_query: &GeoRadius) -> bool {
    return match payload {
        PayloadType::Geo(geo_points) => {
            let query_center = Point::new(geo_radius_query.center.lon, geo_radius_query.center.lat);

            geo_points.iter().any(|geo_point| {
                query_center.haversine_distance(&Point::new(geo_point.lon, geo_point.lat))
                    < geo_radius_query.radius
            })
        }
        _ => false,
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::GeoPoint;

    #[test]
    fn test_geo_matching() {
        let berlin_and_moscow = PayloadType::Geo(vec![
            GeoPoint {
                lat: 52.52197645,
                lon: 13.413637435864272,
            },
            GeoPoint {
                lat: 55.7536283,
                lon: 37.62137960067377,
            },
        ]);
        let near_berlin_query = GeoRadius {
            center: GeoPoint {
                lat: 52.511,
                lon: 13.423637,
            },
            radius: 2000.0,
        };
        let miss_geo_query = GeoRadius {
            center: GeoPoint {
                lat: 52.511,
                lon: 20.423637,
            },
            radius: 2000.0,
        };

        assert!(match_geo_radius(&berlin_and_moscow, &near_berlin_query));
        assert!(!match_geo_radius(&berlin_and_moscow, &miss_geo_query));
    }
}
