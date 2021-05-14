use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{Distance, PointIdType, VectorElementType, ScoredPoint, ScoreType};
use segment::segment::Segment;
use std::path::Path;
use segment::entry::entry_point::{OperationResult, SegmentEntry, OperationError};
use numpy::PyArray1;
use pyo3::PyErr;
use pyo3::exceptions::PyException;


#[pyclass(unsendable, module="qdrant_segment_py")]
struct PySegment {
    pub segment: Segment
}

fn handle_inner_result<T> (result: OperationResult<T>) -> PyResult<T> {
    match result {
        Err(error) => {
            match error {
                OperationError::WrongVector {expected_dim, received_dim} => Err(PyErr::new::<PyException, _>(format!("Wrong vector, expected_dim {} is different from received_dim {}", expected_dim, received_dim))),
                OperationError::PointIdError {missed_point_id} => Err(PyErr::new::<PyException, _>(format!("Wrong point id, Missed id {}", missed_point_id))),
                OperationError::TypeError {field_name, expected_type} => Err(PyErr::new::<PyException, _>(format!("Type Error, Field {} should be of type {}", field_name, expected_type))),
                OperationError::ServiceError {description} => Err(PyErr::new::<PyException, _>(format!("Service Error: {}", description))),
            }
        },
        Ok(inner_result) => Ok(inner_result)
    }
}

impl PySegment {
    pub fn index(&mut self, point_id: PointIdType, vector: &PyArray1<VectorElementType>) -> PyResult<bool> {
        let result = self.segment.upsert_point(100, point_id, &vector.to_vec().unwrap());
        handle_inner_result(result)
    }

    pub fn search(&self, vector: &PyArray1<VectorElementType>, top_k: usize) -> PyResult<(Vec<PointIdType>, Vec<ScoreType>)> {
        fn _convert_scored_point_vec(vec: Vec<ScoredPoint>) -> (Vec<PointIdType>, Vec<ScoreType>) {
            vec.into_iter().map(
                |scored_point| (scored_point.id, scored_point.score)).unzip()
        }
        let result = self.segment.search(&vector.to_vec().unwrap(), None, top_k, None);
        handle_inner_result(result.map(|vec| _convert_scored_point_vec(vec)))
    }
}

#[pyfunction]
fn get_simple_segment(dir: String, vecor_dim: usize) -> PyResult<PySegment> {
    let segment_result = handle_inner_result(build_simple_segment(Path::new(&dir), vecor_dim, Distance::Dot));
    segment_result.map(|segment| PySegment{segment})
}

#[pyfunction]
fn index(pysegment: &mut PySegment, point_id: PointIdType, vector: &PyArray1<VectorElementType>) -> PyResult<bool> {
    pysegment.index(point_id, vector)
}

#[pyfunction]
fn search(pysegment: &PySegment, vector: &PyArray1<VectorElementType>, top_k: usize) -> PyResult<(Vec<PointIdType>, Vec<ScoreType>)>{
    pysegment.search(vector, top_k)
}

/// A Python module implemented in Rust.
#[pymodule]
fn qdrant_segment_py(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PySegment>()?;
    m.add_function(wrap_pyfunction!(get_simple_segment, m)?).unwrap();
    m.add_function(wrap_pyfunction!(index, m)?).unwrap();
    m.add_function(wrap_pyfunction!(search, m)?).unwrap();

    Ok(())
}
