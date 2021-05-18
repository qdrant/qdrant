use pyo3::prelude::*;
use segment::types::{Distance, PointIdType, VectorElementType, ScoredPoint, ScoreType, Indexes, PayloadIndexType, StorageType, SegmentConfig, PayloadKeyType, TheMap, PayloadType, IntPayloadType, FloatPayloadType};
use segment::segment::Segment;
use std::path::Path;
use segment::entry::entry_point::{OperationResult, SegmentEntry, OperationError};
use numpy::PyArray1;
use pyo3::PyErr;
use pyo3::exceptions::PyException;
use segment::segment_constructor::segment_constructor::build_segment;
use std::io::BufReader;
use std::fs::File;
use serde_json;

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

#[pyclass(unsendable, module="qdrant_segment_py", dict)]
struct PyVectorIndexType {
    index: Indexes
}

#[pymethods]
impl PyVectorIndexType {
    #[new]
    fn new(index_type: usize, m: Option<usize>, ef: Option<usize>) -> Self {
        let ind = match index_type {
            0 => Some(Indexes::Plain {}),
            1 => Some(Indexes::Hnsw { m: m.unwrap(), ef_construct: ef.unwrap()}),
            x => {
                println!("Invalid vector index type {}", x);
                None
            },
        };

        PyVectorIndexType { index: ind.unwrap() }
    }
}

#[pyclass(unsendable, module="qdrant_segment_py", dict)]
struct PyPayloadIndexType {
    payload_index_type: PayloadIndexType
}

#[pymethods]
impl PyPayloadIndexType {
    //TODO: LEARN HOW TO ALSO ENABLE READING FROM TEXT
    #[new]
    fn new(payload_index: usize) -> Self {
        let pind = match payload_index {
            0 => Some(PayloadIndexType::Plain {}),
            1 => Some(PayloadIndexType::Struct {}),
            x => {
                println!("Invalid payload index type {}", x);
                None
            },
        };
        PyPayloadIndexType { payload_index_type: pind.unwrap()}
    }
}

#[pyclass(unsendable, module="qdrant_segment_py", dict)]
struct PyDistanceType {
    distance: Distance
}

#[pymethods]
impl PyDistanceType {
    //TODO: LEARN HOW TO ALSO ENABLE READING FROM TEXT
    #[new]
    fn new(distance: usize) -> Self {
        let d = match distance {
            0 => Some(Distance::Cosine),
            1 => Some(Distance::Dot),
            2 => Some(Distance::Euclid),
            x => {
                println!("Invalid distance type {}", x);
                None
            },
        };
        PyDistanceType { distance: d.unwrap()}
    }
}

#[pyclass(unsendable, module="qdrant_segment_py", dict)]
struct PyStorageType {
    storage: StorageType
}

#[pymethods]
impl PyStorageType {
    //TODO: LEARN HOW TO ALSO ENABLE READING FROM TEXT
    #[new]
    fn new(storage: usize) -> Self {
        let stype = match storage {
            0 => Some(StorageType::InMemory),
            1 => Some(StorageType::Mmap),
            x => {
                println!("Invalid storage type {}", x);
                None
            },
        };
        PyStorageType { storage: stype.unwrap()}
    }
}

#[pyclass(unsendable, module="qdrant_segment_py", dict)]
struct PySegmentConfig {
    config: SegmentConfig
}

#[pymethods]
impl PySegmentConfig {
    #[new]
    fn new(vector_size: usize,
           index: &PyVectorIndexType,
           payload_index: Option<&PyPayloadIndexType>,
           distance: &PyDistanceType,
           storage_type: &PyStorageType) -> Self {

        let config = SegmentConfig { vector_size,
            index: index.index,
            payload_index: payload_index.map(|pid| pid.payload_index_type),
            distance: distance.distance,
            storage_type: storage_type.storage};

        PySegmentConfig { config }
    }

    #[staticmethod]
    fn from_config_file(dir: String) ->  Self {
        let file = File::open(Path::new(&dir));
        match file {
            Ok(f) => PySegmentConfig {config: serde_json::from_reader( BufReader::new(f)).unwrap() },
            _ => PySegmentConfig { config: SegmentConfig {
                vector_size: 0,
                index: Default::default(),
                payload_index: None,
                distance: Distance::Cosine,
                storage_type: Default::default()
            } }
        }
    }
}

#[derive(Debug, Clone, FromPyObject)]
enum PyPayloadType {
    Keyword(Vec<String>),
    Integer(IntPayloadType),
    Float(FloatPayloadType),
    IntegerVec(Vec<IntPayloadType>),
    FloatVec(Vec<FloatPayloadType>),
}

#[pyclass(unsendable, module="qdrant_segment_py", dict)]
struct PySegment {
    pub segment: Segment
}

#[pymethods]
impl PySegment {
    const DEFAULT_OP_NUM: u64 = u64::MAX; // Disable skip_by_version for now

    #[new]
    fn new(dir: String, config: &PySegmentConfig) -> Self {
        let segment_result = handle_inner_result(build_segment(Path::new(&dir), &config.config));
        segment_result.map(|segment| PySegment{segment}).unwrap()
    }

    pub fn index(&mut self, point_id: PointIdType, vector: &PyArray1<VectorElementType>) -> PyResult<bool> {
        let result = self.segment.upsert_point(PySegment::DEFAULT_OP_NUM, point_id, &vector.to_vec().unwrap());
        handle_inner_result(result)
    }

    pub fn set_full_payload(&mut self, point_id: PointIdType, payload: TheMap<PayloadKeyType, PyPayloadType>) -> PyResult<bool> {
        let inner_payload = payload.into_iter().map(|(k, v)| {
            match v {
                PyPayloadType::Keyword(x) => (k, PayloadType::Keyword(x)),
                PyPayloadType::Integer(x) => (k, PayloadType::Integer(vec![x])),
                PyPayloadType::Float(x) => (k, PayloadType::Float(vec![x])),
                PyPayloadType::FloatVec(x) => (k, PayloadType::Float(x)),
                PyPayloadType::IntegerVec(x) => (k, PayloadType::Integer(x))
            }
        }).rev().collect();
        let result = self.segment.set_full_payload(PySegment::DEFAULT_OP_NUM, point_id, inner_payload);
        handle_inner_result(result)
    }

    fn get_full_payload(&self, point_id: PointIdType) -> TheMap<PayloadKeyType, String> {
        let payload = self.segment.payload(point_id).unwrap();
        let mut results = TheMap::new();
        for (k, _v) in payload {
            match _v {
                PayloadType::Keyword(x) => results.insert(k,  x.iter().map(String::from).collect()),
                PayloadType::Integer(x) => results.insert(k,  x.iter().map(|k| k.to_string()).collect()),
                PayloadType::Float(x) => results.insert(k,  x.iter().map(|k| k.to_string()).collect()),
                _ => None
            };
        }
        results
    }

    pub fn delete(&mut self, point_id: PointIdType) -> PyResult<bool> {
        let result = self.segment.delete_point(PySegment::DEFAULT_OP_NUM, point_id);
        handle_inner_result(result)
    }

    pub fn search(&self, vector: &PyArray1<VectorElementType>, filter: Option<String>, top_k: usize) -> PyResult<(Vec<PointIdType>, Vec<ScoreType>)> {
        fn _convert_scored_point_vec(vec: Vec<ScoredPoint>) -> (Vec<PointIdType>, Vec<ScoreType>) {
            vec.into_iter().map(
                |scored_point| (scored_point.id, scored_point.score)).unzip()
        }
        let qdrant_filter = filter.map(|f| {
            serde_json::from_str(&f).unwrap()
        });
        let result = self.segment.search(&vector.to_vec().unwrap(), Option::from(&qdrant_filter), top_k, None);
        handle_inner_result(result.map(|vec| _convert_scored_point_vec(vec)))
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn qdrant_segment_py(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyVectorIndexType>()?;
    m.add_class::<PyPayloadIndexType>()?;
    m.add_class::<PyDistanceType>()?;
    m.add_class::<PyStorageType>()?;
    m.add_class::<PySegmentConfig>()?;
    m.add_class::<PySegment>()?;
    Ok(())
}
