use std::collections::HashMap;
use std::mem;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use segment::types::*;

#[pyclass(name = "SegmentConfig")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PySegmentConfig(SegmentConfig);

#[pymethods]
impl PySegmentConfig {
    #[new]
    pub fn new(
        vector_data: HashMap<String, PyVectorDataConfig>,
        sparse_vector_data: HashMap<String, PySparseVectorDataConfig>,
        payload_storage_type: PyPayloadStorageType,
    ) -> Self {
        Self(SegmentConfig {
            vector_data: PyVectorDataConfig::peel_map(vector_data),
            sparse_vector_data: PySparseVectorDataConfig::peel_map(sparse_vector_data),
            payload_storage_type: PayloadStorageType::from(payload_storage_type),
        })
    }

    #[getter]
    pub fn vector_data(&self) -> &HashMap<String, PyVectorDataConfig> {
        PyVectorDataConfig::wrap_map_ref(&self.0.vector_data)
    }

    #[getter]
    pub fn sparse_vector_data(&self) -> &HashMap<String, PySparseVectorDataConfig> {
        PySparseVectorDataConfig::wrap_map_ref(&self.0.sparse_vector_data)
    }
}

#[pyclass(name = "VectorDataConfig")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyVectorDataConfig(VectorDataConfig);

impl PyVectorDataConfig {
    fn peel_map(map: HashMap<String, Self>) -> HashMap<String, VectorDataConfig>
    where
        Self: TransparentWrapper<VectorDataConfig>,
    {
        unsafe { mem::transmute(map) }
    }

    fn wrap_map_ref(map: &HashMap<String, VectorDataConfig>) -> &HashMap<String, Self>
    where
        Self: TransparentWrapper<VectorDataConfig>,
    {
        unsafe { mem::transmute(map) }
    }
}

#[pymethods]
impl PyVectorDataConfig {
    #[new]
    pub fn new(
        size: usize,
        distance: PyDistance,
        storage_type: PyVectorStorageType,
        index: PyIndexes,
        quantization_config: Option<PyQuantizationConfig>,
        multivector_config: Option<PyMultiVectorConfig>,
        datatype: Option<PyVectorStorageDatatype>,
    ) -> Self {
        Self(VectorDataConfig {
            size,
            distance: Distance::from(distance),
            storage_type: VectorStorageType::from(storage_type),
            index: Indexes::from(index),
            quantization_config: quantization_config.map(QuantizationConfig::from),
            multivector_config: multivector_config.map(MultiVectorConfig::from),
            datatype: datatype.map(VectorStorageDatatype::from),
        })
    }

    #[getter]
    pub fn size(&self) -> usize {
        self.0.size
    }

    #[getter]
    pub fn distance(&self) -> PyDistance {
        PyDistance(self.0.distance)
    }

    #[getter]
    pub fn storage_type(&self) -> PyVectorStorageType {
        PyVectorStorageType(self.0.storage_type)
    }

    #[getter]
    pub fn index(&self) -> PyIndexes {
        PyIndexes(self.0.index.clone())
    }

    #[getter]
    pub fn quantization_config(&self) -> Option<PyQuantizationConfig> {
        self.0.quantization_config.clone().map(PyQuantizationConfig)
    }

    #[getter]
    pub fn multivector_config(&self) -> Option<PyMultiVectorConfig> {
        self.0.multivector_config.map(PyMultiVectorConfig)
    }

    #[getter]
    pub fn datatype(&self) -> Option<PyVectorStorageDatatype> {
        self.0.datatype.map(PyVectorStorageDatatype::from)
    }
}

impl<'py> IntoPyObject<'py> for &PyVectorDataConfig {
    type Target = PyVectorDataConfig;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        Bound::new(py, self.clone())
    }
}

#[pyclass(name = "Distance")]
#[derive(Copy, Clone, Debug, Into)]
pub struct PyDistance(Distance);

#[pymethods]
impl PyDistance {
    #[classattr]
    pub const COSINE: Self = Self(Distance::Cosine);

    #[classattr]
    pub const EUCLID: Self = Self(Distance::Euclid);

    #[classattr]
    pub const DOT: Self = Self(Distance::Dot);

    #[classattr]
    pub const MANHATTAN: Self = Self(Distance::Manhattan);
}

impl PyDistance {
    fn _variants(distance: Distance) {
        match distance {
            Distance::Cosine => (),
            Distance::Euclid => (),
            Distance::Dot => (),
            Distance::Manhattan => (),
        }
    }
}

#[pyclass(name = "VectorStorageType")]
#[derive(Copy, Clone, Debug, Into)]
pub struct PyVectorStorageType(VectorStorageType);

#[pymethods]
impl PyVectorStorageType {
    #[classattr]
    pub const MEMORY: Self = Self(VectorStorageType::Memory);

    #[classattr]
    pub const MMAP: Self = Self(VectorStorageType::Mmap);

    #[classattr]
    pub const CHUNKED_MMAP: Self = Self(VectorStorageType::ChunkedMmap);

    #[classattr]
    pub const IN_RAM_CHUNKED_MMAP: Self = Self(VectorStorageType::InRamChunkedMmap);
}

impl PyVectorStorageType {
    fn _variants(storage_type: VectorStorageType) {
        match storage_type {
            VectorStorageType::Memory => (),
            VectorStorageType::Mmap => (),
            VectorStorageType::ChunkedMmap => (),
            VectorStorageType::InRamChunkedMmap => (),
        }
    }
}

#[derive(Clone, Debug, Into)]
pub struct PyIndexes(Indexes);

impl FromPyObject<'_, '_> for PyIndexes {
    type Error = PyErr;

    fn extract(indexes: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            Plain(PyPlainIndexConfig),
            Hnsw(PyHnswIndexConfig),
        }

        fn _variants(indexes: Indexes) {
            match indexes {
                Indexes::Plain {} => (),
                Indexes::Hnsw(_) => (),
            }
        }

        let indexes = match indexes.extract()? {
            Helper::Plain(_) => Indexes::Plain {},
            Helper::Hnsw(hnsw) => Indexes::Hnsw(HnswConfig::from(hnsw)),
        };

        Ok(Self(indexes))
    }
}

impl<'py> IntoPyObject<'py> for PyIndexes {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match self.0 {
            Indexes::Plain {} => PyPlainIndexConfig.into_bound_py_any(py),
            Indexes::Hnsw(hnsw) => PyHnswIndexConfig(hnsw).into_bound_py_any(py),
        }
    }
}

#[pyclass(name = "PlainIndexConfig")]
#[derive(Copy, Clone, Debug, Into)]
pub struct PyPlainIndexConfig;

#[pymethods]
impl PyPlainIndexConfig {
    #[new]
    pub fn new() -> Self {
        Self
    }
}

#[pyclass(name = "HnswIndexConfig")]
#[derive(Copy, Clone, Debug, Into)]
pub struct PyHnswIndexConfig(HnswConfig);

#[pymethods]
impl PyHnswIndexConfig {
    #[new]
    #[pyo3(signature = (m, ef_construct, full_scan_threshold, on_disk = None, payload_m = None, inline_storage = None))]
    pub fn new(
        m: usize,
        ef_construct: usize,
        full_scan_threshold: usize,
        on_disk: Option<bool>,
        payload_m: Option<usize>,
        inline_storage: Option<bool>,
    ) -> Self {
        Self(HnswConfig {
            m,
            ef_construct,
            full_scan_threshold,
            max_indexing_threads: 0,
            on_disk,
            payload_m,
            inline_storage,
        })
    }

    #[getter]
    pub fn m(&self) -> usize {
        self.0.m
    }

    #[getter]
    pub fn ef_construct(&self) -> usize {
        self.0.ef_construct
    }

    #[getter]
    pub fn full_scan_threshold(&self) -> usize {
        self.0.full_scan_threshold
    }

    #[getter]
    pub fn on_disk(&self) -> Option<bool> {
        self.0.on_disk
    }

    #[getter]
    pub fn payload_m(&self) -> Option<usize> {
        self.0.payload_m
    }

    #[getter]
    pub fn inline_storage(&self) -> Option<bool> {
        self.0.inline_storage
    }
}

#[derive(Clone, Debug, Into)]
pub struct PyQuantizationConfig(QuantizationConfig);

impl FromPyObject<'_, '_> for PyQuantizationConfig {
    type Error = PyErr;

    fn extract(conf: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            Scalar(PyScalarQuantizationConfig),
            Product(PyProductQuantizationConfig),
            Binary(PyBinaryQuantizationConfig),
        }

        let conf = match conf.extract()? {
            Helper::Scalar(scalar) => QuantizationConfig::Scalar(ScalarQuantization {
                scalar: ScalarQuantizationConfig::from(scalar),
            }),
            Helper::Product(product) => QuantizationConfig::Product(ProductQuantization {
                product: ProductQuantizationConfig::from(product),
            }),
            Helper::Binary(binary) => QuantizationConfig::Binary(BinaryQuantization {
                binary: BinaryQuantizationConfig::from(binary),
            }),
        };

        Ok(Self(conf))
    }
}

impl<'py> IntoPyObject<'py> for PyQuantizationConfig {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match self.0 {
            QuantizationConfig::Scalar(ScalarQuantization { scalar }) => {
                PyScalarQuantizationConfig(scalar).into_bound_py_any(py)
            }
            QuantizationConfig::Product(ProductQuantization { product }) => {
                PyProductQuantizationConfig(product).into_bound_py_any(py)
            }
            QuantizationConfig::Binary(BinaryQuantization { binary }) => {
                PyBinaryQuantizationConfig(binary).into_bound_py_any(py)
            }
        }
    }
}

#[pyclass(name = "ScalarQuantizationConfig")]
#[derive(Clone, Debug, Into)]
pub struct PyScalarQuantizationConfig(ScalarQuantizationConfig);

#[pymethods]
impl PyScalarQuantizationConfig {
    #[new]
    #[pyo3(signature = (r#type, quantile = None, always_ram = None))]
    pub fn new(r#type: PyScalarType, quantile: Option<f32>, always_ram: Option<bool>) -> Self {
        Self(ScalarQuantizationConfig {
            r#type: ScalarType::from(r#type),
            quantile,
            always_ram,
        })
    }

    #[getter]
    pub fn r#type(&self) -> PyScalarType {
        PyScalarType::from(self.0.r#type)
    }

    #[getter]
    pub fn quantile(&self) -> Option<f32> {
        self.0.quantile
    }

    #[getter]
    pub fn always_ram(&self) -> Option<bool> {
        self.0.always_ram
    }
}

#[pyclass(name = "ScalarType")]
#[derive(Copy, Clone, Debug)]
pub enum PyScalarType {
    Int8,
}

impl From<ScalarType> for PyScalarType {
    fn from(scalar_type: ScalarType) -> Self {
        match scalar_type {
            ScalarType::Int8 => PyScalarType::Int8,
        }
    }
}

impl From<PyScalarType> for ScalarType {
    fn from(scalar_type: PyScalarType) -> Self {
        match scalar_type {
            PyScalarType::Int8 => ScalarType::Int8,
        }
    }
}

#[pyclass(name = "ProductQuantizationConfig")]
#[derive(Clone, Debug, Into)]
pub struct PyProductQuantizationConfig(ProductQuantizationConfig);

#[pymethods]
impl PyProductQuantizationConfig {
    #[new]
    #[pyo3(signature = (compression, always_ram = None))]
    pub fn new(compression: PyCompressionRatio, always_ram: Option<bool>) -> Self {
        Self(ProductQuantizationConfig {
            compression: CompressionRatio::from(compression),
            always_ram,
        })
    }

    #[getter]
    pub fn compression(&self) -> PyCompressionRatio {
        PyCompressionRatio::from(self.0.compression)
    }

    #[getter]
    pub fn always_ram(&self) -> Option<bool> {
        self.0.always_ram
    }
}

#[pyclass(name = "CompressionRatio")]
#[derive(Copy, Clone, Debug)]
pub enum PyCompressionRatio {
    X4,
    X8,
    X16,
    X32,
    X64,
}

impl From<CompressionRatio> for PyCompressionRatio {
    fn from(compression: CompressionRatio) -> Self {
        match compression {
            CompressionRatio::X4 => PyCompressionRatio::X4,
            CompressionRatio::X8 => PyCompressionRatio::X8,
            CompressionRatio::X16 => PyCompressionRatio::X16,
            CompressionRatio::X32 => PyCompressionRatio::X32,
            CompressionRatio::X64 => PyCompressionRatio::X64,
        }
    }
}

impl From<PyCompressionRatio> for CompressionRatio {
    fn from(compression: PyCompressionRatio) -> Self {
        match compression {
            PyCompressionRatio::X4 => CompressionRatio::X4,
            PyCompressionRatio::X8 => CompressionRatio::X8,
            PyCompressionRatio::X16 => CompressionRatio::X16,
            PyCompressionRatio::X32 => CompressionRatio::X32,
            PyCompressionRatio::X64 => CompressionRatio::X64,
        }
    }
}

#[pyclass(name = "BinaryQuantizationConfig")]
#[derive(Clone, Debug, Into)]
pub struct PyBinaryQuantizationConfig(BinaryQuantizationConfig);

#[pymethods]
impl PyBinaryQuantizationConfig {
    #[new]
    #[pyo3(signature = (always_ram = None, encoding = None, query_encoding = None))]
    pub fn new(
        always_ram: Option<bool>,
        encoding: Option<PyBinaryQuantizationEncoding>,
        query_encoding: Option<PyBinaryQuantizationQueryEncoding>,
    ) -> Self {
        Self(BinaryQuantizationConfig {
            always_ram,
            encoding: encoding.map(BinaryQuantizationEncoding::from),
            query_encoding: query_encoding.map(BinaryQuantizationQueryEncoding::from),
        })
    }

    #[getter]
    pub fn always_ram(&self) -> Option<bool> {
        self.0.always_ram
    }

    #[getter]
    pub fn encoding(&self) -> Option<PyBinaryQuantizationEncoding> {
        self.0.encoding.map(PyBinaryQuantizationEncoding::from)
    }

    #[getter]
    pub fn query_encoding(&self) -> Option<PyBinaryQuantizationQueryEncoding> {
        self.0
            .query_encoding
            .map(PyBinaryQuantizationQueryEncoding::from)
    }
}

#[pyclass(name = "BinaryQuantizationEncoding")]
#[derive(Copy, Clone, Debug)]
pub enum PyBinaryQuantizationEncoding {
    OneBit,
    TwoBits,
    OneAndHalfBits,
}

impl From<BinaryQuantizationEncoding> for PyBinaryQuantizationEncoding {
    fn from(encoding: BinaryQuantizationEncoding) -> Self {
        match encoding {
            BinaryQuantizationEncoding::OneBit => PyBinaryQuantizationEncoding::OneBit,
            BinaryQuantizationEncoding::TwoBits => PyBinaryQuantizationEncoding::TwoBits,
            BinaryQuantizationEncoding::OneAndHalfBits => {
                PyBinaryQuantizationEncoding::OneAndHalfBits
            }
        }
    }
}

impl From<PyBinaryQuantizationEncoding> for BinaryQuantizationEncoding {
    fn from(encoding: PyBinaryQuantizationEncoding) -> Self {
        match encoding {
            PyBinaryQuantizationEncoding::OneBit => BinaryQuantizationEncoding::OneBit,
            PyBinaryQuantizationEncoding::TwoBits => BinaryQuantizationEncoding::TwoBits,
            PyBinaryQuantizationEncoding::OneAndHalfBits => {
                BinaryQuantizationEncoding::OneAndHalfBits
            }
        }
    }
}

#[pyclass(name = "BinaryQuantizationQueryEncoding")]
#[derive(Copy, Clone, Debug)]
pub enum PyBinaryQuantizationQueryEncoding {
    Default,
    Binary,
    Scalar4Bits,
    Scalar8Bits,
}

impl From<BinaryQuantizationQueryEncoding> for PyBinaryQuantizationQueryEncoding {
    fn from(encoding: BinaryQuantizationQueryEncoding) -> Self {
        match encoding {
            BinaryQuantizationQueryEncoding::Default => PyBinaryQuantizationQueryEncoding::Default,
            BinaryQuantizationQueryEncoding::Binary => PyBinaryQuantizationQueryEncoding::Binary,
            BinaryQuantizationQueryEncoding::Scalar4Bits => {
                PyBinaryQuantizationQueryEncoding::Scalar4Bits
            }
            BinaryQuantizationQueryEncoding::Scalar8Bits => {
                PyBinaryQuantizationQueryEncoding::Scalar8Bits
            }
        }
    }
}

impl From<PyBinaryQuantizationQueryEncoding> for BinaryQuantizationQueryEncoding {
    fn from(encoding: PyBinaryQuantizationQueryEncoding) -> Self {
        match encoding {
            PyBinaryQuantizationQueryEncoding::Default => BinaryQuantizationQueryEncoding::Default,
            PyBinaryQuantizationQueryEncoding::Binary => BinaryQuantizationQueryEncoding::Binary,
            PyBinaryQuantizationQueryEncoding::Scalar4Bits => {
                BinaryQuantizationQueryEncoding::Scalar4Bits
            }
            PyBinaryQuantizationQueryEncoding::Scalar8Bits => {
                BinaryQuantizationQueryEncoding::Scalar8Bits
            }
        }
    }
}

#[pyclass(name = "MultiVectorConfig")]
#[derive(Copy, Clone, Debug, Into)]
pub struct PyMultiVectorConfig(MultiVectorConfig);

#[pymethods]
impl PyMultiVectorConfig {
    #[new]
    pub fn new(comparator: PyMultiVectorComparator) -> Self {
        Self(MultiVectorConfig {
            comparator: MultiVectorComparator::from(comparator),
        })
    }

    #[getter]
    pub fn comparator(&self) -> PyMultiVectorComparator {
        PyMultiVectorComparator::from(self.0.comparator)
    }
}

#[pyclass(name = "MultiVectorComparator")]
#[derive(Copy, Clone, Debug)]
pub enum PyMultiVectorComparator {
    MaxSim,
}

impl From<MultiVectorComparator> for PyMultiVectorComparator {
    fn from(comparator: MultiVectorComparator) -> Self {
        match comparator {
            MultiVectorComparator::MaxSim => PyMultiVectorComparator::MaxSim,
        }
    }
}

impl From<PyMultiVectorComparator> for MultiVectorComparator {
    fn from(comparator: PyMultiVectorComparator) -> Self {
        match comparator {
            PyMultiVectorComparator::MaxSim => MultiVectorComparator::MaxSim,
        }
    }
}

#[pyclass(name = "VectorStorageDatatype")]
#[derive(Copy, Clone, Debug)]
pub enum PyVectorStorageDatatype {
    Float32,
    Float16,
    Uint8,
}

impl From<VectorStorageDatatype> for PyVectorStorageDatatype {
    fn from(datatype: VectorStorageDatatype) -> Self {
        match datatype {
            VectorStorageDatatype::Float32 => PyVectorStorageDatatype::Float32,
            VectorStorageDatatype::Float16 => PyVectorStorageDatatype::Float16,
            VectorStorageDatatype::Uint8 => PyVectorStorageDatatype::Uint8,
        }
    }
}

impl From<PyVectorStorageDatatype> for VectorStorageDatatype {
    fn from(datatype: PyVectorStorageDatatype) -> Self {
        match datatype {
            PyVectorStorageDatatype::Float32 => VectorStorageDatatype::Float32,
            PyVectorStorageDatatype::Float16 => VectorStorageDatatype::Float16,
            PyVectorStorageDatatype::Uint8 => VectorStorageDatatype::Uint8,
        }
    }
}

#[pyclass(name = "SparseVectorDataConfig")]
#[derive(Copy, Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PySparseVectorDataConfig(SparseVectorDataConfig);

impl PySparseVectorDataConfig {
    fn peel_map(map: HashMap<String, Self>) -> HashMap<String, SparseVectorDataConfig>
    where
        Self: TransparentWrapper<SparseVectorDataConfig>,
    {
        unsafe { mem::transmute(map) }
    }

    fn wrap_map_ref(map: &HashMap<String, SparseVectorDataConfig>) -> &HashMap<String, Self>
    where
        Self: TransparentWrapper<SparseVectorDataConfig>,
    {
        unsafe { mem::transmute(map) }
    }
}

#[pymethods]
impl PySparseVectorDataConfig {
    // TODO
}

impl<'py> IntoPyObject<'py> for &PySparseVectorDataConfig {
    type Target = PySparseVectorDataConfig;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(*self, py)
    }
}

#[pyclass(name = "PayloadStorageType")]
#[derive(Copy, Clone, Debug)]
pub enum PyPayloadStorageType {
    Mmap,
    InRamMmap,
}

impl From<PayloadStorageType> for PyPayloadStorageType {
    fn from(storage_type: PayloadStorageType) -> Self {
        match storage_type {
            PayloadStorageType::Mmap => PyPayloadStorageType::Mmap,
            PayloadStorageType::InRamMmap => PyPayloadStorageType::InRamMmap,
        }
    }
}

impl From<PyPayloadStorageType> for PayloadStorageType {
    fn from(storage_type: PyPayloadStorageType) -> Self {
        match storage_type {
            PyPayloadStorageType::Mmap => PayloadStorageType::Mmap,
            PyPayloadStorageType::InRamMmap => PayloadStorageType::InRamMmap,
        }
    }
}
