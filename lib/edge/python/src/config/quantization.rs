use derive_more::Into;
use pyo3::IntoPyObjectExt as _;
use pyo3::prelude::*;
use segment::types::*;

#[derive(Clone, Debug, Into)]
pub struct PyQuantizationConfig(pub QuantizationConfig);

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
