// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

use std::fmt;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::IntoPyObjectExt as _;
use pyo3::inspect::PyStaticExpr;
use pyo3::prelude::*;
use segment::types::*;

use crate::repr::*;
use crate::type_hint::Alias;

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyQuantizationConfig(pub QuantizationConfig);

pub const QUANTIZATION_CONFIG: Alias = Alias {
    name: "QuantizationConfigType",
    definition: QuantizationConfigHelper::INPUT_TYPE,
};

#[derive(FromPyObject, IntoPyObject)]
enum QuantizationConfigHelper {
    Scalar(PyScalarQuantizationConfig),
    Product(PyProductQuantizationConfig),
    Binary(PyBinaryQuantizationConfig),
    Turbo(PyTurboQuantQuantizationConfig),
}

impl FromPyObject<'_, '_> for PyQuantizationConfig {
    type Error = PyErr;
    const INPUT_TYPE: PyStaticExpr = QUANTIZATION_CONFIG.hint();

    fn extract(conf: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let conf = match conf.extract()? {
            QuantizationConfigHelper::Scalar(scalar) => {
                QuantizationConfig::Scalar(ScalarQuantization {
                    scalar: ScalarQuantizationConfig::from(scalar),
                })
            }
            QuantizationConfigHelper::Product(product) => {
                QuantizationConfig::Product(ProductQuantization {
                    product: ProductQuantizationConfig::from(product),
                })
            }
            QuantizationConfigHelper::Binary(binary) => {
                QuantizationConfig::Binary(BinaryQuantization {
                    binary: BinaryQuantizationConfig::from(binary),
                })
            }
            QuantizationConfigHelper::Turbo(turbo) => {
                QuantizationConfig::Turbo(TurboQuantization {
                    turbo: TurboQuantQuantizationConfig::from(turbo),
                })
            }
        };

        Ok(Self(conf))
    }
}

impl<'py> IntoPyObject<'py> for PyQuantizationConfig {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    const OUTPUT_TYPE: PyStaticExpr = QUANTIZATION_CONFIG.hint();

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match self.0 {
            QuantizationConfig::Scalar(ScalarQuantization { scalar }) => {
                QuantizationConfigHelper::Scalar(PyScalarQuantizationConfig(scalar))
            }
            QuantizationConfig::Product(ProductQuantization { product }) => {
                QuantizationConfigHelper::Product(PyProductQuantizationConfig(product))
            }
            QuantizationConfig::Binary(BinaryQuantization { binary }) => {
                QuantizationConfigHelper::Binary(PyBinaryQuantizationConfig(binary))
            }
            QuantizationConfig::Turbo(TurboQuantization { turbo }) => {
                QuantizationConfigHelper::Turbo(PyTurboQuantQuantizationConfig(turbo))
            }
        }
        .into_bound_py_any(py)
    }
}

impl Repr for PyQuantizationConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.0 {
            QuantizationConfig::Scalar(scalar) => {
                PyScalarQuantizationConfig::wrap_ref(&scalar.scalar).fmt(f)
            }
            QuantizationConfig::Product(product) => {
                PyProductQuantizationConfig::wrap_ref(&product.product).fmt(f)
            }
            QuantizationConfig::Binary(binary) => {
                PyBinaryQuantizationConfig::wrap_ref(&binary.binary).fmt(f)
            }
            QuantizationConfig::Turbo(turbo) => {
                PyTurboQuantQuantizationConfig::wrap_ref(&turbo.turbo).fmt(f)
            }
        }
    }
}

/// Configuration for scalar quantization.
///
/// Create a ScalarQuantizationConfig.
///
/// Args:
///     type: Scalar type (e.g., Int8).
///     quantile: Quantile for normalization.
///     always_ram: Whether to keep in RAM.
#[pyclass(name = "ScalarQuantizationConfig", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyScalarQuantizationConfig(ScalarQuantizationConfig);

#[pyclass_repr]
#[pymethods]
impl PyScalarQuantizationConfig {
    #[new]
    #[pyo3(signature = (r#type, quantile = None, always_ram = None))]
    pub fn new(r#type: PyScalarType, quantile: Option<f32>, always_ram: Option<bool>) -> Self {
        Self(ScalarQuantizationConfig {
            r#type: ScalarType::from(r#type),
            quantile,
            always_ram,
            memory: None,
        })
    }

    /// Scalar type.
    #[getter]
    pub fn r#type(&self) -> PyScalarType {
        PyScalarType::from(self.0.r#type)
    }

    /// Quantile.
    #[getter]
    pub fn quantile(&self) -> Option<f32> {
        self.0.quantile
    }

    /// Always RAM flag.
    #[getter]
    pub fn always_ram(&self) -> Option<bool> {
        self.0.always_ram
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyScalarQuantizationConfig {
    fn _getters(self) {
        // Every field should have a getter method
        let ScalarQuantizationConfig {
            r#type: _,
            quantile: _,
            always_ram: _,
            memory: _,
        } = self.0;
    }
}

/// Scalar quantization types.
#[pyclass(name = "ScalarType", from_py_object)]
#[derive(Copy, Clone, Debug)]
pub enum PyScalarType {
    Int8,
}

impl Repr for PyScalarType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::Int8 => "Int8",
        };

        f.simple_enum::<Self>(repr)
    }
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

/// Configuration for product quantization.
///
/// Create a ProductQuantizationConfig.
///
/// Args:
///     compression: Compression ratio.
///     always_ram: Whether to keep in RAM.
#[pyclass(name = "ProductQuantizationConfig", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyProductQuantizationConfig(ProductQuantizationConfig);

#[pyclass_repr]
#[pymethods]
impl PyProductQuantizationConfig {
    #[new]
    #[pyo3(signature = (compression, always_ram = None))]
    pub fn new(compression: PyCompressionRatio, always_ram: Option<bool>) -> Self {
        Self(ProductQuantizationConfig {
            compression: CompressionRatio::from(compression),
            always_ram,
            memory: None,
        })
    }

    /// Compression ratio.
    #[getter]
    pub fn compression(&self) -> PyCompressionRatio {
        PyCompressionRatio::from(self.0.compression)
    }

    /// Always RAM flag.
    #[getter]
    pub fn always_ram(&self) -> Option<bool> {
        self.0.always_ram
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyProductQuantizationConfig {
    fn _getters(self) {
        // Every field should have a getter method
        let ProductQuantizationConfig {
            compression: _,
            always_ram: _,
            memory: _,
        } = self.0;
    }
}

/// Product quantization compression ratios.
#[pyclass(name = "CompressionRatio", from_py_object)]
#[derive(Copy, Clone, Debug)]
pub enum PyCompressionRatio {
    X4,
    X8,
    X16,
    X32,
    X64,
}

impl Repr for PyCompressionRatio {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::X4 => "X4",
            Self::X8 => "X8",
            Self::X16 => "X16",
            Self::X32 => "X32",
            Self::X64 => "X64",
        };

        f.simple_enum::<Self>(repr)
    }
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

/// Configuration for binary quantization.
///
/// Create a BinaryQuantizationConfig.
///
/// Args:
///     always_ram: Whether to keep in RAM.
///     encoding: Binary encoding type.
///     query_encoding: Query encoding type.
#[pyclass(name = "BinaryQuantizationConfig", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyBinaryQuantizationConfig(BinaryQuantizationConfig);

#[pyclass_repr]
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
            memory: None,
            encoding: encoding.map(BinaryQuantizationEncoding::from),
            query_encoding: query_encoding.map(BinaryQuantizationQueryEncoding::from),
        })
    }

    /// Always RAM flag.
    #[getter]
    pub fn always_ram(&self) -> Option<bool> {
        self.0.always_ram
    }

    /// Encoding.
    #[getter]
    pub fn encoding(&self) -> Option<PyBinaryQuantizationEncoding> {
        self.0.encoding.map(PyBinaryQuantizationEncoding::from)
    }

    /// Query encoding.
    #[getter]
    pub fn query_encoding(&self) -> Option<PyBinaryQuantizationQueryEncoding> {
        self.0
            .query_encoding
            .map(PyBinaryQuantizationQueryEncoding::from)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyBinaryQuantizationConfig {
    fn _getters(self) {
        // Every field should have a getter method
        let BinaryQuantizationConfig {
            always_ram: _,
            memory: _,
            encoding: _,
            query_encoding: _,
        } = self.0;
    }
}

/// Binary quantization encoding types.
#[pyclass(name = "BinaryQuantizationEncoding", from_py_object)]
#[derive(Copy, Clone, Debug)]
pub enum PyBinaryQuantizationEncoding {
    OneBit,
    TwoBits,
    OneAndHalfBits,
}

impl Repr for PyBinaryQuantizationEncoding {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::OneBit => "OneBit",
            Self::TwoBits => "TwoBits",
            Self::OneAndHalfBits => "OneAndHalfBits",
        };

        f.simple_enum::<Self>(repr)
    }
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

/// Binary quantization query encoding types.
#[pyclass(name = "BinaryQuantizationQueryEncoding", from_py_object)]
#[derive(Copy, Clone, Debug)]
pub enum PyBinaryQuantizationQueryEncoding {
    Default,
    Binary,
    Scalar4Bits,
    Scalar8Bits,
}

impl Repr for PyBinaryQuantizationQueryEncoding {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::Default => "Default",
            Self::Binary => "Binary",
            Self::Scalar4Bits => "Scalar4Bits",
            Self::Scalar8Bits => "Scalar8Bits",
        };

        f.simple_enum::<Self>(repr)
    }
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

/// Configuration for TurboQuant quantization.
///
/// Create a TurboQuantQuantizationConfig.
///
/// Args:
///     always_ram: Whether to keep in RAM.
///     plus: Enable the TurboQuant+ variant.
///     bits: Bit size used for compressed codes.
#[pyclass(name = "TurboQuantQuantizationConfig", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyTurboQuantQuantizationConfig(TurboQuantQuantizationConfig);

#[pyclass_repr]
#[pymethods]
impl PyTurboQuantQuantizationConfig {
    #[new]
    #[pyo3(signature = (always_ram = None, bits = None))]
    pub fn new(always_ram: Option<bool>, bits: Option<PyTurboQuantBitSize>) -> Self {
        Self(TurboQuantQuantizationConfig {
            always_ram,
            memory: None,
            bits: bits.map(TurboQuantBitSize::from),
        })
    }

    /// Always RAM flag.
    #[getter]
    pub fn always_ram(&self) -> Option<bool> {
        self.0.always_ram
    }

    /// Bit size.
    #[getter]
    pub fn bits(&self) -> Option<PyTurboQuantBitSize> {
        self.0.bits.map(PyTurboQuantBitSize::from)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyTurboQuantQuantizationConfig {
    fn _getters(self) {
        // Every field should have a getter method
        let TurboQuantQuantizationConfig {
            always_ram: _,
            memory: _,
            bits: _,
        } = self.0;
    }
}

/// TurboQuant bit size for compressed codes.
#[pyclass(name = "TurboQuantBitSize", from_py_object)]
#[derive(Copy, Clone, Debug)]
pub enum PyTurboQuantBitSize {
    Bits1,
    Bits1_5,
    Bits2,
    Bits4,
}

impl Repr for PyTurboQuantBitSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::Bits1 => "Bits1",
            Self::Bits1_5 => "Bits1_5",
            Self::Bits2 => "Bits2",
            Self::Bits4 => "Bits4",
        };

        f.simple_enum::<Self>(repr)
    }
}

impl From<TurboQuantBitSize> for PyTurboQuantBitSize {
    fn from(bits: TurboQuantBitSize) -> Self {
        match bits {
            TurboQuantBitSize::Bits1 => PyTurboQuantBitSize::Bits1,
            TurboQuantBitSize::Bits1_5 => PyTurboQuantBitSize::Bits1_5,
            TurboQuantBitSize::Bits2 => PyTurboQuantBitSize::Bits2,
            TurboQuantBitSize::Bits4 => PyTurboQuantBitSize::Bits4,
        }
    }
}

impl From<PyTurboQuantBitSize> for TurboQuantBitSize {
    fn from(bits: PyTurboQuantBitSize) -> Self {
        match bits {
            PyTurboQuantBitSize::Bits1 => TurboQuantBitSize::Bits1,
            PyTurboQuantBitSize::Bits1_5 => TurboQuantBitSize::Bits1_5,
            PyTurboQuantBitSize::Bits2 => TurboQuantBitSize::Bits2,
            PyTurboQuantBitSize::Bits4 => TurboQuantBitSize::Bits4,
        }
    }
}
