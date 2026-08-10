//! `--quantization` presets, mapped onto [`QuantizationConfig`].
#![allow(
    deprecated,
    reason = "always_ram is deprecated but still constructible"
)]

use clap::ValueEnum;
use edge::{
    BinaryQuantizationConfig, CompressionRatio, ProductQuantizationConfig, QuantizationConfig,
    ScalarQuantizationConfig, ScalarType,
};
use segment::types::{TurboQuantBitSize, TurboQuantQuantizationConfig, TurboQuantization};

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum QuantizationPreset {
    #[value(name = "scalar")]
    Scalar,
    #[value(name = "binary")]
    Binary,
    #[value(name = "product-x4")]
    ProductX4,
    #[value(name = "product-x8")]
    ProductX8,
    #[value(name = "product-x16")]
    ProductX16,
    #[value(name = "product-x32")]
    ProductX32,
    #[value(name = "product-x64")]
    ProductX64,
    #[value(name = "turbo1")]
    Turbo1,
    #[value(name = "turbo1.5")]
    Turbo1_5,
    #[value(name = "turbo2")]
    Turbo2,
    #[value(name = "turbo4")]
    Turbo4,
}

impl QuantizationPreset {
    pub fn to_config(self) -> QuantizationConfig {
        let product = |compression: CompressionRatio| -> QuantizationConfig {
            ProductQuantizationConfig {
                compression,
                always_ram: None,
                memory: None,
            }
            .into()
        };
        let turbo = |bits: TurboQuantBitSize| -> QuantizationConfig {
            QuantizationConfig::Turbo(TurboQuantization {
                turbo: TurboQuantQuantizationConfig {
                    always_ram: None,
                    memory: None,
                    bits: Some(bits),
                },
            })
        };

        match self {
            Self::Scalar => ScalarQuantizationConfig {
                r#type: ScalarType::Int8,
                quantile: Some(0.99),
                always_ram: None,
                memory: None,
            }
            .into(),
            Self::Binary => BinaryQuantizationConfig {
                always_ram: None,
                memory: None,
                encoding: None,
                query_encoding: None,
            }
            .into(),
            Self::ProductX4 => product(CompressionRatio::X4),
            Self::ProductX8 => product(CompressionRatio::X8),
            Self::ProductX16 => product(CompressionRatio::X16),
            Self::ProductX32 => product(CompressionRatio::X32),
            Self::ProductX64 => product(CompressionRatio::X64),
            Self::Turbo1 => turbo(TurboQuantBitSize::Bits1),
            Self::Turbo1_5 => turbo(TurboQuantBitSize::Bits1_5),
            Self::Turbo2 => turbo(TurboQuantBitSize::Bits2),
            Self::Turbo4 => turbo(TurboQuantBitSize::Bits4),
        }
    }
}
