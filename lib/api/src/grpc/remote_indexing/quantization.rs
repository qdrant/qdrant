//! Conversions of the quantization of a task: the quantization config and the encoder
//! parameters the indexer reads the quantized codes with.

use quantization::encoded_vectors_binary::{BinaryParameters, Encoding, QueryEncoding};
use quantization::encoded_vectors_tq::{ErrorCorrectionMetadata, TurboParameters};
use quantization::encoded_vectors_u8::ScalarParameters;
use quantization::turboquant::{TQBits, TQMode, TQRotation};
use quantization::vector_stats::{VectorElementStats, VectorStats};
use segment::index::hnsw_index::remote::wire::{QuantizerParameters, RemoteQuantization};

use crate::grpc::qdrant::{
    IndexingBinaryEncoding, IndexingBinaryParameters, IndexingBinaryQueryEncoding,
    IndexingQuantization, IndexingScalarParameters, IndexingTurboBits,
    IndexingTurboErrorCorrection, IndexingTurboMode, IndexingTurboParameters,
    IndexingTurboRotation, IndexingVectorElementStats, indexing_quantization,
};

pub fn quantization_to_grpc(quantization: &RemoteQuantization) -> IndexingQuantization {
    let RemoteQuantization { config, parameters } = quantization;
    let parameters = match parameters {
        QuantizerParameters::Scalar(parameters) => {
            indexing_quantization::Parameters::Scalar(scalar_to_grpc(*parameters))
        }
        QuantizerParameters::Binary(parameters) => {
            indexing_quantization::Parameters::Binary(binary_to_grpc(parameters))
        }
        QuantizerParameters::Turbo(parameters) => {
            indexing_quantization::Parameters::Turbo(turbo_to_grpc(parameters))
        }
    };
    IndexingQuantization {
        config: Some(crate::grpc::qdrant::QuantizationConfig::from(
            config.clone(),
        )),
        parameters: Some(parameters),
    }
}

pub fn quantization_from_grpc(
    quantization: IndexingQuantization,
) -> Result<RemoteQuantization, String> {
    let IndexingQuantization { config, parameters } = quantization;
    let config = config.ok_or_else(|| "quantization without config".to_string())?;
    let config = segment::types::QuantizationConfig::try_from(config)
        .map_err(|status| format!("quantization config: {}", status.message()))?;
    let parameters = match parameters {
        Some(indexing_quantization::Parameters::Scalar(parameters)) => {
            QuantizerParameters::Scalar(scalar_from_grpc(parameters))
        }
        Some(indexing_quantization::Parameters::Binary(parameters)) => {
            QuantizerParameters::Binary(binary_from_grpc(parameters)?)
        }
        Some(indexing_quantization::Parameters::Turbo(parameters)) => {
            QuantizerParameters::Turbo(turbo_from_grpc(parameters)?)
        }
        None => return Err("quantization without encoder parameters".to_string()),
    };
    Ok(RemoteQuantization { config, parameters })
}

fn scalar_to_grpc(parameters: ScalarParameters) -> IndexingScalarParameters {
    let ScalarParameters {
        actual_dim,
        alpha,
        offset,
        multiplier,
    } = parameters;
    IndexingScalarParameters {
        actual_dim: actual_dim as u64,
        alpha,
        offset,
        multiplier,
    }
}

fn scalar_from_grpc(parameters: IndexingScalarParameters) -> ScalarParameters {
    let IndexingScalarParameters {
        actual_dim,
        alpha,
        offset,
        multiplier,
    } = parameters;
    ScalarParameters {
        actual_dim: actual_dim as usize,
        alpha,
        offset,
        multiplier,
    }
}

fn binary_to_grpc(parameters: &BinaryParameters) -> IndexingBinaryParameters {
    let BinaryParameters {
        encoding,
        query_encoding,
        vector_stats,
    } = parameters;
    let encoding = match encoding {
        Encoding::OneBit => IndexingBinaryEncoding::IndexingBinaryOneBit,
        Encoding::TwoBits => IndexingBinaryEncoding::IndexingBinaryTwoBits,
        Encoding::OneAndHalfBits => IndexingBinaryEncoding::IndexingBinaryOneAndHalfBits,
    };
    let query_encoding = match query_encoding {
        QueryEncoding::SameAsStorage => {
            IndexingBinaryQueryEncoding::IndexingBinaryQuerySameAsStorage
        }
        QueryEncoding::Scalar4bits => IndexingBinaryQueryEncoding::IndexingBinaryQueryScalar4Bits,
        QueryEncoding::Scalar8bits => IndexingBinaryQueryEncoding::IndexingBinaryQueryScalar8Bits,
    };
    IndexingBinaryParameters {
        encoding: i32::from(encoding),
        query_encoding: i32::from(query_encoding),
        vector_stats: vector_stats
            .iter()
            .flat_map(|stats| &stats.elements_stats)
            .map(|stats| {
                let VectorElementStats {
                    min,
                    max,
                    mean,
                    stddev,
                } = *stats;
                IndexingVectorElementStats {
                    min,
                    max,
                    mean,
                    stddev,
                }
            })
            .collect(),
    }
}

fn binary_from_grpc(parameters: IndexingBinaryParameters) -> Result<BinaryParameters, String> {
    let IndexingBinaryParameters {
        encoding,
        query_encoding,
        vector_stats,
    } = parameters;
    let encoding = match IndexingBinaryEncoding::try_from(encoding) {
        Ok(IndexingBinaryEncoding::IndexingBinaryOneBit) => Encoding::OneBit,
        Ok(IndexingBinaryEncoding::IndexingBinaryTwoBits) => Encoding::TwoBits,
        Ok(IndexingBinaryEncoding::IndexingBinaryOneAndHalfBits) => Encoding::OneAndHalfBits,
        Ok(IndexingBinaryEncoding::Unknown) => return Err("no binary encoding".to_string()),
        Err(_) => return Err(format!("unknown binary encoding {encoding}")),
    };
    let query_encoding = match IndexingBinaryQueryEncoding::try_from(query_encoding) {
        Ok(IndexingBinaryQueryEncoding::IndexingBinaryQuerySameAsStorage) => {
            QueryEncoding::SameAsStorage
        }
        Ok(IndexingBinaryQueryEncoding::IndexingBinaryQueryScalar4Bits) => {
            QueryEncoding::Scalar4bits
        }
        Ok(IndexingBinaryQueryEncoding::IndexingBinaryQueryScalar8Bits) => {
            QueryEncoding::Scalar8bits
        }
        Ok(IndexingBinaryQueryEncoding::Unknown) => {
            return Err("no binary query encoding".to_string());
        }
        Err(_) => return Err(format!("unknown binary query encoding {query_encoding}")),
    };
    let vector_stats = (!vector_stats.is_empty()).then(|| VectorStats {
        elements_stats: vector_stats
            .into_iter()
            .map(|stats| {
                let IndexingVectorElementStats {
                    min,
                    max,
                    mean,
                    stddev,
                } = stats;
                VectorElementStats {
                    min,
                    max,
                    mean,
                    stddev,
                }
            })
            .collect(),
    });
    Ok(BinaryParameters {
        encoding,
        query_encoding,
        vector_stats,
    })
}

fn turbo_to_grpc(parameters: &TurboParameters) -> IndexingTurboParameters {
    let TurboParameters {
        bits,
        mode,
        rotation,
        error_correction,
    } = parameters;
    let bits = match bits {
        TQBits::Bits8 => IndexingTurboBits::IndexingTurbo8Bits,
        TQBits::Bits16 => IndexingTurboBits::IndexingTurbo16Bits,
        TQBits::Bits4 => IndexingTurboBits::IndexingTurbo4Bits,
        TQBits::Bits2 => IndexingTurboBits::IndexingTurbo2Bits,
        TQBits::Bits1_5 => IndexingTurboBits::IndexingTurboOneAndHalfBits,
        TQBits::Bits1 => IndexingTurboBits::IndexingTurbo1Bit,
    };
    let mode = match mode {
        TQMode::Normal => IndexingTurboMode::IndexingTurboNormal,
        TQMode::Plus => IndexingTurboMode::IndexingTurboPlus,
    };
    let rotation = match rotation {
        TQRotation::Padded => IndexingTurboRotation::IndexingTurboPadded,
        TQRotation::Unpadded => IndexingTurboRotation::IndexingTurboUnpadded,
    };
    IndexingTurboParameters {
        bits: i32::from(bits),
        mode: i32::from(mode),
        rotation: i32::from(rotation),
        error_correction: error_correction.as_ref().map(|correction| {
            let ErrorCorrectionMetadata { shift, scale } = correction;
            IndexingTurboErrorCorrection {
                shift: shift.clone(),
                scale: scale.clone(),
            }
        }),
    }
}

fn turbo_from_grpc(parameters: IndexingTurboParameters) -> Result<TurboParameters, String> {
    let IndexingTurboParameters {
        bits,
        mode,
        rotation,
        error_correction,
    } = parameters;
    let bits = match IndexingTurboBits::try_from(bits) {
        Ok(IndexingTurboBits::IndexingTurbo8Bits) => TQBits::Bits8,
        Ok(IndexingTurboBits::IndexingTurbo16Bits) => TQBits::Bits16,
        Ok(IndexingTurboBits::IndexingTurbo4Bits) => TQBits::Bits4,
        Ok(IndexingTurboBits::IndexingTurbo2Bits) => TQBits::Bits2,
        Ok(IndexingTurboBits::IndexingTurboOneAndHalfBits) => TQBits::Bits1_5,
        Ok(IndexingTurboBits::IndexingTurbo1Bit) => TQBits::Bits1,
        Ok(IndexingTurboBits::Unknown) => return Err("no TurboQuant bits".to_string()),
        Err(_) => return Err(format!("unknown TurboQuant bits {bits}")),
    };
    let mode = match IndexingTurboMode::try_from(mode) {
        Ok(IndexingTurboMode::IndexingTurboNormal) => TQMode::Normal,
        Ok(IndexingTurboMode::IndexingTurboPlus) => TQMode::Plus,
        Ok(IndexingTurboMode::Unknown) => return Err("no TurboQuant mode".to_string()),
        Err(_) => return Err(format!("unknown TurboQuant mode {mode}")),
    };
    let rotation = match IndexingTurboRotation::try_from(rotation) {
        Ok(IndexingTurboRotation::IndexingTurboPadded) => TQRotation::Padded,
        Ok(IndexingTurboRotation::IndexingTurboUnpadded) => TQRotation::Unpadded,
        Ok(IndexingTurboRotation::Unknown) => return Err("no TurboQuant rotation".to_string()),
        Err(_) => return Err(format!("unknown TurboQuant rotation {rotation}")),
    };
    Ok(TurboParameters {
        bits,
        mode,
        rotation,
        error_correction: error_correction.map(|correction| {
            let IndexingTurboErrorCorrection { shift, scale } = correction;
            ErrorCorrectionMetadata { shift, scale }
        }),
    })
}
