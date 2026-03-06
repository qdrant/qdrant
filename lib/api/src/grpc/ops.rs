use std::collections::HashMap;

use crate::grpc::{HardwareUsage, InferenceUsage, ModelUsage, Usage};

impl HardwareUsage {
    pub fn add(&mut self, other: Self) {
        let Self {
            cpu,
            payload_io_read,
            payload_io_write,
            payload_index_io_read,
            payload_index_io_write,
            vector_io_read,
            vector_io_write,
        } = other;

        self.cpu += cpu;
        self.payload_io_read += payload_io_read;
        self.payload_io_write += payload_io_write;
        self.payload_index_io_read += payload_index_io_read;
        self.payload_index_io_write += payload_index_io_write;
        self.vector_io_read += vector_io_read;
        self.vector_io_write += vector_io_write;
    }

    pub fn is_empty(&self) -> bool {
        let Self {
            cpu,
            payload_io_read,
            payload_io_write,
            payload_index_io_read,
            payload_index_io_write,
            vector_io_read,
            vector_io_write,
        } = self;

        *cpu == 0
            && *payload_io_read == 0
            && *payload_io_write == 0
            && *payload_index_io_read == 0
            && *payload_index_io_write == 0
            && *vector_io_read == 0
            && *vector_io_write == 0
    }

    pub fn into_non_empty(self) -> Option<Self> {
        (!self.is_empty()).then_some(self)
    }
}

impl Usage {
    pub fn new(hardware: Option<HardwareUsage>, inference: Option<InferenceUsage>) -> Self {
        Self {
            hardware,
            inference,
        }
    }

    pub fn is_empty(&self) -> bool {
        let Usage {
            hardware,
            inference,
        } = self;

        let hardware_empty = hardware.as_ref().is_none_or(|h| h.is_empty());
        let inference_empty = inference.as_ref().is_none_or(|i| i.is_empty());

        hardware_empty && inference_empty
    }

    pub fn into_non_empty(self) -> Option<Self> {
        (!self.is_empty()).then_some(self)
    }

    pub fn from_hardware_usage(hardware: Option<HardwareUsage>) -> Self {
        Usage {
            hardware,
            inference: None,
        }
    }

    pub fn from_inference_usage(inference: Option<InferenceUsage>) -> Self {
        Usage {
            hardware: None,
            inference,
        }
    }
}

impl InferenceUsage {
    pub fn new() -> Self {
        Self {
            models: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.models.is_empty()
    }

    pub fn into_non_empty(self) -> Option<Self> {
        (!self.is_empty()).then_some(self)
    }

    pub fn merge(&mut self, other: Self) {
        for (model_name, model_usage) in other.models {
            self.models
                .entry(model_name)
                .and_modify(|existing| {
                    let ModelUsage { tokens } = existing;
                    *tokens += model_usage.tokens;
                })
                .or_insert(model_usage);
        }
    }

    pub fn merge_opt(&mut self, other: Option<Self>) {
        if let Some(other) = other {
            self.merge(other);
        }
    }
}
