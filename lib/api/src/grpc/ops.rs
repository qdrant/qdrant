use std::collections::HashMap;

use crate::grpc::{HardwareUsage, InferenceUsage, ModelUsage, Usage};

/// Sum of two optional counters. `None` means "not measured", so it only
/// survives when both sides are `None`.
fn add_opt(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (None, None) => None,
        (a, b) => Some(a.unwrap_or(0) + b.unwrap_or(0)),
    }
}

fn is_zero_opt(value: Option<u64>) -> bool {
    value.is_none_or(|v| v == 0)
}

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
            remote_io_cached_read,
            remote_io_non_cached_read,
        } = other;

        self.cpu = add_opt(self.cpu, cpu);
        self.payload_io_read = add_opt(self.payload_io_read, payload_io_read);
        self.payload_io_write = add_opt(self.payload_io_write, payload_io_write);
        self.payload_index_io_read = add_opt(self.payload_index_io_read, payload_index_io_read);
        self.payload_index_io_write = add_opt(self.payload_index_io_write, payload_index_io_write);
        self.vector_io_read = add_opt(self.vector_io_read, vector_io_read);
        self.vector_io_write = add_opt(self.vector_io_write, vector_io_write);
        self.remote_io_cached_read = add_opt(self.remote_io_cached_read, remote_io_cached_read);
        self.remote_io_non_cached_read =
            add_opt(self.remote_io_non_cached_read, remote_io_non_cached_read);
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
            remote_io_cached_read,
            remote_io_non_cached_read,
        } = *self;

        is_zero_opt(cpu)
            && is_zero_opt(payload_io_read)
            && is_zero_opt(payload_io_write)
            && is_zero_opt(payload_index_io_read)
            && is_zero_opt(payload_index_io_write)
            && is_zero_opt(vector_io_read)
            && is_zero_opt(vector_io_write)
            && is_zero_opt(remote_io_cached_read)
            && is_zero_opt(remote_io_non_cached_read)
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
