use crate::grpc::{HardwareUsage, Usage};

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
}

impl Usage {
    pub fn is_empty(&self) -> bool {
        let Usage { hardware } = self;

        hardware.is_none()
    }
}

pub fn usage_or_none(hardware: Option<HardwareUsage>) -> Option<Usage> {
    let usage = Usage { hardware };
    if usage.is_empty() { None } else { Some(usage) }
}
