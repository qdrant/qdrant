use crate::grpc::HardwareUsage;

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
