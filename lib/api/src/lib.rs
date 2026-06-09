pub mod conversions;
pub mod grpc;
pub mod rest;

pub const HTTP_HEADER_API_KEY: &str = "api-key";

/// HTTP header / gRPC metadata key carrying the optional deterministic read
/// routing token. Requests sharing the same value are consistently routed to the
/// same shard replicas, avoiding "blinking" results caused by deferred updates.
pub const HTTP_HEADER_ROUTING_TOKEN: &str = "X-Qdrant-Routing-Token";
