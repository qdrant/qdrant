// `c:\Users\Admin\Desktop\ag\budget\qdrant\tests\grpc_metrics_test.rs`
use std::time::{Instant, Duration};
use std::sync::Arc;
use prost::Message;

// Uses mock structure definitions to link our logic up.
use qdrant::telemetry::GrpcTelemetry;
use qdrant::common::metrics::render_prometheus_grpc;
use qdrant::tonic::interceptor::ExtractCollectionName;

#[test]
fn test_metrics_cardinality_and_prometheus_export() {
    let telemetry = Arc::new(GrpcTelemetry::new(2, false));
    
    let path = "/qdrant.Points/Search";
    telemetry.record(path, "coll_A", Duration::from_millis(100));
    telemetry.record(path, "coll_A", Duration::from_millis(200));  // Avg will be 0.15s
    telemetry.record(path, "coll_B", Duration::from_millis(50));
    
    // The 3rd collection triggers the OOM capacity limit -> diverts to Overflow global metric
    telemetry.record(path, "coll_C", Duration::from_millis(500));
    telemetry.record(path, "coll_C", Duration::from_millis(500));

    let output = render_prometheus_grpc(&telemetry);
    println!("Prometheus Output:\n{}", output);

    // Validate Export String structure ensuring labels format and calculation works
    assert!(output.contains("qdrant_grpc_responses_duration_seconds{endpoint=\"/qdrant.Points/Search\", collection=\"coll_A\"} 0.150000"));
    assert!(output.contains("qdrant_grpc_responses_duration_seconds{endpoint=\"global\", collection=\"overflow\"} 0.500000"));
    assert!(!output.contains("coll_C"));
}

#[test]
fn bench_extraction_overhead() {
    let telemetry = Arc::new(GrpcTelemetry::new(100, false));
    
    // Create realistic gRPC payload containing `collection_name` equivalent to parsing `SearchPoints`
    let msg = ExtractCollectionName { collection_name: "high_perf_test".to_string() };
    let mut buf = Vec::new();
    
    // 5 byte prefix mimicking actual http framed gRPC streams
    buf.extend_from_slice(&[0, 0, 0, 0, 0]);
    msg.encode(&mut buf).unwrap();
    let protobuf_len = buf.len() - 5;
    buf[1..5].copy_from_slice(&(protobuf_len as u32).to_be_bytes());
    
    // Test the mock extraction decoding looping exactly like middleware handles latency overheads
    let iterations = 100_000;
    
    let start = Instant::now();
    for _ in 0..iterations {
        // Drop the 5 byte framing
        let grpc_msg_bytes = &buf[5..];
        
        // Execute the Prost parsing routine and save tracking
        if let Ok(decoded) = ExtractCollectionName::decode(grpc_msg_bytes) {
            telemetry.record("/qdrant.Points/Search", &decoded.collection_name, Duration::from_micros(1));
        }
    }
    
    let elapsed = start.elapsed();
    let time_per_req_ns = (elapsed.as_nanos() as f64) / (iterations as f64);
    
    println!("Prost Decode + Atomic Tracking Overhead: {:.2} ns / request", time_per_req_ns);
    
    // Enforce 10 microseconds max performance threshold validation
    assert!(time_per_req_ns < 10000.0, "Execution routing overhead crossed performance budget limits!");
}
