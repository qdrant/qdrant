#!/usr/bin/env python3
"""
Script to apply per-collection metrics changes to src/common/metrics.rs

This script adds the per-collection metrics export logic to the MetricsProvider
implementations for WebApiTelemetry and GrpcTelemetry.

Usage:
    python3 apply_metrics_changes.py
"""

import re

def apply_changes():
    file_path = "src/common/metrics.rs"
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Find the WebApiTelemetry implementation
    web_api_pattern = r'(impl MetricsProvider for WebApiTelemetry \{.*?builder\.build\(prefix, "rest", metrics\);)'
    
    web_api_addition = r'''\1
        
        // Add per-collection metrics
        let mut collection_builder = OperationDurationMetricsBuilder::default();
        for (collection, methods) in &self.responses_by_collection {
            for (endpoint, responses) in methods {
                let Some((method, endpoint)) = endpoint.split_once(' ') else {
                    continue;
                };
                // Endpoint must be whitelisted
                if REST_ENDPOINT_WHITELIST.binary_search(&endpoint).is_err() {
                    continue;
                }
                for (status, stats) in responses {
                    collection_builder.add(
                        stats,
                        &[
                            ("collection", collection.as_str()),
                            ("method", method),
                            ("endpoint", endpoint),
                            ("status", &status.to_string()),
                        ],
                        *status == REST_TIMINGS_FOR_STATUS,
                    );
                }
            }
        }
        collection_builder.build(prefix, "rest_collection", metrics);'''
    
    content = re.sub(web_api_pattern, web_api_addition, content, flags=re.DOTALL)
    
    # Find the GrpcTelemetry implementation
    grpc_pattern = r'(impl MetricsProvider for GrpcTelemetry \{.*?builder\.build\(prefix, "grpc", metrics\);)'
    
    grpc_addition = r'''\1
        
        // Add per-collection metrics
        let mut collection_builder = OperationDurationMetricsBuilder::default();
        for (collection, methods) in &self.responses_by_collection {
            for (endpoint, stats) in methods {
                // Endpoint must be whitelisted
                if GRPC_ENDPOINT_WHITELIST
                    .binary_search(&endpoint.as_str())
                    .is_err()
                {
                    continue;
                }
                collection_builder.add(
                    stats,
                    &[
                        ("collection", collection.as_str()),
                        ("endpoint", endpoint.as_str()),
                    ],
                    true,
                );
            }
        }
        collection_builder.build(prefix, "grpc_collection", metrics);'''
    
    content = re.sub(grpc_pattern, grpc_addition, content, flags=re.DOTALL)
    
    with open(file_path, 'w') as f:
        f.write(content)
    
    print(f"✅ Successfully updated {file_path}")
    print("Per-collection metrics have been added to WebApiTelemetry and GrpcTelemetry")

if __name__ == "__main__":
    try:
        apply_changes()
    except Exception as e:
        print(f"❌ Error: {e}")
        exit(1)
