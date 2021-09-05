# Quick Start with Qdrant using GRPC

DISCLAIMER: The GRPC API for Qdrant are under development and disabled by default in production builds. 
Limited functionality is exposed at the moment. This draft tutorial is for testing and internal use only.

## Install grpcurl
This tutorial would use the grpcurl command line tool in order to perform grpc requests. Please follow the
steps provided in the [grpcurl repository](https://github.com/fullstorydev/grpcurl) in order to install it.

## Build Qdrant with GRPC feature
The GRPC feature is disabled by default in order not to impact the production binary until complete.
In order to run qdrant with grpc feature enabled, executed the following command:
```bash
cargo run --features=grpc --bin qdrant
```
It will run qdrant exposing both json and grpc API. If you do not want to use JSON API, add ``--no-default-features ``
flag as well:
```bash
cargo run --features=grpc --no-default-features  --bin qdrant
```
Note that actix is not compiled in this case.

## GRPC Service Health Check
Execute the following command
```bash
grpcurl -plaintext -import-path ./src/tonic/proto -proto qdrant.proto -d '{}' [::]:6334 qdrant.Qdrant/HealthCheck
```
Here and below the ```./src/tonic/proto``` should be a path to the folder with a probuf schemas.
Expected response:
```json
{
  "title": "qdrant - vector search engine",
  "version": "<vesion>"
}
```

## Create collection
First - let's create a collection with dot-production metric.
```bash
grpcurl -plaintext -import-path ./src/tonic/proto -proto qdrant.proto -d '{
        "create_collection": {
            "name": "test_collection",
            "vector_size": 4,
            "distance": "Dot"
        }
    }' \
[::]:6334 qdrant.Collections/UpdateCollections
```

Expected response:
```json
{
  "result": true,
  "time": 0.482865481
}
```