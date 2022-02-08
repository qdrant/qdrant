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
cargo run --features=grpc --no-default-features --bin qdrant
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

## Collections

### Create collection
First - let's create a collection with dot-production metric.
```bash
grpcurl -plaintext -import-path ./src/tonic/proto -proto qdrant.proto -d '{
        "name": "test_collection",
        "vector_size": 4,
        "distance": "Dot"
    }' \
[::]:6334 qdrant.Collections/Create
```

Expected response:
```json
{
  "result": true,
  "time": 0.482865481
}
```

### List all collections
We can now view the list of collections to ensure that the collection was created:
```bash
grpcurl -plaintext -import-path ./src/tonic/proto -proto qdrant.proto [::]:6334 qdrant.Collections/List
```

Expected response:
```json
{
  "collections": [
    {
      "name": "test_collection"
    }
  ],
  "time": 9.4219e-05
}
```

### Update collection
The collection could also be updated:
```bash
grpcurl -plaintext -import-path ./src/tonic/proto -proto qdrant.proto -d '{
        "name": "test_collection",
        "optimizers_config": {
          "max_segment_number": 100
        }
    }' \
[::]:6334 qdrant.Collections/Update
```

### Delete collection
The qdrant.Collections/UpdateCollections rpc could also be used to delete a collection:
```bash
grpcurl -plaintext -import-path ./src/tonic/proto -proto qdrant.proto -d '{
          "name": "test_collection"
    }' \
[::]:6334 qdrant.Collections/Delete
```

## Add points
Let's now add vectors with some payload:

```bash
grpcurl -plaintext -import-path ./src/tonic/proto -proto qdrant.proto -d '{
  "collection": "test_collection",
  "wait": true,
  "points": [
    {
      "id": 1,
      "vector": [0.05, 0.61, 0.76, 0.74],
      "payload": {
        "city": { "keyword": { "value": ["Berlin"] }},
        "country": { "keyword": { "value": ["Germany"] }},
        "population": { "integer": { "value": [1000000] }},
        "square": { "float": { "value": [12.5] }},
        "coords": { "geo": { "value": [{ "lat": 1.0, "lon": 2.0 }]}}
      }
    },
    {"id": 2, "vector": [0.18, 0.01, 0.85, 0.80], "payload": {"square": {"float": { "value": [10, 11]}}}},
    {"id": 3, "vector": [0.24, 0.18, 0.22, 0.45], "payload": {"count": {"integer": {"value": [0]}}}},
    {"id": 4, "vector": [0.24, 0.18, 0.22, 0.45], "payload": {"coords": {"geo": {"value": [{ "lat": 1.0, "lon": 2.0}, { "lat": 3.0, "lon": 4.0}]}}}},
    {"id": 5, "vector": [0.35, 0.08, 0.11, 0.44]}
  ]
}' \
[::]:6334 qdrant.Points/Upsert
```

Expected response:
```json
{
  "result": {
    "status": "Completed"
  },
  "time": 0.004128988
}
```


