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
QDRANT__SERVICE__GRPC_PORT="6334" cargo run --bin qdrant
```
It will run qdrant exposing both json and grpc API. If you do not want to use JSON API, add ``--no-default-features ``
flag as well:
```bash
QDRANT__SERVICE__GRPC_PORT="6334" cargo run --no-default-features --bin qdrant
```
Note that actix is not compiled in this case.

## GRPC Service Health Check
Execute the following command
```bash
grpcurl -plaintext -import-path ./lib/api/src/grpc/proto/ -proto qdrant.proto -d '{}' 0.0.0.0:6334 qdrant.Qdrant/HealthCheck
```
Here and below the ```./lib/api/src/grpc/proto/``` should be a path to the folder with a protobuf schemas.
Expected response:
```json
{
  "title": "qdrant - vector search engine",
  "version": "<version>"
}
```

## Collections

### Create collection
First - let's create a collection with dot-production metric.
```bash
grpcurl -plaintext -import-path ./lib/api/src/grpc/proto/ -proto qdrant.proto -d '{
        "collection_name": "test_collection",
        "vectors_config": {
            "params": {
                "size": 4,
                "distance": "Dot"
            }
        }
    }' \
0.0.0.0:6334 qdrant.Collections/Create
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
grpcurl -plaintext -import-path ./lib/api/src/grpc/proto/ -proto qdrant.proto 0.0.0.0:6334 qdrant.Collections/List
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
grpcurl -plaintext -import-path ./lib/api/src/grpc/proto/ -proto qdrant.proto -d '{
        "collection_name": "test_collection",
        "optimizers_config": {
          "max_segment_size": 100
        }
    }' \
0.0.0.0:6334 qdrant.Collections/Update
```

## Add points
Let's now add vectors with some payload:

```bash
grpcurl -plaintext -import-path ./lib/api/src/grpc/proto/ -proto qdrant.proto -d '{
  "collection_name": "test_collection",
  "wait": true,
  "points": [
    {
      "id": {
        "num": 1
      }, 
      "vectors": { 
        "vector": { 
          "data": [0.05, 0.61, 0.76, 0.74] 
        } 
      }, 
      "payload": 
      {
        "city": { 
          "string_value": "Berlin" 
        }, 
        "country": { 
          "string_value": "Germany" 
        }, 
        "population": { 
          "integer_value": 1000000 
        }, 
        "square": { 
          "double_value": 12.5 
        }, 
        "coords": { 
          "struct_value": {
            "fields":{ 
              "lat": { 
                "double_value":1.0 
              }, 
              "lon": { 
                "double_value": 2.0 
              } 
            }
          }
        }
      }
    },
    {"id": {"num": 2}, "vectors": {"vector": {"data": [0.18, 0.01, 0.85, 0.80]}}, "payload": {"square": {"list_value": {"values": [{"integer_value": 10}, {"integer_value": 11}]}} }},
    {"id": {"num": 3}, "vectors": {"vector": {"data": [0.24, 0.18, 0.22, 0.45]}}, "payload": {"count": {"list_value": {"values": [{"integer_value": 0}]}} }},
    {"id": {"num": 4}, "vectors": {"vector": {"data": [0.24, 0.18, 0.22, 0.45]}}, "payload": {"coords": {"list_value": {"values": [{ "struct_value": {"fields": { "lat": { "double_value":1.0 }, "lon": { "double_value": 2.0 } } } }, { "struct_value": {"fields":  { "lat": { "double_value":3.0 }, "lon": { "double_value": 4.0 } } } }] }} }},
    {"id": {"num": 5}, "vectors": {"vector": {"data": [0.35, 0.08, 0.11, 0.44]}}}
  ]
}' \
0.0.0.0:6334 qdrant.Points/Upsert
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

### Delete collection
The qdrant.Collections/UpdateCollections rpc could also be used to delete a collection:
```bash
grpcurl -plaintext -import-path ./lib/api/src/grpc/proto/ -proto qdrant.proto -d '{
          "collection_name": "test_collection"
    }' \
0.0.0.0:6334 qdrant.Collections/Delete
```
