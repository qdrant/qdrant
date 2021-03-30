# :black_square_button: Qdrant

![Tests](https://github.com/qdrant/qdrant/workflows/Tests/badge.svg)

Qdrant (read: _quadrant_ ) is a vector similarity search engine.
It provides a production-ready service with a convenient API to store, search, and manage points (vectors with additional payload).

It is tailored on extended support of filtering, which makes it useful for all sorts of neural-network or semantic based matching, faceted search, and other applications. 

Qdrant is written in Rust :crab:, which makes it reliable even under high load.

## API [![OpenAPI docs](https://img.shields.io/badge/docs-OpenAPI3.0-success)](https://qdrant.github.io/qdrant/redoc/index.html)

Online OpenAPI 3.0 documentation is available [here](https://qdrant.github.io/qdrant/redoc/index.html).
OpenAPI makes it easy to generate client for virtually any framework or programing language.

You can also download raw OpenAPI [definitions](openapi/openapi.yaml).

## Features

### Filtering

Qdrant supports any combinations of `should`, `must` and `must_not` conditions,
which makes it possible to use in applications when object could not be described solely by vector.
It could be location features, availability flags, and other custom properties businesses should take into account.

### Write-ahead logging

Once service confirmed an update - it won't lose data even in case of power shut down. 
All operations are stored in the update journal and the latest database state could be easily reconstructed at any moment.

### Stand-alone

Qdrant does not rely on any external database or orchestration controller, which makes it very easy to configure.

## Usage

### Docker

Build your own from source

```bash
docker build . --tag=qdrant
```

Or use latest pre-built image from [DockerHub](https://hub.docker.com/r/generall/qdrant)

```bash
docker pull generall/qdrant
```

To run container use command:

```bash
docker run -p 6333:6333 \
    -v $(pwd)/path/to/data:/qdrant/storage \
    -v $(pwd)/path/to/custom_config.yaml:/qdrant/config/production.yaml \
    qdrant
```

* `/qdrant/storage` - is a place where Qdrant persists all your data. 
Make sure to mount it as a volume, otherwise docker will drop it with the container. 
* `/qdrant/config/production.yaml` - is the file with engine configuration. You can override any value from the [reference config](config/config.yaml) 

Now Qdrant should be accessible at [localhost:6333](http://localhost:6333/)

## Examples

This example covers the most basic use-case - collection creation and basic vector search.
For additional information please refer to the [documentation](https://qdrant.github.io/qdrant/redoc/index.html).

### Create collection
First - let's create a collection with dot-production metric.
```bash
curl -X POST 'http://localhost:6333/collections' \
    -H 'Content-Type: application/json' \
    --data-raw '{
        "create_collection": {
            "name": "test_collection",
            "vector_size": 4,
            "distance": "Dot"
        }
    }'
```

Expected response:
```json
{
    "result": true,
    "status": "ok",
    "time": 0.031095451
}
```


We can ensure that collection was created:
```bash
curl 'http://localhost:6333/collections/test_collection'
```

Expected response:

```json
{
  "result": {
    "vectors_count": 0,
    "segments_count": 5,
    "disk_data_size": 0,
    "ram_data_size": 0,
    "config": {
      "vector_size": 4,
      "index": {
        "type": "plain",
        "options": {}
      },
      "distance": "Dot",
      "storage_type": {
        "type": "in_memory"
      }
    }
  },
  "status": "ok",
  "time": 2.1199e-05
}
```


### Add points
Let's now add vectors with some payload:

```bash
curl -L -X POST 'http://localhost:6333/collections/test_collection?wait=true' \
    -H 'Content-Type: application/json' \
    --data-raw '{
      "upsert_points": {
        "points": [
          {"id": 1, "vector": [0.05, 0.61, 0.76, 0.74], "payload": {"city": {"type": "keyword", "value": "Berlin"}}},
          {"id": 2, "vector": [0.19, 0.81, 0.75, 0.11], "payload": {"city": {"type": "keyword", "value": ["Berlin", "London"] }}},
          {"id": 3, "vector": [0.36, 0.55, 0.47, 0.94], "payload": {"city": {"type": "keyword", "value": ["Berlin", "Moscow"] }}},
          {"id": 4, "vector": [0.18, 0.01, 0.85, 0.80], "payload": {"city": {"type": "keyword", "value": ["London", "Moscow"]}}},
          {"id": 5, "vector": [0.24, 0.18, 0.22, 0.44], "payload": {"count": {"type": "integer", "value": [0]}}},
          {"id": 6, "vector": [0.35, 0.08, 0.11, 0.44]}
        ]
      }
    }'
```

Expected response:
```json
{
    "result": {
        "operation_id": 0,
        "status": "completed"
    },
    "status": "ok",
    "time": 0.000206061
}
```

### Search with filtering

Let's start with a basic request:

```bash
curl -L -X POST 'http://localhost:6333/collections/test_collection/points/search' \
    -H 'Content-Type: application/json' \
    --data-raw '{
        "vector": [0.2,0.1,0.9,0.7],
        "top": 3
    }'
```

Expected response:

```json
{
    "result": [
        { "id": 4, "score": 1.362 },
        { "id": 1, "score": 1.273 },
        { "id": 3, "score": 1.208 }
    ],
    "status": "ok",
    "time": 0.000055785
}
```

But result is different if we add a filter:

```bash
curl -L -X POST 'http://localhost:6333/collections/test_collection/points/search' \
    -H 'Content-Type: application/json' \
    --data-raw '{
      "filter": {
          "should": [
              {
                  "key": "city",
                  "match": {
                      "keyword": "London"
                  }
              }
          ]
      },
      "vector": [0.2, 0.1, 0.9, 0.7],
      "top": 3
  }'
```

Expected response:
```json
{
    "result": [
        { "id": 4, "score": 1.362 },
        { "id": 2, "score": 0.871 }
    ],
    "status": "ok",
    "time": 0.000093972
}
```
