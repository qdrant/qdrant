# Quick Start

This example covers the most basic use-case - collection creation and basic vector search.
For additional information please refer to the [API documentation](https://qdrant.github.io/qdrant/redoc/index.html).

You can also try all Qdrant REST API in our [Swagger UI](https://ui.qdrant.tech/).

## Docker 🐳

Use latest pre-built image from [DockerHub](https://hub.docker.com/r/qdrant/qdrant)

```bash
docker pull qdrant/qdrant
```

Run it with default configuration:

```bash
docker run -p 6333:6333 qdrant/qdrant
```

Build your own from source

```bash
docker build . --tag=qdrant/qdrant
```

And once you need a fine-grained setup, you can also define a storage path and custom configuration:

```bash
docker run -p 6333:6333 \
    -v $(pwd)/path/to/data:/qdrant/storage \
    -v $(pwd)/path/to/snapshots:/qdrant/snapshots \
    -v $(pwd)/path/to/custom_config.yaml:/qdrant/config/production.yaml \
    qdrant/qdrant
```

- `/qdrant/storage` - is the place where Qdrant persists all your data.
  Make sure to mount it as a volume, otherwise docker will drop it with the container.
- `/qdrant/snapshots` - is the place where Qdrant stores [snapshots](https://qdrant.tech/documentation/concepts/snapshots/)
- `/qdrant/config/production.yaml` - is the file with engine configuration. You can override any value from the [reference config](https://github.com/qdrant/qdrant/blob/master/config/config.yaml). In a real production environment, you should enable authentication by setting `service.apiKey`.
- For production environments, consider also setting [`--read-only`](https://docs.docker.com/reference/cli/docker/container/run/#read-only) and `--user=1000:2000` to further secure your Qdrant instance. Or use [our Helm chart](https://github.com/qdrant/qdrant-helm) or [Qdrant Cloud](https://qdrant.tech/documentation/cloud/) which sets these by default.

Now Qdrant should be accessible at [localhost:6333](http://localhost:6333/).

## Create collection

First - let's create a collection with dot-production metric.

```bash
curl -X PUT 'http://localhost:6333/collections/test_collection' \
    -H 'Content-Type: application/json' \
    --data-raw '{
        "vectors": {
          "size": 4,
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
    "status": "green",
    "vectors_count": 0,
    "segments_count": 5,
    "disk_data_size": 0,
    "ram_data_size": 0,
    "config": {
      "params": {
        "vectors": {
          "size": 4,
          "distance": "Dot"
        }
      },
      "hnsw_config": {
        "m": 16,
        "ef_construct": 100,
        "full_scan_threshold": 10000
      },
      "optimizer_config": {
        "deleted_threshold": 0.2,
        "vacuum_min_vector_number": 1000,
        "max_segment_number": 5,
        "memmap_threshold": 50000,
        "indexing_threshold": 20000,
        "flush_interval_sec": 1
      },
      "wal_config": {
        "wal_capacity_mb": 32,
        "wal_segments_ahead": 0
      }
    }
  },
  "status": "ok",
  "time": 2.1199e-5
}
```

## Add points

Let's now add vectors with some payload:

```bash
curl -L -X PUT 'http://localhost:6333/collections/test_collection/points?wait=true' \
    -H 'Content-Type: application/json' \
    --data-raw '{
        "points": [
          {"id": 1, "vector": [0.05, 0.61, 0.76, 0.74], "payload": {"city": "Berlin"}},
          {"id": 2, "vector": [0.19, 0.81, 0.75, 0.11], "payload": {"city": ["Berlin", "London"] }},
          {"id": 3, "vector": [0.36, 0.55, 0.47, 0.94], "payload": {"city": ["Berlin", "Moscow"] }},
          {"id": 4, "vector": [0.18, 0.01, 0.85, 0.80], "payload": {"city": ["London", "Moscow"] }},
          {"id": 5, "vector": [0.24, 0.18, 0.22, 0.44], "payload": {"count": [0] }},
          {"id": 6, "vector": [0.35, 0.08, 0.11, 0.44]}
        ]
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

## Search with filtering

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
    { "id": 4, "score": 1.362, "payload": null, "version": 0 },
    { "id": 1, "score": 1.273, "payload": null, "version": 0 },
    { "id": 3, "score": 1.208, "payload": null, "version": 0 }
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
                      "value": "London"
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
