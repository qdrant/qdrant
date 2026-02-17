from qdrant_client import QdrantClient
from qdrant_client.http import models

q = QdrantClient(url="http://localhost:6333", prefer_grpc=True)

# We have points with

def migrate_points():
    points_to_migrate = []
    offset = 0

    while True:
        print(f"Fetching points with offset: {offset}")
        points, offset = q.scroll(
            collection_name="benchmark",
            scroll_filter=models.Filter(
                must=[models.FieldCondition(key="a", match=models.MatchValue(value="keyword_0"))]
            ),
            with_payload=True,
            with_vectors=True,
            offset=offset,
            # limit=10,
        )

        if not points or offset is None:
            break

        points_to_migrate.extend(points)

        if True:
            break

    # Create new shard for tenant
    shard_key = "org0" # models.ShardKey()

    try:
        q.create_shard_key("benchmark", shard_key) # missing API to list shard keys
    except Exception as e:
        if e.args[0].details == 'Bad request: Sharding key "org0" already exists for collection benchmark':
            print("Shard key already exists")
        else:
            raise e

    points_to_migrate = [models.PointStruct(id=point.id, vector=point.vector, payload=point.payload) for point in points_to_migrate]

    print("Total points to migrate:", len(points_to_migrate))

    # Insert into new shard
    q.upsert(
        collection_name="benchmark",
        points=points_to_migrate,
        wait=True,
        shard_key_selector=shard_key,
    )

    migrated_points, _ = q.scroll(
        collection_name="benchmark",
        shard_key_selector=shard_key,
        limit=len(points_to_migrate),
    )

    print("Migrated points count:", len(migrated_points))

    # Delete from old shard
    q.delete(
        collection_name="benchmark",
        points_selector=models.Filter(
            must=[models.FieldCondition(key="a", match=models.MatchValue(value="keyword_0"))]
        ),
        shard_key_selector="default",
        wait=True,
    )

if __name__ == "__main__":
    migrate_points()


# Steps:
# bfb --keywords 2 --tenants true --skip-wait-index --shard-key default --skip-upload # create collection
#
# PUT /collections/benchmark/shards
# {
#   "shard_key": "default"
# }
#
# bfb --keywords 2 --tenants true --skip-wait-index --shard-key default --skip-create # upsert points
