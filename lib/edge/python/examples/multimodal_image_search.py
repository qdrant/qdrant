#!/usr/bin/env python3
"""Multimodal image search example using Qdrant Edge and FastEmbed CLIP.

Overview:
- Multimodal search requires text and image embeddings in a shared space.
- CLIP embeddings are used to index the downloaded sample images.
- Usage of FastEmbed embeddings for text and image search. 
- The example runs a text query and an image query against the index.
"""

import os
import shutil
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen

from fastembed import ImageEmbedding, TextEmbedding
from qdrant_edge import (
    Distance,
    EdgeConfig,
    EdgeShard,
    EdgeVectorParams,
    Point,
    Query,
    QueryRequest,
    UpdateOperation,
)


MODEL_DIM = 512
VECTOR_NAME = "clip"
IMAGE_MODEL = "Qdrant/clip-ViT-B-32-vision"
TEXT_MODEL = "Qdrant/clip-ViT-B-32-text"

DATA_DIR = Path(__file__).parent.parent.parent / "data"
TMP_DIR = DATA_DIR / "tmp" / "qdrant_edge_multimodal_image_search"
IMAGES_DIR = TMP_DIR / "images"
SHARD_DIR = TMP_DIR / "shard"

ITEMS = [
    {
        "id": 1,
        "name": "Apple",
        "image_url": "https://media.istockphoto.com/id/185262648/photo/red-apple-with-leaf-isolated-on-white-background.jpg",
    },
    {
        "id": 2,
        "name": "Mug",
        "image_url": "https://t4.ftcdn.net/jpg/14/34/55/33/360_F_1434553337_mvkuQdipEtEXKmwCBGDSYbW5IgzRR84t.jpg",
    },
    {
        "id": 3,
        "name": "Backpack",
        "image_url": "https://media.istockphoto.com/id/525626653/photo/green-backpack-isolated-on-white.jpg",
    },
    {
        "id": 4,
        "name": "Orange",
        "image_url": "https://media.istockphoto.com/id/477836156/photo/orange-fruit-isolated-on-white.jpg",
    },
    {
        "id": 5,
        "name": "Tea Cup",
        "image_url": "https://m.media-amazon.com/images/I/715K5qJKmsL._AC_UF894,1000_QL80_.jpg",
    },
    {
        "id": 6,
        "name": "Suitcase",
        "image_url": "https://hauptstadtkoffer.de/media/catalog/product/cache/92babe6398cec55ede6460e286277906/h/k/hk20-8278-co.main.jpg",
    },
]

def download(url: str, path: Path) -> None:
    request = Request(url, headers={"User-Agent": "qdrant-edge-example"})
    with urlopen(request, timeout=50) as response:
        path.write_bytes(response.read())

def embed_text(model: TextEmbedding, text: str) -> list[float]:
    """Generate embedding vector from text query using the text model."""
    return list(model.embed([text]))[0].tolist()

def embed_image(model: ImageEmbedding, image_path: Path) -> list[float]:
    """Generate embedding vector from image using the image model."""
    return list(model.embed([image_path]))[0].tolist()

def print_results(title: str, results) -> None:
    print(f"\n{title}")
    for point in results:
        print(f"{point.score:.4f}  {point.payload['name']}")

def main() -> None:
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    os.makedirs(IMAGES_DIR)
    os.makedirs(SHARD_DIR)

    image_model = ImageEmbedding(model_name=IMAGE_MODEL)
    text_model = TextEmbedding(model_name=TEXT_MODEL)

    shard = EdgeShard.create(
        SHARD_DIR,
        EdgeConfig(
            vectors={
                VECTOR_NAME: EdgeVectorParams(
                    size=MODEL_DIM,
                    distance=Distance.Cosine,
                )
            }
        ),
    )

    points = []
    for item in ITEMS:
        image_path = IMAGES_DIR / f"{item['id']}.jpg"
        if not image_path.exists():
            download(item["image_url"], image_path)

        points.append(
            Point(
                item["id"],
                {VECTOR_NAME: embed_image(image_model, image_path)},
                {"name": item["name"]},
            )
        )

    shard.update(UpdateOperation.upsert_points(points))
    shard.optimize()

    # Text-based search: user query as text
    user_text_query = "a red apple"
    text_embedding = embed_text(text_model, user_text_query)
    text_results = shard.query(
        QueryRequest(
            query=Query.Nearest(text_embedding, using=VECTOR_NAME),
            limit=2,
            with_payload=True,
        )
    )
    print_results(f"Text query: {user_text_query}", text_results)

    # Image-based search: user query as image
    user_image_query = IMAGES_DIR / "2.jpg"
    image_embedding = embed_image(image_model, user_image_query)
    image_results = shard.query(
        QueryRequest(
            query=Query.Nearest(image_embedding, using=VECTOR_NAME),
            limit=2,
            with_payload=True,
        )
    )
    print_results(f"Image query: {user_image_query}", image_results)

if __name__ == "__main__":
    main()
