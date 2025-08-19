# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0


#!/usr/bin/env python3
import argparse
import hashlib
import json
from pathlib import Path
from typing import Dict, List

import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.http.models import (
    Distance,
    FieldCondition,
    Filter,
    MatchValue,
    PointStruct,
    Range,
    VectorParams,
)

DATA = Path(__file__).resolve().parents[1] / "data" / "synthetic_docs.jsonl"
COLLECTION = "abac_hybrid_demo"
DIM = 64

USERS = {
    "EL_ONLY": {"tenant": "alpha", "allowed_languages": ["EL"], "clearance": 2},
    "EN_ONLY": {"tenant": "alpha", "allowed_languages": ["EN"], "clearance": 2},
    "BILINGUAL": {"tenant": "beta", "allowed_languages": ["EL", "EN"], "clearance": 3},
}


def text_to_vec(text: str, dim: int = DIM) -> np.ndarray:
    """Create a tiny, deterministic hash-based embedding for a text.

    This is a CPU-only toy embedding used to keep the example self-contained
    (no external model calls). Each token is hashed into a fixed-size vector.

    Args:
    ----
        text: Input string to embed.
        dim: Embedding dimensionality.

    Returns:
    -------
        A (dim,) float32 numpy array normalized to unit length.
    """
    v = np.zeros(dim, dtype=np.float32)
    for tok in text.lower().split():
        h = int(hashlib.md5(tok.encode()).hexdigest(), 16)
        v[h % dim] += 1.0
    n = np.linalg.norm(v)
    return v / (n + 1e-9)


def keyword_score(query: str, text: str) -> float:
    """Compute a simple keyword-overlap score between query and text.

    The score is (#matching tokens) / (#tokens in text). It’s a trivial proxy
    for keyword relevance and is intentionally lightweight.

    Args:
    ----
        query: The user query string.
        text: The document text.

    Returns:
    -------
        A float in [0, 1] indicating overlap strength.
    """
    qs = set(query.lower().split())
    ts = text.lower().split()
    hits = sum(1 for w in ts if w in qs)
    return hits / (len(ts) + 1e-9)


def build_base_filter(user: Dict) -> Filter:
    """Build a Qdrant metadata filter for pre-retrieval policy gates.

    This filter enforces tenant equality and a maximum sensitivity level.
    Language membership is enforced later in a response-time post-check,
    because the user may allow multiple languages.

    Args:
    ----
        user: Dict with keys: 'tenant' (str) and 'clearance' (int).

    Returns:
    -------
        Qdrant Filter object to use in query_points(..., filter=...).
    """
    return Filter(
        must=[
            FieldCondition(key="tenant", match=MatchValue(value=user["tenant"])),
            FieldCondition(key="sensitivity", range=Range(lte=int(user["clearance"]))),
        ]
    )


def load_docs(path: Path) -> List[Dict]:
    """Load a tiny synthetic corpus from a JSONL file.

    Each line should be a JSON object containing:
    id, tenant, language, sensitivity, text.

    Args:
    ----
        path: Path to the .jsonl file.

    Returns:
    -------
        List of document dicts.
    """
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            out.append(json.loads(line))
    return out


def ensure_collection(client: QdrantClient):
    """Create or recreate the demo collection with cosine vectors.

    If the collection does not exist, it is created with the configured
    dimensionality and cosine similarity.

    Args:
    ----
        client: Initialized QdrantClient connected to the target instance.
    """
    names = [c.name for c in client.get_collections().collections]
    if COLLECTION not in names:
        client.recreate_collection(
            collection_name=COLLECTION,
            vectors_config=VectorParams(size=DIM, distance=Distance.COSINE),
        )


def upload_points(client: QdrantClient, docs: List[Dict]):
    """Upsert documents as points with vectors and payload metadata.

    Payload schema: {tenant, language, sensitivity, text}

    Args:
    ----
        client: Qdrant client.
        docs: List of documents to upload.
    """
    points = []
    for d in docs:
        vec = text_to_vec(d["text"]).tolist()
        payload = {
            "tenant": d["tenant"],
            "language": d["language"],
            "sensitivity": int(d["sensitivity"]),
            "text": d["text"],
        }
        points.append(PointStruct(id=int(d["id"]), vector=vec, payload=payload))
    client.upsert(collection_name=COLLECTION, points=points)


def rrf(rankings, k=60):
    """Reciprocal Rank Fusion (RRF) for combining ranked lists.

    Given one or more ranked lists (e.g., vector and keyword), RRF assigns a
    fused score to each key based on 1 / (k + rank). Lower ranks (better items)
    contribute more to the total.

    Args:
    ----
        rankings: Iterable of ranked lists, each as [(key, score), ...].
        k: RRF constant; larger k smooths differences between ranks.

    Returns:
    -------
        Dict mapping key -> fused score (float).
    """
    from collections import defaultdict

    scores = defaultdict(float)
    for ranking in rankings:
        for rank, (key, _) in enumerate(ranking, start=1):
            scores[key] += 1.0 / (k + rank)
    return scores


def postcheck(payload, user):
    """Response-time policy recheck with deny-by-default.

    This enforces the full ABAC policy at response time (defense in depth):
    - tenant must match
    - language must be in user's allowed_languages
    - sensitivity must be <= user's clearance

    Args:
    ----
        payload: Document payload dict.
        user: User claims dict with 'tenant', 'allowed_languages', 'clearance'.

    Returns:
    -------
        True if the payload passes policy; False otherwise.
    """
    return (
        payload["tenant"] == user["tenant"]
        and payload["language"] in user["allowed_languages"]
        and int(payload["sensitivity"]) <= int(user["clearance"])
    )


def is_relevant(query: str, payload: Dict) -> bool:
    """Toy relevance check used to compute quick metrics.

    Uses token overlap between query and payload['text'] as a simple proxy for
    “is this relevant?”. This keeps the example CPU-only and deterministic.

    Args:
    ----
        query: The user query string.
        payload: A document payload dict with a 'text' field.

    Returns:
    -------
        True if there is at least one overlapping token; False otherwise.
    """
    qs = set(query.lower().split())
    ts = set(payload["text"].lower().split())
    return len(qs & ts) > 0


def compute_metrics(query: str, user: Dict, ranked_payloads: List[Dict], k: int = 5):
    """Compute tiny retrieval metrics over a filtered candidate pool.

    Steps:
      1) Scroll a small, policy-filtered pool (tenant + sensitivity) from Qdrant.
      2) Apply the language gate (user.allowed_languages) to the pool.
      3) Define “relevant” via keyword overlap (toy proxy).
      4) Compute hits@k, precision@k, recall@k for the top-k ranked results.

    Args:
    ----
        query: The query string to evaluate.
        user: User claims dict (tenant, allowed_languages, clearance).
        ranked_payloads: Final ranked list (already post-checked).
        k: Cutoff for metrics.

    Returns:
    -------
        Dict with keys: 'hits@k', 'precision@k', 'recall@k', 'relevant_pool'.
    """
    client = QdrantClient(url="http://localhost:6333")
    base_filter = build_base_filter(user)
    scroll_docs, _ = client.scroll(
        collection_name=COLLECTION,
        scroll_filter=base_filter,
        limit=200,
        with_payload=True,
    )
    candidate_payloads = [p.payload for p in scroll_docs]
    candidate_payloads = [
        p for p in candidate_payloads if p["language"] in user["allowed_languages"]
    ]
    relevant = [p for p in candidate_payloads if is_relevant(query, p)]
    rel_ids = {p["text"] for p in relevant}
    topk = ranked_payloads[:k]
    pred_ids = {p["text"] for p in topk}
    hits = len(rel_ids & pred_ids)
    prec = hits / max(k, 1)
    rec = hits / max(len(rel_ids), 1)
    return {
        "hits@k": hits,
        "precision@k": round(prec, 3),
        "recall@k": round(rec, 3),
        "relevant_pool": len(rel_ids),
    }


def main():
    """CLI entrypoint: runs the ABAC + hybrid retrieval demo end-to-end.

    Flow:
      - Parse CLI args (host/port/query/user/topk).
      - Load docs, ensure collection, upsert points.
      - Build pre-filter from user claims (tenant + sensitivity).
      - Run vector search via query_points(..., filter=...).
      - Rank the same allowed set with a keyword score.
      - Fuse with RRF; apply postcheck; print top-k.
      - Compute tiny metrics (hits@k / precision@k / recall@k).

    Notes:
      - This demo is CPU-only and model-agnostic.
      - Postcheck enforces the language gate and deny-by-default posture.

    Returns:
    -------
      None. Prints results and metrics to stdout.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="http://localhost")
    ap.add_argument("--port", type=int, default=6333)
    ap.add_argument("--query", required=True)
    ap.add_argument("--user", choices=list(USERS.keys()), default="EL_ONLY")
    ap.add_argument("--topk", type=int, default=5)
    args = ap.parse_args()

    user = USERS[args.user]
    client = QdrantClient(url=f"{args.host}:{args.port}")

    docs = load_docs(DATA)
    ensure_collection(client)
    upload_points(client, docs)
    base_filter = build_base_filter(user)

    qvec = text_to_vec(args.query).tolist()
    _res = client.query_points(
        collection_name=COLLECTION,
        query=qvec,
        limit=50,
        query_filter=base_filter,  # SEE NOTE BELOW
        with_payload=True,
        with_vectors=False,
    )
    vec_hits = _res.points if hasattr(_res, "points") else _res

    allowed_docs = [h.payload for h in vec_hits]
    if not allowed_docs:
        scroll_res = client.scroll(
            collection_name=COLLECTION,
            scroll_filter=base_filter,
            limit=100,
            with_payload=True,
        )
        allowed_docs = [p.payload for p in scroll_res[0]]

    kw_ranking = sorted(
        [(d["text"], keyword_score(args.query, d["text"])) for d in allowed_docs],
        key=lambda x: x[1],
        reverse=True,
    )
    vec_ranking = [(h.payload["text"], float(h.score)) for h in vec_hits]

    fused_scores = rrf([vec_ranking, kw_ranking])
    text2payload = {d["text"]: d for d in allowed_docs}

    final_payloads = []
    for txt, score in sorted(fused_scores.items(), key=lambda x: x[1], reverse=True):
        payload = text2payload.get(txt)
        if not payload:
            continue
        if not postcheck(payload, user):
            continue
        final_payloads.append(payload)
        if len(final_payloads) >= args.topk:
            break

    print(f"User={args.user} | Query='{args.query}'")
    print("-" * 72)
    for p in final_payloads:
        print(f"tenant={p['tenant']} lang={p['language']} sens={p['sensitivity']}")
        print(f"  {p['text']}\n")

    metrics = compute_metrics(args.query, user, final_payloads, k=args.topk)
    print("Metrics:", metrics)


if __name__ == "__main__":
    main()
