# ABAC-style Hybrid Retrieval (Qdrant) with Post-Retrieval Checks

**Why this matters.** Enterprise RAG often needs attribute-gated access (tenant / language / sensitivity) and a defense-in-depth posture.  
This recipe shows: (1) metadata **pre-filters**, (2) **hybrid retrieval** (keyword + vector), (3) **RRF** fusion, and (4) a **deny-by-default** post-retrieval recheck per chunk.

## Policy & Entities
**User claims:** `tenant`, `allowed_languages`, `clearance`  
**Doc payload:** `tenant`, `language`, `sensitivity`  
**Allow iff:** tenant matches ∧ language is allowed ∧ sensitivity ≤ clearance.

## Steps
1) Index docs with payload metadata  
2) Apply **pre-filter** (policy) at query time  
3) Run **vector** and **keyword** searches on the allowed set  
4) Fuse with **RRF**  
5) **Post-retrieval recheck** each chunk (deny on mismatch)

---

## How to run

### Prerequisites
- Docker (to run Qdrant locally)
- Python 3.10+
- `pip install qdrant-client numpy`

### Start Qdrant and run the example
```bash
docker run -p 6333:6333 -p 6334:6334 qdrant/qdrant
pip install qdrant-client numpy
python docs/examples/abac_hybrid_policy_qdrant.py --query "Annual filing requirements" --user EN_ONLY
```

## Minimal example (core pattern)
Helper functions `text_to_vec`, `keyword_score`, `rrf`, and `postcheck` are defined in the full script at `docs/examples/abac_hybrid_policy_qdrant.py`.
```python
from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue, Range

# User claims (example)
user = {"tenant": "alpha", "allowed_languages": ["EN"], "clearance": 2}

# 1) Pre-filter: tenant + sensitivity (language rechecked post-retrieval)
base_filter = Filter(must=[
    FieldCondition(key="tenant", match=MatchValue(value=user["tenant"])),
    FieldCondition(key="sensitivity", range=Range(lte=int(user["clearance"]))),
])

client = QdrantClient(url="http://localhost:6333")

# 2) Vector query with the modern API (no deprecation warnings)
qvec = text_to_vec(query).tolist()  # any embedding; this recipe uses a tiny CPU-only hash emb
res = client.query_points(
    collection_name=COLLECTION,
    query=qvec,
    filter=base_filter,
    limit=50,
    with_payload=True,
    with_vectors=False,
)
vec_hits = res.points  # payload + scores on the allowed set

# 3) Keyword ranking on the same allowed set (client-side)
kw_ranking  = sorted([(h.payload["text"], keyword_score(query, h.payload["text"])) for h in vec_hits],
                     key=lambda x: x[1], reverse=True)
vec_ranking = [(h.payload["text"], float(h.score)) for h in vec_hits]

# 4) RRF fusion
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

fused = rrf([vec_ranking, kw_ranking])
text2payload = {h.payload["text"]: h.payload for h in vec_hits}

# 5) Post-retrieval recheck (deny-by-default) adds the language gate
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

final = [text2payload[t] for t, _ in sorted(fused.items(), key=lambda x: x[1], reverse=True)
         if postcheck(text2payload[t], user)][:topk]

```