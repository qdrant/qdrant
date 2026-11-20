"""Latency of profiled vs. unprofiled requests. Usage: bench.py PORT LABEL [--load] [--profile analyze]"""
import http.client, json, random, statistics, sys, time
port = int(sys.argv[1]); label = sys.argv[2]; load = "--load" in sys.argv
profile = sys.argv[sys.argv.index("--profile") + 1] if "--profile" in sys.argv else None
C = "bench"; DIM = 128; N = 100_000
conn = http.client.HTTPConnection("127.0.0.1", port)
def req(method, path, body=None):
    conn.request(method, path, body=json.dumps(body) if body is not None else None, headers={"content-type": "application/json"})
    r = conn.getresponse(); data = r.read(); return json.loads(data)
rnd = random.Random(1)
if load:
    req("DELETE", f"/collections/{C}")
    req("PUT", f"/collections/{C}", {"vectors": {"size": DIM, "distance": "Cosine"}, "optimizers_config": {"default_segment_number": 4}})
    req("PUT", f"/collections/{C}/index?wait=true", {"field_name": "city", "field_schema": "keyword"})
    for s in range(0, N, 2000):
        req("PUT", f"/collections/{C}/points?wait=true", {"points": [{"id": i, "vector": [rnd.random() for _ in range(DIM)], "payload": {"city": f"c{i % 100}", "color": ["red", "green", "blue"][i % 3], "price": i % 1000}} for i in range(s, s + 2000)]})
    while True:
        info = req("GET", f"/collections/{C}")["result"]
        if info["status"] == "green" and info.get("optimizer_status") == "ok": break
        time.sleep(2)
    print("loaded", info["points_count"], "segments", info["segments_count"], file=sys.stderr)
qs = random.Random(2)
vectors = [[qs.random() for _ in range(DIM)] for _ in range(200)]
workloads = {
    "query_unfiltered": lambda v: ("/points/query", {"query": v, "limit": 10}),
    "query_indexed_filter": lambda v: ("/points/query", {"query": v, "limit": 10, "filter": {"must": [{"key": "city", "match": {"value": "c7"}}]}}),
    "query_unindexed_filter": lambda v: ("/points/query", {"query": v, "limit": 10, "filter": {"must": [{"key": "color", "match": {"value": "red"}}]}}),
    "scroll_filter": lambda v: ("/points/scroll", {"limit": 10, "filter": {"must": [{"key": "city", "match": {"value": "c7"}}]}}),
    "count_exact": lambda v: ("/points/count", {"exact": True, "filter": {"must": [{"key": "color", "match": {"value": "red"}}]}}),
}
suffix = f"?profile={profile}" if profile else ""
out = {}
for name, make in workloads.items():
    times = []
    for rep in range(3):  # warmup + 2 measured rounds
        for v in vectors:
            path, body = make(v)
            t = time.perf_counter(); r = req("POST", f"/collections/{C}{path}{suffix}", body); dt = time.perf_counter() - t
            assert r["status"] == "ok", r
            if rep > 0: times.append(dt * 1000)
    times.sort()
    out[name] = {"median_ms": statistics.median(times), "mean_ms": statistics.fmean(times), "p95_ms": times[int(len(times) * 0.95)]}
print(json.dumps({"label": label, "results": out}))
