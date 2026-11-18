"""Paired A/B latency: interleave identical requests to two endpoints.
Usage: paired.py LABEL HOST_A SUFFIX_A HOST_B SUFFIX_B  (HOST = port)"""
import http.client, json, random, statistics, sys, time
label, pa, sa, pb, sb = sys.argv[1], int(sys.argv[2]), sys.argv[3], int(sys.argv[4]), sys.argv[5]
sa = "" if sa == "-" else sa; sb = "" if sb == "-" else sb
ca, cb = http.client.HTTPConnection("127.0.0.1", pa), http.client.HTTPConnection("127.0.0.1", pb)
def call(conn, path, body):
    data = json.dumps(body)
    t = time.perf_counter(); conn.request("POST", path, body=data, headers={"content-type": "application/json"})
    r = json.loads(conn.getresponse().read()); dt = (time.perf_counter() - t) * 1000
    assert r["status"] == "ok", r; return dt
qs = random.Random(2); DIM = 128
vectors = [[qs.random() for _ in range(DIM)] for _ in range(200)]
workloads = {
    "query_unfiltered": lambda v: ("/points/query", {"query": v, "limit": 10}),
    "query_indexed_filter": lambda v: ("/points/query", {"query": v, "limit": 10, "filter": {"must": [{"key": "city", "match": {"value": "c7"}}]}}),
    "query_unindexed_filter": lambda v: ("/points/query", {"query": v, "limit": 10, "filter": {"must": [{"key": "color", "match": {"value": "red"}}]}}),
    "scroll_filter": lambda v: ("/points/scroll", {"limit": 10, "filter": {"must": [{"key": "city", "match": {"value": "c7"}}]}}),
    "count_exact": lambda v: ("/points/count", {"exact": True, "filter": {"must": [{"key": "color", "match": {"value": "red"}}]}}),
}
res = {}
for name, make in workloads.items():
    n = 1000 if name not in ("count_exact", "query_unindexed_filter") else 300
    a, b, ratios = [], [], []
    for i in range(n + 50):
        path, body = make(vectors[i % len(vectors)])
        order = [(ca, sa, a), (cb, sb, b)]
        if i % 2: order.reverse()          # alternate who goes first
        ts = {}
        for conn, suf, sink in order:
            ts[id(sink)] = call(conn, f"/collections/bench{path}{suf}", body)
        if i >= 50:                          # warmup
            a.append(ts[id(a)]); b.append(ts[id(b)]); ratios.append(ts[id(b)] / ts[id(a)])
    res[name] = {"a_median_ms": statistics.median(a), "b_median_ms": statistics.median(b),
                 "median_ratio_pct": (statistics.median(ratios) - 1) * 100,
                 "mean_ratio_pct": (statistics.fmean(ratios) - 1) * 100, "n": len(ratios)}
print(json.dumps({"label": label, "results": res}))
