import requests
import sys


# Create points in peer's collection
r = requests.put(
   f"http://127.0.0.1:{sys.argv[2]}/collections/{sys.argv[1]}/points?wait=true", json={
       "points": [
           {
               "id": 1,
               "vector": [0.05, 0.61, 0.76, 0.74],
               "payload": {
                   "city": "Berlin",
                   "country": "Germany",
                   "count": 1000000,
                   "square": 12.5,
                   "coords": {"lat": 1.0, "lon": 2.0}
               }
           },
           {"id": 2, "vector": [0.19, 0.81, 0.75, 0.11],
               "payload": {"city": ["Berlin", "London"]}},
           {"id": 3, "vector": [0.36, 0.55, 0.47, 0.94],
               "payload": {"city": ["Berlin", "Moscow"]}},
           {"id": 4, "vector": [0.18, 0.01, 0.85, 0.80],
               "payload": {"city": ["London", "Moscow"]}},
           {"id": 5, "vector": [0.24, 0.18, 0.22,
                                0.44], "payload": {"count": [0]}},
           {"id": 6, "vector": [0.35, 0.08, 0.11, 0.44]}
       ]
   })
assert r.status_code == 200, r.text