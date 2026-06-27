# s3_proxy

Serve a **local directory as S3**, with a **bandwidth + latency cap** that
simulates a production network — so you can exercise S3-backed code paths
locally instead of hitting a real remote bucket over a slow link.

Self-contained shell tool (`s3_proxy.sh`) driving a Docker container.

## Quick start

```bash
cd lib/edge/tools/s3_proxy

./s3_proxy.sh up                  # start server + 10gbit cap
./s3_proxy.sh status              # container + qdisc + bucket contents
./s3_proxy.sh bench               # measure throughput (auto-creates a 2GB object)
./s3_proxy.sh stage /path/to/dir my-prefix   # copy a dir into the bucket

./s3_proxy.sh throttle 1gbit              # rate only
./s3_proxy.sh throttle 10gbit 20ms        # rate + 20ms latency
./s3_proxy.sh throttle 10gbit 20ms 5ms    # + 5ms jitter
./s3_proxy.sh unthrottle                  # line rate

./s3_proxy.sh down                # stop & remove the container
```

Files under `data/<bucket>/<key>` are served as `http://localhost:9000/<bucket>/<key>` (default
bucket `test-bucket`).

## Throttle notes

`throttle RATE [DELAY] [JITTER]` builds `tbf` (rate) → `netem` (delay) on the
container egress. `tbf` is accurate at 10 Gbit; netem's own `rate` is not, so
it's used only for delay. Measured: `10gbit` → ~9.6 Gbit/s, `1gbit` → ~0.96,
unshaped → 15–33 — the cap binds.

- Delay is on the **server's egress** (download direction / each GET response),
  so it's **per-request** — it bites hardest on many-small-GET workloads.
- A large `DELAY` lowers throughput on short transfers (TCP slow-start over the
  bandwidth-delay product); the rate cap is still the ceiling. Only long
  transfers reach line rate — realistic high-BDP behaviour.

The cap is on the container's egress, so it applies to any client, local or
remote (reach the server via the published port, e.g. `host.docker.internal:9000`
from another container).

## Config (env overrides)

`DATA_DIR` (default `./data`), `BUCKET` (`test-bucket`), `PORT` (`9000`),
`RATE_DEFAULT` (`10gbit`), `LATENCY_DEFAULT` (`50ms`) `CONTAINER` (`s3-proxy`), `IFACE` (`eth0`),
`S3PROXY_IMAGE`, `NETSHOOT_IMAGE`.

## Requirements

Docker, pulls `andrewgaul/s3proxy` and `nicolaka/netshoot` on
first use.
