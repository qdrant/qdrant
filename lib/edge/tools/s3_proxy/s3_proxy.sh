#!/usr/bin/env bash
#
# s3_proxy.sh — serve a local directory as S3, with a bandwidth + latency cap
# that simulates a production network. Lets you exercise S3-backed code paths
# locally instead of hitting a real remote bucket over a slow link.
#
#   * S3 server : s3proxy (filesystem backend) in a Docker container. Serves
#                 $DATA_DIR/<bucket>/<key> directly as S3 objects, so you stage
#                 data with plain `cp` — no mc/aws/rclone needed.
#   * Throttle  : Linux `tc` on the container's eth0 egress (server->client =
#                 the download direction), via a netshoot sidecar sharing the
#                 container's net namespace. `tbf` caps bandwidth (accurate at
#                 10gbit, where macOS dnctl/dummynet is not); `netem` adds
#                 latency. No host sudo — only --cap-add NET_ADMIN on the sidecar.
#
# Measured: `rate 10gbit` -> ~9.6 Gbit/s, `rate 1gbit` -> ~0.96 Gbit/s, unshaped
# loopback 15-33 Gbit/s — so the cap genuinely binds for bulk bytes.
#
# Usage:
#   ./s3_proxy.sh up [RATE]               start server + apply throttle (default 10gbit)
#   ./s3_proxy.sh down                    stop & remove the server container
#   ./s3_proxy.sh throttle RATE [DELAY]   bandwidth cap + optional latency,
#                                         e.g. throttle 10gbit 20ms  /  throttle 1gbit
#   ./s3_proxy.sh unthrottle              remove the cap (line rate)
#   ./s3_proxy.sh status                  container + qdisc + bucket listing
#   ./s3_proxy.sh stage SRC [PREFIX]      copy a local dir into the bucket under PREFIX
#   ./s3_proxy.sh bench [KEY]             measure download throughput (default test-bucket/bigfile)
#
set -euo pipefail

# ---- config (override via env) ---------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${DATA_DIR:-$SCRIPT_DIR/data}"   # absolute: the bucket store, served as S3
CONTAINER="${CONTAINER:-s3-proxy}"
PORT="${PORT:-9000}"
BUCKET="${BUCKET:-test-bucket}"
RATE_DEFAULT="${RATE_DEFAULT:-10gbit}"
LATENCY_DEFAULT="${LATENCY_DEFAULT:-50ms}"
IFACE="${IFACE:-eth0}"
S3PROXY_IMAGE="${S3PROXY_IMAGE:-andrewgaul/s3proxy:latest}"
NETSHOOT_IMAGE="${NETSHOOT_IMAGE:-nicolaka/netshoot:latest}"
ENDPOINT="http://localhost:${PORT}"

log() { printf '\033[33m# %s\033[0m\n' "$*"; }
err() { printf '\033[31m! %s\033[0m\n' "$*" >&2; }

# Run a `tc` command inside the server container's net namespace.
tc_ns() { docker run --rm --net "container:${CONTAINER}" --cap-add NET_ADMIN "$NETSHOOT_IMAGE" tc "$@"; }

cmd_up() {
  local rate="${1:-$RATE_DEFAULT}"
  local latency="${2:-$LATENCY_DEFAULT}"
  mkdir -p "$DATA_DIR/$BUCKET"
  chmod -R a+rX "$DATA_DIR"
  if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    log "removing existing container $CONTAINER"
    docker rm -f "$CONTAINER" >/dev/null
  fi
  log "starting s3proxy ($BUCKET <- $DATA_DIR) on $ENDPOINT"
  docker run -d --name "$CONTAINER" --cap-add NET_ADMIN \
    -p "${PORT}:80" \
    -v "$DATA_DIR:/data" \
    -e S3PROXY_AUTHORIZATION=none \
    -e S3PROXY_ENDPOINT=http://0.0.0.0:80 \
    -e JCLOUDS_PROVIDER=filesystem \
    -e JCLOUDS_FILESYSTEM_BASEDIR=/data \
    "$S3PROXY_IMAGE" >/dev/null
  local i code
  for i in $(seq 1 30); do
    code=$(curl -s -o /dev/null -w '%{http_code}' "$ENDPOINT/" 2>/dev/null || true)
    [ "$code" = "200" ] || [ "$code" = "403" ] && { log "s3proxy ready (HTTP $code)"; break; }
    sleep 1
  done
  cmd_throttle "$rate" "$latency"
}

cmd_down() {
  docker rm -f "$CONTAINER" >/dev/null 2>&1 && log "removed $CONTAINER" || log "$CONTAINER not running"
}

# throttle RATE [DELAY] [JITTER]
#   RATE   bandwidth cap (tbf), e.g. 10gbit / 1gbit / 200mbit
#   DELAY  optional one-way egress latency (netem), e.g. 20ms / 50ms. Omit/0 = none.
#   JITTER optional +/- variation on DELAY, e.g. 5ms (requires DELAY)
#
# Built as tbf(root, rate) -> netem(child, delay): tbf is accurate at 10gbit
# (netem's own `rate` is not), netem adds latency on top. Note: a large DELAY
# lowers achievable throughput on short transfers (TCP slow-start over the
# bandwidth-delay product) — that's realistic link behaviour, the cap is still
# the ceiling. The delay is on the server's egress (server->client = download).
cmd_throttle() {
  local rate="${1:-$RATE_DEFAULT}"
  local delay="${2:-}"
  local jitter="${3:-}"
  # burst ~ rate/HZ; 10mb is generous and reaches 10gbit cleanly. Scale down for low rates.
  local burst="10mb"
  case "$rate" in
    *mbit|*mbps) burst="1mb" ;;
    *kbit) burst="64kb" ;;
  esac
  tc_ns qdisc del dev "$IFACE" root 2>/dev/null || true
  tc_ns qdisc add dev "$IFACE" root handle 1: tbf rate "$rate" burst "$burst" latency 100ms
  if [ -n "$delay" ] && [ "$delay" != "0" ] && [ "$delay" != "0ms" ]; then
    # limit 100000 pkts (~150MB) holds the bandwidth-delay product up to ~10gbit*100ms.
    tc_ns qdisc add dev "$IFACE" parent 1:1 handle 10: netem delay "$delay" ${jitter:+$jitter} limit 100000
    log "throttle: rate $rate  +  latency $delay${jitter:+ +/-$jitter} (egress) on $IFACE"
  else
    log "throttle: rate $rate (no added latency) on $IFACE"
  fi
  tc_ns qdisc show dev "$IFACE" | sed 's/^/  /'
}

cmd_unthrottle() {
  tc_ns qdisc del dev "$IFACE" root 2>/dev/null || true
  log "throttle removed (line rate)"
}

cmd_status() {
  log "container:"; docker ps --filter "name=$CONTAINER" --format '  {{.Names}}  {{.Status}}  {{.Ports}}' || true
  log "qdisc:";     tc_ns qdisc show dev "$IFACE" 2>/dev/null | sed 's/^/  /' || echo "  (server down)"
  local xml count
  xml=$(curl -s "$ENDPOINT/$BUCKET/?list-type=2" 2>/dev/null || true)
  count=$(printf '%s' "$xml" | tr '<' '\n' | grep -c '^Key>' || true)
  log "bucket $BUCKET: ${count:-0} object(s)"
  printf '%s' "$xml" | tr '<' '\n' | sed -n 's/^Key>\(.*\)/  - \1/p' | head -8
  [ "${count:-0}" -gt 8 ] 2>/dev/null && echo "  ... and $((count - 8)) more"
}

# Copy a local directory into the bucket under PREFIX (default: SRC's basename),
# so its files are served as S3 objects at <bucket>/<prefix>/...
cmd_stage() {
  local src="${1:?usage: stage SRC [PREFIX]}"
  local prefix="${2:-$(basename "$src")}"
  [ -d "$src" ] || { err "SRC '$src' is not a directory"; exit 1; }
  local dst="$DATA_DIR/$BUCKET/$prefix"
  log "staging $src  ->  $BUCKET/$prefix"
  mkdir -p "$dst"
  cp -a "$src"/. "$dst"/
  chmod -R a+rX "$DATA_DIR"
  log "staged $(find "$dst" -type f | wc -l | tr -d ' ') file(s) under $BUCKET/$prefix"
}

cmd_bench() {
  local key="${1:-$BUCKET/bigfile}"
  # Auto-create the default 2GB sparse test object if it's missing (data/ is gitignored).
  if [ "$key" = "$BUCKET/bigfile" ] && [ ! -f "$DATA_DIR/$key" ]; then
    log "creating 2GB sparse test object $key"
    mkdir -p "$(dirname "$DATA_DIR/$key")"
    if command -v mkfile >/dev/null 2>&1; then mkfile -n 2g "$DATA_DIR/$key"; else truncate -s 2G "$DATA_DIR/$key"; fi
    chmod a+r "$DATA_DIR/$key"
  fi
  log "downloading $key to measure throughput through the throttle..."
  curl -s -o /dev/null -w '%{speed_download} %{size_download}\n' "$ENDPOINT/$key" \
    | awk '{printf "  %.2f Gbit/s  (%.0f MB/s,  %.0f MB transferred)\n", $1*8/1e9, $1/1048576, $2/1048576}' \
    2>/dev/null || err "download failed (does $key exist?)"
}

case "${1:-}" in
  up)         shift; cmd_up "$@" ;;
  down)       shift; cmd_down "$@" ;;
  throttle)   shift; cmd_throttle "$@" ;;
  unthrottle) shift; cmd_unthrottle "$@" ;;
  status)     shift; cmd_status "$@" ;;
  stage)      shift; cmd_stage "$@" ;;
  bench)      shift; cmd_bench "$@" ;;
  # print the leading comment block as help
  *) awk 'NR>1 && /^#/{sub(/^# ?/,""); print; next} NR>1{exit}' "$0"; exit 1 ;;
esac
