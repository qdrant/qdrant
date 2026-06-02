#!/usr/bin/env python3
"""
Slowly download a streaming shard snapshot from a given URL, to manually
reproduce sender-side backpressure against a *remote* node (where, unlike
localhost, a killed/stalled consumer does not produce a prompt RST).

The sender serves the shard snapshot at:
    GET /collections/{collection}/shards/{shard}/snapshot

While this client throttles (or stops) reading, the sender's snapshot writer
parks on the 4 KiB duplex while holding the SegmentHolder upgradable-read lock.
That lets you watch, on the remote node, whether/when it logs a broken pipe,
releases the lock, and whether concurrent ops / other transfers wedge.

Examples
--------
# Drip-read at ~20 KiB/s forever (Ctrl-C to stop):
  ./slow_snapshot_download.py http://NODE:6333/collections/c/shards/1/snapshot

# Read the first 1 MiB, then HOLD the connection open without reading
# (simulates a stalled/half-open receiver — connection stays ESTABLISHED):
  ./slow_snapshot_download.py URL --read-bytes 1MiB --then hold

# Read the first 1 MiB, then abruptly RST the socket
# (simulates the client process being kill -9'd / crashing):
  ./slow_snapshot_download.py URL --read-bytes 1MiB --then rst

# Read the first 1 MiB, then BLACKHOLE the connection: drop all packets so the
# sender gets no FIN/RST at all (simulates the machine suddenly going offline /
# a network partition). Needs root or passwordless sudo for iptables; if it
# can't, it prints the exact commands to run by hand. THIS is the 18h-hang case.
  sudo ./slow_snapshot_download.py URL --read-bytes 1MiB --then blackhole

# 8 KiB every 0.5s, with an API key, ignore TLS verification:
  ./slow_snapshot_download.py URL --chunk 8KiB --delay 0.5 --api-key KEY --insecure

# Open N concurrent slow downloads to pile up streams on the sender:
  ./slow_snapshot_download.py URL --concurrency 10 --then hold

Uses only the Python standard library (no requests), so it runs anywhere.
"""

import argparse
import socket
import ssl
import sys
import threading
import time
from urllib.parse import urlsplit


def parse_size(s: str) -> int:
    """Parse sizes like '4096', '8KiB', '1MiB', '2MB'."""
    s = s.strip()
    units = {
        "": 1,
        "b": 1,
        "kib": 1024,
        "kb": 1000,
        "k": 1024,
        "mib": 1024**2,
        "mb": 1000**2,
        "m": 1024**2,
        "gib": 1024**3,
        "gb": 1000**3,
        "g": 1024**3,
    }
    num = s
    unit = ""
    for i, ch in enumerate(s):
        if not (ch.isdigit() or ch == "."):
            num, unit = s[:i], s[i:].strip().lower()
            break
    if unit not in units:
        raise argparse.ArgumentTypeError(f"unknown size unit in {s!r}")
    return int(float(num) * units[unit])


def human(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024 or unit == "GiB":
            return f"{n:.1f}{unit}" if unit != "B" else f"{n}B"
        n /= 1024
    return f"{n}"


def open_socket(url: str, api_key: str | None, insecure: bool, timeout: float) -> socket.socket:
    """Send the GET request and return a connected socket positioned at the body."""
    parts = urlsplit(url)
    host = parts.hostname
    if host is None:
        raise ValueError(f"no host in URL {url!r}")
    is_tls = parts.scheme == "https"
    port = parts.port or (443 if is_tls else 80)
    path = parts.path or "/"
    if parts.query:
        path += "?" + parts.query

    raw = socket.create_connection((host, port), timeout=timeout)
    if is_tls:
        ctx = ssl.create_default_context()
        if insecure:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        sock = ctx.wrap_socket(raw, server_hostname=host)
    else:
        sock = raw

    req = [
        f"GET {path} HTTP/1.1",
        f"Host: {parts.netloc}",
        "User-Agent: slow-snapshot-download",
        "Accept: */*",
        "Connection: close",
    ]
    if api_key:
        req.append(f"Api-Key: {api_key}")
    req.append("")
    req.append("")
    sock.sendall("\r\n".join(req).encode())
    return sock


def drain_headers(sock: socket.socket, tag: str) -> bytes:
    """Read up to the end of HTTP headers; return any leftover body bytes."""
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("connection closed before headers completed")
        buf += chunk
    head, _, rest = buf.partition(b"\r\n\r\n")
    status_line = head.split(b"\r\n", 1)[0].decode(errors="replace")
    print(f"[{tag}] {status_line}")
    if " 200 " not in status_line:
        # Print a few header lines for context on non-200.
        for line in head.split(b"\r\n")[1:8]:
            print(f"[{tag}]   {line.decode(errors='replace')}")
    return rest


def download(idx: int, args) -> None:
    tag = f"dl{idx}"
    started = time.time()
    total = 0
    try:
        sock = open_socket(args.url, args.api_key, args.insecure, args.connect_timeout)
    except Exception as e:  # noqa: BLE001
        print(f"[{tag}] connect failed: {e}")
        return

    try:
        leftover = drain_headers(sock, tag)
        total += len(leftover)

        target = args.read_bytes  # None => read forever (throttled)
        last_report = started
        # Use a recv timeout so we can periodically report even when the
        # server stops sending (backpressure -> our recv blocks).
        sock.settimeout(args.delay if args.delay > 0 else 5.0)

        while target is None or total < target:
            if args.delay > 0:
                time.sleep(args.delay)
            try:
                chunk = sock.recv(args.chunk)
            except socket.timeout:
                # No data within delay window — server is backpressured/stalled.
                now = time.time()
                if now - last_report >= 1.0:
                    print(f"[{tag}] (no data) total={human(total)} elapsed={now-started:.1f}s")
                    last_report = now
                continue
            if not chunk:
                print(f"[{tag}] server closed stream: total={human(total)} "
                      f"elapsed={time.time()-started:.1f}s")
                return
            total += len(chunk)
            now = time.time()
            if now - last_report >= 1.0:
                rate = total / max(now - started, 1e-9)
                print(f"[{tag}] read={human(total)} rate={human(int(rate))}/s "
                      f"elapsed={now-started:.1f}s")
                last_report = now

        # Reached the read target: decide what to do next.
        print(f"[{tag}] reached read target {human(total)} after {time.time()-started:.1f}s "
              f"-> action={args.then}")
        if args.then in ("rst", "close"):
            # Abortive close: kernel sends RST, no graceful FIN.
            # This is what a kill -9 / process crash looks like to the sender.
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, _linger_zero())
            except OSError:
                pass
            sock.close()
            print(f"[{tag}] socket RST (abortive close) — like kill -9 / crash")
        elif args.then == "fin":
            sock.close()
            print(f"[{tag}] socket closed (graceful FIN)")
        elif args.then == "blackhole":
            # Drop all packets on this connection so the sender receives no
            # FIN/RST — as if the machine vanished / the network partitioned.
            cleanup = _blackhole(sock, tag)
            print(f"[{tag}] connection BLACKHOLED, sender gets silence. "
                  f"Watch the remote node's logs. Ctrl-C to restore.")
            try:
                while True:
                    time.sleep(3600)
            finally:
                if cleanup:
                    cleanup()
        else:  # hold
            print(f"[{tag}] HOLDING connection open (still ESTABLISHED), not reading. "
                  f"Watch the remote node's logs. Ctrl-C to release.")
            while True:
                time.sleep(3600)
    except KeyboardInterrupt:
        print(f"[{tag}] interrupted, closing")
    finally:
        try:
            sock.close()
        except OSError:
            pass


def _linger_zero() -> bytes:
    import struct
    return struct.pack("ii", 1, 0)


def _blackhole(sock: socket.socket, tag: str):
    """Drop every packet on this TCP connection so the remote sender receives
    no FIN/RST — simulating the machine suddenly going offline / a network
    partition. Returns a cleanup callable (or None if rules couldn't be set).

    Linux only; needs root or passwordless sudo. Falls back to printing the
    exact iptables commands so they can be run by hand.
    """
    import os
    import shutil
    import subprocess

    try:
        laddr = sock.getsockname()
        raddr = sock.getpeername()
    except OSError as e:
        print(f"[{tag}] cannot read socket addresses for blackhole: {e}")
        return None

    lport = laddr[1]
    rip, rport = raddr[0], raddr[1]
    ipt = "ip6tables" if sock.family == socket.AF_INET6 else "iptables"

    out_match = ["-p", "tcp", "-d", rip, "--dport", str(rport), "--sport", str(lport), "-j", "DROP"]
    in_match = ["-p", "tcp", "-s", rip, "--sport", str(rport), "--dport", str(lport), "-j", "DROP"]
    adds = [[ipt, "-I", "OUTPUT", *out_match], [ipt, "-I", "INPUT", *in_match]]
    dels = [[ipt, "-D", "OUTPUT", *out_match], [ipt, "-D", "INPUT", *in_match]]

    prefix: list[str] = []
    if hasattr(os, "geteuid") and os.geteuid() != 0:
        if shutil.which("sudo"):
            prefix = ["sudo", "-n"]

    def show_manual():
        print(f"[{tag}] run these to blackhole, and the matching -D to restore:")
        for cmd in adds:
            print("    sudo " + " ".join(cmd))

    if shutil.which(ipt) is None:
        print(f"[{tag}] {ipt} not found (Linux only); cannot blackhole automatically.")
        show_manual()
        return None

    for cmd in adds:
        res = subprocess.run(prefix + cmd, capture_output=True, text=True)
        if res.returncode != 0:
            print(f"[{tag}] failed to install rule ({res.stderr.strip() or 'permission denied'}).")
            show_manual()
            # Best-effort: try to undo whatever we managed to add.
            for d in dels:
                subprocess.run(prefix + d, capture_output=True, text=True)
            return None

    print(f"[{tag}] BLACKHOLE active: all packets to/from {rip}:{rport} (our port {lport}) dropped.")

    def cleanup():
        for d in dels:
            subprocess.run(prefix + d, capture_output=True, text=True)
        print(f"[{tag}] blackhole rules removed")

    return cleanup


def main() -> None:
    p = argparse.ArgumentParser(
        description="Slowly download a streaming shard snapshot to test sender backpressure.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("url", help="snapshot URL, e.g. http://NODE:6333/collections/c/shards/1/snapshot")
    p.add_argument("--chunk", type=parse_size, default=4096,
                   help="bytes per recv() (default 4KiB)")
    p.add_argument("--delay", type=float, default=0.2,
                   help="seconds to sleep between reads (default 0.2 => ~20KiB/s)")
    p.add_argument("--read-bytes", type=parse_size, default=None,
                   help="stop reading after this many bytes, then run --then "
                        "(default: read forever, throttled)")
    p.add_argument("--then", choices=("hold", "rst", "close", "fin", "blackhole"), default="hold",
                   help="after --read-bytes: "
                        "'hold' keep connection ESTABLISHED w/o reading (stalled/half-open); "
                        "'rst' (alias 'close') abortive RST, like the client kill -9'd / crashed; "
                        "'fin' graceful close; "
                        "'blackhole' drop all packets so the sender gets no FIN/RST, like the "
                        "machine went offline (Linux, needs root/sudo for iptables). "
                        "(default hold)")
    p.add_argument("--concurrency", type=int, default=1,
                   help="number of parallel downloads to open (default 1)")
    p.add_argument("--api-key", default=None, help="value for the Api-Key header")
    p.add_argument("--insecure", action="store_true", help="skip TLS certificate verification")
    p.add_argument("--connect-timeout", type=float, default=10.0,
                   help="TCP connect timeout seconds (default 10)")
    args = p.parse_args()

    print(f"target: {args.url}")
    print(f"chunk={human(args.chunk)} delay={args.delay}s "
          f"read_bytes={human(args.read_bytes) if args.read_bytes else 'unlimited'} "
          f"then={args.then} concurrency={args.concurrency}")

    if args.concurrency == 1:
        download(0, args)
        return

    threads = [threading.Thread(target=download, args=(i, args), daemon=True)
               for i in range(args.concurrency)]
    for t in threads:
        t.start()
    try:
        while any(t.is_alive() for t in threads):
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("interrupted, exiting (daemon downloads will be torn down)")


if __name__ == "__main__":
    main()
