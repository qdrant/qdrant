#!/usr/bin/env python3
"""A read-only, S3-compatible-enough HTTP server over a local directory.

Speaks the two requests the wasm build makes: `GET /?list-type=2&prefix=…` returning a
ListObjectsV2 XML body, and `GET /<key>` returning the object. CORS headers are permissive so a
page served from anywhere can talk to it — which is exactly what a real bucket has to be
configured for.

    ./s3_stub.py <directory> [port]

The directory is served as if it were the bucket root, so the keys are the paths relative to it.
"""

import http.server
import os
import socketserver
import sys
import urllib.parse
from xml.sax.saxutils import escape

ROOT = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else ".")
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 9000


def keys_under(prefix):
    for dirpath, _dirnames, filenames in os.walk(ROOT):
        for name in filenames:
            full = os.path.join(dirpath, name)
            key = os.path.relpath(full, ROOT).replace(os.sep, "/")
            if key.startswith(prefix):
                yield key, os.path.getsize(full)


class Handler(http.server.BaseHTTPRequestHandler):
    def _send(self, status, body, content_type):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self._send(204, b"", "text/plain")

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)

        if query.get("list-type") == ["2"]:
            prefix = query.get("prefix", [""])[0]
            entries = "".join(
                f"<Contents><Key>{escape(key)}</Key><Size>{size}</Size></Contents>"
                for key, size in sorted(keys_under(prefix))
            )
            body = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                f"<Name>bucket</Name><Prefix>{escape(prefix)}</Prefix>"
                f"<IsTruncated>false</IsTruncated>{entries}"
                "</ListBucketResult>"
            ).encode()
            self._send(200, body, "application/xml")
            return

        key = urllib.parse.unquote(parsed.path.lstrip("/"))
        path = os.path.normpath(os.path.join(ROOT, key))
        if not path.startswith(ROOT) or not os.path.isfile(path):
            self._send(404, b"not found", "text/plain")
            return

        with open(path, "rb") as handle:
            self._send(200, handle.read(), "application/octet-stream")

    def log_message(self, *_args):
        pass


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    print(f"serving {ROOT} on http://localhost:{PORT}", flush=True)
    Server(("127.0.0.1", PORT), Handler).serve_forever()
