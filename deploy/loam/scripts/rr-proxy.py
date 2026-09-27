#!/usr/bin/env python3
"""Round-robin HTTP proxy: spreads each POST across several Resonate servers
that share one store, so a single conctrace run exercises cross-replica
reads. Usage: rr-proxy.py LISTEN_PORT BACKEND_URL [BACKEND_URL ...]"""
import itertools
import sys
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

port = int(sys.argv[1])
backends = itertools.cycle(sys.argv[2:])
lock = threading.Lock()


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _forward(self, method):
        with lock:
            base = next(backends)
        n = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(n) if n else None
        req = urllib.request.Request(base.rstrip("/") + self.path, data=body, method=method)
        req.add_header("content-type", self.headers.get("content-type", "application/json"))
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                data, code = r.read(), r.status
        except urllib.error.HTTPError as e:
            data, code = e.read(), e.code
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        self._forward("POST")

    def do_GET(self):
        self._forward("GET")


ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
