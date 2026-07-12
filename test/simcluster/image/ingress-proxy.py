#!/usr/bin/env python3
# ingress-proxy.py — S0-ingress: a tiny Python-stdlib HTTPS reverse proxy that TERMINATES TLS on a
# broker-hostname-addressed port and forwards to the broker's LOOPBACK product listener — exactly what the
# production Caddy front does (install.sh writes `$DOMAIN:443 { reverse_proxy ... }`). It runs as a SIDECAR
# sharing the broker container's network namespace (`docker run --network container:<brk>`), so it can reach
# the broker's 127.0.0.1:<manifest_listen> while being addressable as `https://<brk>:443` from peer
# containers on the docker bridge (a plain bridge container cannot reach another container's 127.0.0.1 —
# verified live 2026-07-11). The product listeners (well-known manifest / /sub) STAY loopback-only,
# fail-closed, and are NEVER rebound or weakened (roadmap OQ-5): this front only relays bytes to them.
#
# It is NOT the product and does NOT weaken any security posture: the manifest is still built + account-
# signed by the broker, fetched + verified by the agent's own Go-x509 fetcher against the OOB-pinned
# account (AdoptDecision). This sidecar only provides the operator's TLS-terminating front (Mandate ③).
#
# fail-loud: any bind / cert-load / route-parse error exits non-zero so the drill's readiness poll sees a
# dead front instead of a silent partial one.
#
# SCOPE (S2 external-review recommendation): this is TEST INFRASTRUCTURE, not a hardened general reverse
# proxy. It is scoped to explicitly-configured LOOPBACK routes serving the bounded GET-only well-known
# manifest (and a future bounded /sub response). Do NOT reuse it for streaming, untrusted request bodies
# without size limits, or as a public-facing proxy — production fronts the loopback listeners with Caddy.
#
# Usage:
#   ingress-proxy.py --listen :443 --cert <pem> --key <pem> \
#       --route /.well-known/tether/=127.0.0.1:7480 [--route /sub/=127.0.0.1:8090 ...]
import argparse
import http.client
import ssl
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROUTES = []  # list of (path_prefix, backend_host, backend_port), longest-prefix-first


def pick_backend(path):
    for prefix, host, port in ROUTES:
        if path.startswith(prefix):
            return host, port
    return None


class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # keep journald quiet; errors still go to stderr via log_error
        pass

    def _proxy(self):
        backend = pick_backend(self.path)
        if backend is None:
            self.send_error(404, "no route for path")
            return
        host, port = backend
        length = int(self.headers.get("Content-Length", 0) or 0)
        body = self.rfile.read(length) if length else None
        try:
            conn = http.client.HTTPConnection(host, port, timeout=10)
            # forward the method + path + a minimal header set to the loopback backend.
            fwd_headers = {k: v for k, v in self.headers.items()
                           if k.lower() not in ("host", "connection")}
            fwd_headers["Host"] = "%s:%d" % (host, port)
            conn.request(self.command, self.path, body=body, headers=fwd_headers)
            resp = conn.getresponse()
            data = resp.read()
            self.send_response(resp.status)
            for k, v in resp.getheaders():
                if k.lower() in ("transfer-encoding", "connection", "content-length"):
                    continue
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            conn.close()
        except Exception as e:  # noqa: BLE001 — a dead backend must surface as a 502, not crash the front
            self.send_error(502, "backend error: %s" % e)

    do_GET = _proxy
    do_POST = _proxy
    do_HEAD = _proxy
    do_PUT = _proxy
    do_DELETE = _proxy


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", default=":443")
    ap.add_argument("--cert", required=True)
    ap.add_argument("--key", required=True)
    ap.add_argument("--route", action="append", default=[],
                    help="PATHPREFIX=HOST:PORT (repeatable)")
    args = ap.parse_args()

    for r in args.route:
        try:
            prefix, target = r.split("=", 1)
            host, port = target.rsplit(":", 1)
            ROUTES.append((prefix, host, int(port)))
        except ValueError:
            sys.stderr.write("ingress-proxy: bad --route %r (want PATHPREFIX=HOST:PORT)\n" % r)
            sys.exit(2)
    if not ROUTES:
        sys.stderr.write("ingress-proxy: at least one --route required\n")
        sys.exit(2)
    ROUTES.sort(key=lambda t: len(t[0]), reverse=True)  # longest prefix wins

    host, _, port = args.listen.rpartition(":")
    bind_host = host or "0.0.0.0"
    bind_port = int(port)

    try:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(certfile=args.cert, keyfile=args.key)
    except Exception as e:  # noqa: BLE001 — cert-load failure must be loud + fatal
        sys.stderr.write("ingress-proxy: cert load failed: %s\n" % e)
        sys.exit(1)

    try:
        httpd = ThreadingHTTPServer((bind_host, bind_port), ProxyHandler)
    except Exception as e:  # noqa: BLE001 — bind failure must be loud + fatal
        sys.stderr.write("ingress-proxy: bind %s:%d failed: %s\n" % (bind_host, bind_port, e))
        sys.exit(1)
    httpd.socket = ctx.wrap_socket(httpd.socket, server_side=True)
    sys.stderr.write("ingress-proxy: listening on %s:%d, routes=%s\n" % (bind_host, bind_port, ROUTES))
    sys.stderr.flush()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
