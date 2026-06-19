"""
GMGN Scanner - Reverse Proxy (v4 - SSE streaming support)
Serves static files on port 3000, proxies /api/* to backend on port 8000.
"""

import http.server
import urllib.request
import os
import mimetypes
import socketserver

BACKEND = "http://127.0.0.1:8000"
PUBLIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "public")

class RobustHTTPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

class ProxyHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        if self.path.startswith("/api/"):
            self._proxy()
        else:
            self._serve_static()

    def _serve_static(self):
        path = self.path.split("?")[0]
        if path == "/":
            path = "/index.html"
        filepath = os.path.join(PUBLIC_DIR, path.lstrip("/"))
        filepath = os.path.normpath(filepath)
        if not filepath.startswith(PUBLIC_DIR):
            self.send_error(403)
            return
        if not os.path.isfile(filepath):
            self.send_error(404)
            return
        mime, _ = mimetypes.guess_type(filepath)
        if not mime:
            mime = "application/octet-stream"
        try:
            with open(filepath, "rb") as f:
                data = f.read()
            self.send_response(200)
            self.send_header("Content-Type", mime)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, Exception):
            pass

    def _proxy(self):
        url = BACKEND + self.path
        is_stream = "/api/stream" in self.path

        try:
            req = urllib.request.Request(url)
            resp = urllib.request.urlopen(req, timeout=30 if not is_stream else None)

            self.send_response(200)
            ct = resp.headers.get("Content-Type", "application/json")
            self.send_header("Content-Type", ct)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "no-cache")
            if is_stream:
                self.send_header("X-Accel-Buffering", "no")  # disable nginx buffering
                self.send_header("Connection", "keep-alive")
            self.end_headers()

            if is_stream:
                # Stream SSE line by line — no buffering
                while True:
                    line = resp.readline()
                    if not line:
                        break
                    try:
                        self.wfile.write(line)
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError):
                        break
            else:
                data = resp.read()
                self.wfile.write(data)

        except urllib.error.HTTPError as e:
            try:
                err = f'{{"error":"{e.code} {e.reason}"}}'.encode()
                self.send_response(e.code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(err)))
                self.end_headers()
                self.wfile.write(err)
            except (BrokenPipeError, Exception):
                pass
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            try:
                err = f'{{"error":"{str(e)}"}}'.encode()
                self.send_response(502)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(err)))
                self.end_headers()
                self.wfile.write(err)
            except (BrokenPipeError, Exception):
                pass

if __name__ == "__main__":
    server = RobustHTTPServer(("0.0.0.0", 3000), ProxyHandler)
    print("Proxy on :3000 → static + API proxy to :8000 (SSE streaming)", flush=True)
    server.serve_forever()
