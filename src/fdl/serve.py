"""Local HTTP server for DuckLake artifacts (CORS + Range request support)."""

import os
import re
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

from fdl.console import console


class CORSRangeHandler(SimpleHTTPRequestHandler):
    """HTTP handler with CORS headers and Range request support."""

    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        super().end_headers()

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.end_headers()

    def do_HEAD(self) -> None:
        path = self.translate_path(self.path)
        if os.path.isfile(path):
            self.send_response(200)
            self.send_header("Content-Length", str(os.path.getsize(path)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
        else:
            super().do_HEAD()

    def do_GET(self) -> None:
        range_header = self.headers.get("Range")
        if not range_header:
            return super().do_GET()

        path = self.translate_path(self.path)
        try:
            f = open(path, "rb")  # noqa: SIM115
        except OSError:
            self.send_error(404)
            return

        size = os.fstat(f.fileno()).st_size
        m = re.match(r"bytes=(\d*)-(\d*)", range_header)
        if not m:
            f.close()
            return super().do_GET()

        start = int(m.group(1)) if m.group(1) else 0
        end = int(m.group(2)) if m.group(2) else size - 1
        end = min(end, size - 1)
        length = end - start + 1

        self.send_response(206)
        self.send_header("Content-Type", self.guess_type(path))
        self.send_header("Content-Length", str(length))
        self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        f.seek(start)
        self.wfile.write(f.read(length))
        f.close()


def run_server(directory: Path, port: int) -> None:
    """Start the HTTP server."""
    directory.mkdir(parents=True, exist_ok=True)
    handler = partial(CORSRangeHandler, directory=str(directory))
    server = HTTPServer(("", port), handler)
    console.print(f"Serving {directory} at [bold]http://localhost:{port}[/bold]")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        console.print("\nShutting down.")
        server.server_close()
