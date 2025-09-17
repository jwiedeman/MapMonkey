import argparse
import json
import os
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

from db import (
    count_businesses,
    fetch_recent_businesses,
    get_dsn,
    get_storage,
    init_db,
)


class DashboardDataSource:
    def __init__(self, dsn: Optional[str], storage: Optional[str]) -> None:
        self.storage = get_storage(storage)
        self.dsn = get_dsn(dsn)
        self._lock = threading.Lock()
        self._conn = None
        self._cache: Dict[str, Dict[str, Any]] = {}
        if self.storage != "csv":
            try:
                self._conn = init_db(self.dsn, storage=self.storage)
            except Exception:
                self._conn = None

    def _cache_get(self, key: str, ttl: float) -> Optional[Any]:
        entry = self._cache.get(key)
        if not entry:
            return None
        if time.monotonic() - entry["ts"] > ttl:
            return None
        return entry["value"]

    def _cache_set(self, key: str, value: Any) -> None:
        self._cache[key] = {"value": value, "ts": time.monotonic()}

    def get_total(self) -> Optional[int]:
        with self._lock:
            cached = self._cache_get("total", 5.0)
            if cached is not None:
                return cached
            total = None
            if self.storage == "csv":
                path = Path(self.dsn)
                if path.exists():
                    with path.open() as f:
                        total = max(sum(1 for _ in f) - 1, 0)
                else:
                    total = 0
            elif self._conn is not None:
                total = count_businesses(self._conn, storage=self.storage)
            self._cache_set("total", total)
            return total

    def get_recent(self, limit: int) -> list[dict[str, Any]]:
        with self._lock:
            cached = self._cache_get(f"recent:{limit}", 2.0)
            if cached is not None:
                return cached
            if self.storage == "csv":
                path = Path(self.dsn)
                if not path.exists():
                    recent: list[dict[str, Any]] = []
                else:
                    recent = fetch_recent_businesses(path, limit, storage=self.storage)
            elif self._conn is not None:
                recent = fetch_recent_businesses(self._conn, limit, storage=self.storage)
            else:
                recent = []
            self._cache_set(f"recent:{limit}", recent)
            return recent


class DashboardHandler(BaseHTTPRequestHandler):
    state_file: Path
    dashboard_path: Path
    data_source: DashboardDataSource

    def _set_headers(self, status: HTTPStatus = HTTPStatus.OK, *, content_type: str = "application/json") -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()

    def _send_json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload).encode("utf-8")
        self._set_headers(status)
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._serve_dashboard()
        elif parsed.path == "/api/summary":
            self._serve_summary()
        elif parsed.path == "/api/recent":
            params = parse_qs(parsed.query)
            limit = int(params.get("limit", [25])[0])
            self._serve_recent(limit)
        else:
            self.send_error(HTTPStatus.NOT_FOUND)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003, D401
        # Quiet handler logs to keep console clean.
        return

    def _serve_dashboard(self) -> None:
        try:
            with self.dashboard_path.open("rb") as f:
                content = f.read()
        except FileNotFoundError:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        self._set_headers(HTTPStatus.OK, content_type="text/html; charset=utf-8")
        self.wfile.write(content)

    def _read_state(self) -> Dict[str, Any]:
        try:
            with self.state_file.open() as f:
                return json.load(f)
        except Exception:
            return {}

    def _serve_summary(self) -> None:
        state = self._read_state()
        total = self.data_source.get_total()
        now = time.time()
        workers = []
        stuck_threshold = float(os.environ.get("MAPMONKEY_STUCK_THRESHOLD", "180"))
        for ident, info in state.get("workers", {}).items():
            heartbeat = info.get("heartbeat")
            stuck = bool(heartbeat and now - heartbeat > stuck_threshold)
            workers.append({
                "id": ident,
                "city": info.get("city"),
                "term": info.get("term"),
                "assigned_at": info.get("assigned_at"),
                "heartbeat": heartbeat,
                "stuck": stuck,
            })

        batch = state.get("batch", {})
        response = {
            "overall": {
                "progress": state.get("overall_progress", 0),
                "total": state.get("overall_total", 0),
                "city_index": state.get("city_index", 0),
                "total_cities": state.get("total_cities", 0),
                "term_index": state.get("term_index", 0),
                "total_terms": state.get("total_terms", 0),
                "current_city": state.get("current_city"),
            },
            "batch": {
                "fill": batch.get("fill", 0),
                "total": batch.get("total", 0),
                "worker": batch.get("worker"),
            },
            "workers": workers,
            "alerts": state.get("alerts", []),
            "events": state.get("events", []),
            "metrics": state.get("metrics", {}),
            "recent_businesses": state.get("recent_businesses", []),
            "database": {
                "storage": self.data_source.storage,
                "total": total,
            },
        }
        self._send_json(response)

    def _serve_recent(self, limit: int) -> None:
        recent = self.data_source.get_recent(limit)
        self._send_json({"results": recent})


def serve(args: argparse.Namespace) -> None:
    handler = DashboardHandler
    handler.state_file = Path(args.state_file)
    handler.dashboard_path = Path(args.dashboard)
    handler.data_source = DashboardDataSource(args.dsn, args.store)

    server = ThreadingHTTPServer((args.host, args.port), handler)
    print(f"Dashboard server running on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:  # pragma: no cover - manual stop
        pass
    finally:
        server.server_close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve the MapMonkey monitoring dashboard")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--state-file", default="run_state.json")
    parser.add_argument("--dashboard", default="dashboard.html")
    parser.add_argument("--dsn", help="Optional DSN/path override for data source")
    parser.add_argument("--store", help="Storage backend override")
    return parser.parse_args()


if __name__ == "__main__":
    serve(parse_args())
