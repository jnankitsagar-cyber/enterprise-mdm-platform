"""REST API for Golden Record access.

    GET /health
    GET /golden-records?email=&phone=&last_name=&limit=&offset=
    GET /golden-records/{golden_id}                  (follows MERGE redirects)
    GET /golden-records/{golden_id}/lineage          (full source->golden trace)
    GET /sources/{source_system}/{source_record_id}  (xref lookup: "which golden record is my CRM customer?")
    GET /reconciliation/latest

FastAPI is used when installed (OpenAPI docs at /docs). Without it,
`serve_stdlib` exposes the same routes with Python's standard library so the
demo has no hard dependency. In Azure this runs on Azure Container Apps /
App Service behind API Management (auth via Entra ID, rate limiting).
"""
from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from mdm.config import PlatformConfig
from mdm.serving.repository import GoldenRecordRepository


def create_app(cfg: PlatformConfig):
    from fastapi import FastAPI, HTTPException, Query

    repo = GoldenRecordRepository(cfg)
    app = FastAPI(title="Enterprise MDM - Golden Record API", version="1.0.0")

    @app.get("/health")
    def health():
        return {"status": "ok", **repo.stats()}

    @app.get("/golden-records")
    def search(email: str | None = None, phone: str | None = None, last_name: str | None = None,
               limit: int = Query(50, le=500), offset: int = 0):
        return repo.search(email, phone, last_name, limit, offset)

    @app.get("/golden-records/{golden_id}")
    def get(golden_id: str):
        r = repo.get(golden_id)
        if not r:
            raise HTTPException(404, "golden record not found")
        return r

    @app.get("/golden-records/{golden_id}/lineage")
    def lineage(golden_id: str):
        r = repo.lineage(golden_id)
        if not r:
            raise HTTPException(404, "golden record not found")
        return r

    @app.get("/sources/{source_system}/{source_record_id}")
    def by_source(source_system: str, source_record_id: str):
        r = repo.by_source(source_system, source_record_id)
        if not r:
            raise HTTPException(404, "source record not mastered")
        return r

    @app.get("/reconciliation/latest")
    def recon():
        return repo.latest_reconciliation() or {}

    return app


def route(repo: GoldenRecordRepository, path: str, query: dict) -> tuple[int, object]:
    """Framework-free router shared by the stdlib server and tests."""
    q = {k: v[0] for k, v in query.items()}
    if path == "/health":
        return 200, {"status": "ok", **repo.stats()}
    if path == "/golden-records":
        return 200, repo.search(q.get("email"), q.get("phone"), q.get("last_name"),
                                min(int(q.get("limit", 50)), 500), int(q.get("offset", 0)))
    if m := re.fullmatch(r"/golden-records/([^/]+)/lineage", path):
        r = repo.lineage(m.group(1))
        return (200, r) if r else (404, {"detail": "golden record not found"})
    if m := re.fullmatch(r"/golden-records/([^/]+)", path):
        r = repo.get(m.group(1))
        return (200, r) if r else (404, {"detail": "golden record not found"})
    if m := re.fullmatch(r"/sources/([^/]+)/([^/]+)", path):
        r = repo.by_source(m.group(1), m.group(2))
        return (200, r) if r else (404, {"detail": "source record not mastered"})
    if path == "/reconciliation/latest":
        return 200, repo.latest_reconciliation() or {}
    return 404, {"detail": "not found"}


def serve_stdlib(cfg: PlatformConfig, host: str = "127.0.0.1", port: int = 8000) -> None:
    repo = GoldenRecordRepository(cfg)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            u = urlparse(self.path)
            status, body = route(repo, u.path.rstrip("/") or "/", parse_qs(u.query))
            data = json.dumps(body, default=str, indent=2).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *a):
            pass

    print(f"Golden Record API (stdlib) on http://{host}:{port}  - try /golden-records?last_name=Johnson")
    ThreadingHTTPServer((host, port), Handler).serve_forever()


def serve(cfg: PlatformConfig, host: str = "127.0.0.1", port: int = 8000) -> None:
    try:
        import uvicorn
        app = create_app(cfg)
        print(f"Golden Record API (FastAPI) on http://{host}:{port}/docs")
        uvicorn.run(app, host=host, port=port)
    except ImportError:
        serve_stdlib(cfg, host, port)
