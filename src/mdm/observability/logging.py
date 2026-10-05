"""Structured (JSON) logging with run-level correlation ids.

Every log line carries run_id / stage / source so that, in production, logs
shipped to Azure Monitor / Log Analytics can be joined to metrics and to
reconciliation exceptions with a single KQL query:

    MdmLogs_CL | where run_id_s == "<run>" and level_s == "ERROR"
"""
from __future__ import annotations

import contextvars
import json
import logging
import sys
from datetime import datetime, timezone

_run_ctx: contextvars.ContextVar[dict] = contextvars.ContextVar("mdm_run_ctx", default={})


def set_run_context(**kwargs) -> None:
    ctx = dict(_run_ctx.get())
    ctx.update({k: v for k, v in kwargs.items() if v is not None})
    _run_ctx.set(ctx)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            **_run_ctx.get(),
        }
        extra = getattr(record, "fields", None)
        if extra:
            payload.update(extra)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ctx = _run_ctx.get()
        stage = ctx.get("stage", "-")
        extra = getattr(record, "fields", None) or {}
        tail = " ".join(f"{k}={v}" for k, v in extra.items())
        return f"{record.levelname:<7} [{stage:<14}] {record.getMessage()} {tail}".rstrip()


class _Adapter(logging.LoggerAdapter):
    """logger.info("msg", rows=10) -> structured fields."""

    def process(self, msg, kwargs):
        fields = {k: kwargs.pop(k) for k in list(kwargs) if k not in ("exc_info", "stack_info", "stacklevel", "extra")}
        kwargs.setdefault("extra", {})["fields"] = fields
        return msg, kwargs


_configured = False


def configure_logging(level: str = "INFO", fmt: str = "json", logfile: str | None = None) -> None:
    global _configured
    root = logging.getLogger("mdm")
    root.handlers.clear()
    root.setLevel(level)
    formatter = JsonFormatter() if fmt == "json" else TextFormatter()
    h = logging.StreamHandler(sys.stderr)
    h.setFormatter(formatter)
    root.addHandler(h)
    if logfile:
        fh = logging.FileHandler(logfile, encoding="utf-8")
        fh.setFormatter(JsonFormatter())          # files are always JSON (machine-readable)
        root.addHandler(fh)
    root.propagate = False
    _configured = True


def get_logger(name: str) -> _Adapter:
    if not _configured:
        configure_logging()
    return _Adapter(logging.getLogger(f"mdm.{name}"), {})
