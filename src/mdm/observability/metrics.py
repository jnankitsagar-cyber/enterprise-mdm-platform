"""Run metrics: counters, gauges and stage timings.

Exported as JSON (consumed by the dashboard + reconciliation) and as the
Prometheus text format. In Azure the same values are pushed to Azure Monitor
custom metrics / Application Insights; alerts in config/platform.yaml are
mirrored as Azure Monitor alert rules.
"""
from __future__ import annotations

import json
import operator
import time
from contextlib import contextmanager
from pathlib import Path

from mdm.observability.logging import get_logger, set_run_context

log = get_logger("metrics")

_OPS = {"<": operator.lt, "<=": operator.le, ">": operator.gt, ">=": operator.ge, "==": operator.eq}


class Metrics:
    def __init__(self, run_id: str):
        self.run_id = run_id
        self.values: dict[str, float] = {}
        self.timings: dict[str, float] = {}
        self.alerts: list[dict] = []

    def inc(self, name: str, value: float = 1) -> None:
        self.values[name] = self.values.get(name, 0) + value

    def set(self, name: str, value: float) -> None:
        self.values[name] = value

    def get(self, name: str, default: float = 0) -> float:
        return self.values.get(name, default)

    @contextmanager
    def stage(self, name: str):
        """Times a pipeline stage and tags all logs inside it with the stage name."""
        set_run_context(stage=name)
        t0 = time.perf_counter()
        log.info(f"stage started: {name}")
        try:
            yield
        except Exception:
            self.inc(f"stage.{name}.failed")
            log.exception(f"stage failed: {name}")
            raise
        finally:
            dur = round(time.perf_counter() - t0, 3)
            self.timings[name] = dur
            log.info(f"stage finished: {name}", seconds=dur)

    def evaluate_alerts(self, rules: list[dict]) -> list[dict]:
        for r in rules or []:
            if r["metric"] not in self.values:
                continue
            v = self.values[r["metric"]]
            if _OPS[r["op"]](v, r["threshold"]):
                alert = {**r, "value": v, "run_id": self.run_id}
                self.alerts.append(alert)
                (log.error if r.get("severity") == "critical" else log.warning)(
                    f"ALERT {r['name']}", metric=r["metric"], value=v, threshold=r["threshold"]
                )
        return self.alerts

    def to_dict(self) -> dict:
        return {"run_id": self.run_id, "metrics": self.values, "timings_seconds": self.timings, "alerts": self.alerts}

    def write(self, out_dir: Path) -> None:
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "metrics.json").write_text(json.dumps(self.to_dict(), indent=2))
        lines = []
        for k, v in sorted(self.values.items()):
            name = "mdm_" + k.replace(".", "_").replace("-", "_")
            lines.append(f'{name}{{run_id="{self.run_id}"}} {v}')
        for k, v in sorted(self.timings.items()):
            lines.append(f'mdm_stage_duration_seconds{{run_id="{self.run_id}",stage="{k}"}} {v}')
        (out_dir / "metrics.prom").write_text("\n".join(lines) + "\n")
