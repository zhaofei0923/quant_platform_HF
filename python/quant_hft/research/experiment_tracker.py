from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ExperimentRecord:
    run_id: str
    template: str
    factor_id: str
    spec_signature: str
    metrics: dict[str, float]
    created_ts_ns: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "template": self.template,
            "factor_id": self.factor_id,
            "spec_signature": self.spec_signature,
            "metrics": dict(self.metrics),
            "created_ts_ns": self.created_ts_ns,
        }


class ExperimentTracker:
    def __init__(
        self,
        output_jsonl: Path | str,
        now_ns_fn: Callable[[], int] | None = None,
    ) -> None:
        self._output = Path(output_jsonl)
        self._output.parent.mkdir(parents=True, exist_ok=True)
        self._now_ns_fn = now_ns_fn or time.time_ns

    def append(
        self,
        *,
        run_id: str,
        template: str,
        factor_id: str,
        spec_signature: str,
        metrics: dict[str, float],
    ) -> ExperimentRecord:
        if not run_id.strip():
            raise ValueError("run_id is required")
        if not template.strip():
            raise ValueError("template is required")
        if not factor_id.strip():
            raise ValueError("factor_id is required")
        record = ExperimentRecord(
            run_id=run_id.strip(),
            template=template.strip(),
            factor_id=factor_id.strip(),
            spec_signature=spec_signature.strip(),
            metrics={key: float(value) for key, value in metrics.items()},
            created_ts_ns=int(self._now_ns_fn()),
        )
        with self._output.open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(record.to_dict(), ensure_ascii=True) + "\n")
        return record

    def load_all(self) -> list[ExperimentRecord]:
        if not self._output.exists():
            return []
        records: list[ExperimentRecord] = []
        for raw_line in self._output.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            records.append(
                ExperimentRecord(
                    run_id=str(payload["run_id"]),
                    template=str(payload["template"]),
                    factor_id=str(payload["factor_id"]),
                    spec_signature=str(payload.get("spec_signature", "")),
                    metrics={str(k): float(v) for k, v in dict(payload.get("metrics", {})).items()},
                    created_ts_ns=int(payload["created_ts_ns"]),
                )
            )
        return records
