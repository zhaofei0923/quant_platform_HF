#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

try:
    from quant_hft.ops.daily_settlement_orchestrator import main
except ModuleNotFoundError:  # pragma: no cover
    repo_python = Path(__file__).resolve().parents[2] / "python"
    if str(repo_python) not in sys.path:
        sys.path.insert(0, str(repo_python))
    from quant_hft.ops.daily_settlement_orchestrator import main  # type: ignore[no-redef]


if __name__ == "__main__":
    raise SystemExit(main())
