from __future__ import annotations

import subprocess
import sys


def test_verify_contract_sync_passes_for_repository_contracts() -> None:
    command = [
        sys.executable,
        "scripts/build/verify_contract_sync.py",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "contract sync verification passed" in (completed.stdout + completed.stderr).lower()
