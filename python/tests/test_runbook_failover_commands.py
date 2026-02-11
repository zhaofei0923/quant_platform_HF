from __future__ import annotations

from pathlib import Path

_REQUIRED_SNIPPETS = (
    "scripts/infra/bootstrap_prodlike_multi_host.sh",
    "scripts/ops/failover_orchestrator.py",
    "scripts/ops/verify_failover_evidence.py",
)

_TARGET_DOCS = (
    Path("docs/SYSTEMD_DEPLOYMENT_RUNBOOK.md"),
    Path("docs/K8S_DEPLOYMENT_RUNBOOK.md"),
    Path("docs/WAL_RECOVERY_RUNBOOK.md"),
    Path("develop/04-基础设施与运维/04-02-部署、灾备与持续集成方案.md"),
    Path("develop/07-部署与运维/07-02-运维操作手册.md"),
)


def test_runbooks_include_multi_host_failover_commands() -> None:
    for path in _TARGET_DOCS:
        content = path.read_text(encoding="utf-8")
        for snippet in _REQUIRED_SNIPPETS:
            assert snippet in content, f"missing '{snippet}' in {path}"
