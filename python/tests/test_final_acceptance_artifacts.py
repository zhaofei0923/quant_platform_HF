from __future__ import annotations

from pathlib import Path


def test_failover_evidence_and_reports_exist() -> None:
    evidence = Path("docs/results/failover_result.env")
    report = Path("docs/results/failover_drill_report_2026-02-11.md")
    assert evidence.exists(), f"missing failover evidence: {evidence}"
    assert report.exists(), f"missing failover drill report: {report}"

    payload = evidence.read_text(encoding="utf-8")
    assert "FAILOVER_SUCCESS=true" in payload
    assert "FAILOVER_TOTAL_STEPS=5" in payload
    assert "STEP_4_NAME=promote_standby" in payload


def test_final_acceptance_includes_failover_scope() -> None:
    acceptance = Path("docs/results/final_capability_acceptance_2026-02-11.md").read_text(
        encoding="utf-8"
    )
    assert "M0-M10" in acceptance
    assert "failover" in acceptance.lower()
    assert "docs/results/failover_result.env" in acceptance


def test_develop_00_docs_include_m10_completion_markers() -> None:
    roadmap = Path("develop/00-未完成能力补齐路线图.md").read_text(encoding="utf-8")
    matrix = Path("develop/00-实现对齐矩阵与缺口总览.md").read_text(encoding="utf-8")

    assert "M10" in roadmap
    assert "状态：`done`" in roadmap
    assert "failover_orchestrator.py" in matrix
    assert "verify_failover_evidence.py" in matrix
