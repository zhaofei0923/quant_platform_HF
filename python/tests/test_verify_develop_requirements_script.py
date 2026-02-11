from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_verify_develop_requirements_passes_repository_manifest() -> None:
    command = [
        sys.executable,
        "scripts/build/verify_develop_requirements.py",
        "--requirements-file",
        "docs/requirements/develop_requirements.yaml",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "verified requirements" in (completed.stdout + completed.stderr).lower()


def test_verify_develop_requirements_rejects_missing_path(tmp_path: Path) -> None:
    requirements_file = tmp_path / "requirements.yaml"
    requirements_file.write_text(
        json.dumps(
            {
                "requirements": [
                    {
                        "id": "REQ-FAIL-001",
                        "doc": "develop/00-项目总览与设计原则.md",
                        "description": "intentionally broken requirement",
                        "code_paths": ["src/not_found.cpp"],
                        "test_paths": ["python/tests/test_contract_sync.py"],
                        "evidence_paths": [
                            "docs/results/final_capability_acceptance_2026-02-11.md"
                        ],
                    }
                ]
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    command = [
        sys.executable,
        "scripts/build/verify_develop_requirements.py",
        "--requirements-file",
        str(requirements_file),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    assert "missing path" in (completed.stdout + completed.stderr).lower()


def test_verify_develop_requirements_rejects_forbidden_completion_language(
    tmp_path: Path,
) -> None:
    develop_root = tmp_path / "develop"
    develop_root.mkdir(parents=True, exist_ok=True)
    (develop_root / "doc.md").write_text(
        "# sample\n\n## 规划内容（未落地）\n\n1. placeholder\n",
        encoding="utf-8",
    )
    (tmp_path / "src").mkdir(parents=True, exist_ok=True)
    (tmp_path / "python" / "tests").mkdir(parents=True, exist_ok=True)
    (tmp_path / "docs" / "results").mkdir(parents=True, exist_ok=True)
    (tmp_path / "src" / "ok.cpp").write_text("// ok\n", encoding="utf-8")
    (tmp_path / "python" / "tests" / "ok_test.py").write_text("# ok\n", encoding="utf-8")
    (tmp_path / "docs" / "results" / "ok.env").write_text("OK=1\n", encoding="utf-8")

    requirements_file = tmp_path / "requirements.yaml"
    requirements_file.write_text(
        json.dumps(
            {
                "requirements": [
                    {
                        "id": "REQ-FAIL-002",
                        "doc": "develop/doc.md",
                        "description": "doc completion language must be converged",
                        "code_paths": [str(tmp_path / "src" / "ok.cpp")],
                        "test_paths": [str(tmp_path / "python" / "tests" / "ok_test.py")],
                        "evidence_paths": [str(tmp_path / "docs" / "results" / "ok.env")],
                    }
                ]
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    command = [
        sys.executable,
        "scripts/build/verify_develop_requirements.py",
        "--requirements-file",
        str(requirements_file),
        "--develop-root",
        str(develop_root),
        "--forbidden-term",
        "规划内容（未落地）",
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode != 0
    output = (completed.stdout + completed.stderr).lower()
    assert "forbidden completion language" in output


def test_verify_develop_requirements_can_generate_report_evidence_in_same_run(
    tmp_path: Path,
) -> None:
    develop_root = tmp_path / "develop"
    develop_root.mkdir(parents=True, exist_ok=True)
    (develop_root / "doc.md").write_text(
        "# sample\n\nall requirements are implemented.\n",
        encoding="utf-8",
    )
    code_path = tmp_path / "src" / "ok.cpp"
    test_path = tmp_path / "python" / "tests" / "ok_test.py"
    report_path = tmp_path / "docs" / "results" / "completion_report.json"
    code_path.parent.mkdir(parents=True, exist_ok=True)
    test_path.parent.mkdir(parents=True, exist_ok=True)
    code_path.write_text("// ok\n", encoding="utf-8")
    test_path.write_text("# ok\n", encoding="utf-8")

    requirements_file = tmp_path / "requirements.yaml"
    requirements_file.write_text(
        json.dumps(
            {
                "requirements": [
                    {
                        "id": "REQ-OK-001",
                        "doc": "develop/doc.md",
                        "description": "report evidence can be generated in verify run",
                        "code_paths": [str(code_path)],
                        "test_paths": [str(test_path)],
                        "evidence_paths": [str(report_path)],
                    }
                ]
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    command = [
        sys.executable,
        "scripts/build/verify_develop_requirements.py",
        "--requirements-file",
        str(requirements_file),
        "--develop-root",
        str(develop_root),
        "--forbidden-term",
        "不会命中的词条",
        "--completion-language-report",
        str(report_path),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert report_path.exists()
