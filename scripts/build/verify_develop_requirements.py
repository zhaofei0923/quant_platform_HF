#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

DEFAULT_FORBIDDEN_COMPLETION_TERMS = (
    "未落地",
    "规划中",
    "部分落地",
    "规划内容（未落地）",
    "规划示例（未落地）",
    "规划 SOP（未落地）",
    "进入实现阶段触发条件",
    "未来扩展（未落地）",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify develop requirements mapping against repo artifacts"
    )
    parser.add_argument(
        "--requirements-file",
        default="docs/requirements/develop_requirements.yaml",
        help="Path to requirement mapping file (JSON content in YAML extension is supported)",
    )
    parser.add_argument(
        "--develop-root",
        default="develop",
        help="Path to develop markdown root directory",
    )
    parser.add_argument(
        "--forbidden-term",
        action="append",
        default=None,
        help=(
            "Forbidden completion-language term. "
            "Can be repeated. Defaults to built-in convergence terms."
        ),
    )
    parser.add_argument(
        "--completion-language-report",
        default=None,
        help="Optional path to write forbidden-language scan report as JSON",
    )
    return parser.parse_args()


def _load_payload(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"requirements file must contain valid JSON: {exc}") from exc


def _as_string_list(raw: Any, *, field: str, req_id: str, errors: list[str]) -> list[str]:
    if not isinstance(raw, list) or not raw:
        errors.append(f"{req_id}: {field} must be a non-empty list")
        return []
    values: list[str] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, str) or not item.strip():
            errors.append(f"{req_id}: {field}[{idx}] must be a non-empty string")
            continue
        values.append(item.strip())
    return values


def _verify_requirement(
    requirement: Any,
    *,
    index: int,
    all_docs: set[str],
    seen_ids: set[str],
    covered_docs: set[str],
    generated_paths: set[Path],
    errors: list[str],
) -> None:
    if not isinstance(requirement, dict):
        errors.append(f"requirements[{index}] must be an object")
        return

    req_id_raw = requirement.get("id")
    req_id = req_id_raw.strip() if isinstance(req_id_raw, str) else ""
    if not req_id:
        errors.append(f"requirements[{index}] missing id")
        req_id = f"requirements[{index}]"
    elif req_id in seen_ids:
        errors.append(f"duplicate requirement id: {req_id}")
    else:
        seen_ids.add(req_id)

    doc_raw = requirement.get("doc")
    doc_path = doc_raw.strip() if isinstance(doc_raw, str) else ""
    if not doc_path:
        errors.append(f"{req_id}: doc must be a non-empty string")
    elif doc_path not in all_docs:
        errors.append(f"{req_id}: doc does not map to existing develop markdown: {doc_path}")
    else:
        covered_docs.add(doc_path)

    description = requirement.get("description")
    if not isinstance(description, str) or not description.strip():
        errors.append(f"{req_id}: description must be a non-empty string")

    code_paths = _as_string_list(
        requirement.get("code_paths"),
        field="code_paths",
        req_id=req_id,
        errors=errors,
    )
    test_paths = _as_string_list(
        requirement.get("test_paths"),
        field="test_paths",
        req_id=req_id,
        errors=errors,
    )
    evidence_paths = _as_string_list(
        requirement.get("evidence_paths"),
        field="evidence_paths",
        req_id=req_id,
        errors=errors,
    )

    for field_name, paths in (
        ("code_paths", code_paths),
        ("test_paths", test_paths),
        ("evidence_paths", evidence_paths),
    ):
        for repo_path in paths:
            candidate = Path(repo_path)
            if candidate.exists():
                continue
            if candidate.resolve() in generated_paths:
                continue
            if not candidate.exists():
                errors.append(f"{req_id}: missing path in {field_name}: {repo_path}")


def _scan_forbidden_completion_language(
    *,
    docs_by_key: dict[str, Path],
    forbidden_terms: tuple[str, ...],
    errors: list[str],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for doc_key in sorted(docs_by_key):
        doc_path = docs_by_key[doc_key]
        try:
            lines = doc_path.read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            errors.append(f"{doc_key}: failed to read document: {exc}")
            continue
        for term in forbidden_terms:
            hit_lines = [
                index + 1
                for index, line in enumerate(lines)
                if term in line
            ]
            if not hit_lines:
                continue
            errors.append(
                f"{doc_key}: contains forbidden completion language "
                f"'{term}' at lines {','.join(str(item) for item in hit_lines)}"
            )
            findings.append(
                {
                    "doc": doc_key,
                    "term": term,
                    "line_numbers": hit_lines,
                }
            )
    return findings


def main() -> int:
    args = _parse_args()
    requirements_file = Path(args.requirements_file)
    if not requirements_file.exists():
        print(f"error: requirements file not found: {requirements_file}")
        return 2

    try:
        payload = _load_payload(requirements_file)
    except ValueError as exc:
        print(f"error: {exc}")
        return 2

    raw_requirements = payload.get("requirements")
    if not isinstance(raw_requirements, list) or not raw_requirements:
        print("error: requirements must be a non-empty list")
        return 2

    develop_root = Path(args.develop_root)
    if not develop_root.exists():
        print(f"error: develop root not found: {develop_root}")
        return 2

    docs_by_key = {
        f"develop/{path.relative_to(develop_root).as_posix()}": path
        for path in sorted(develop_root.rglob("*.md"))
    }
    if not docs_by_key:
        print(f"error: no develop markdown files were found under: {develop_root}")
        return 2

    seen_ids: set[str] = set()
    covered_docs: set[str] = set()
    errors: list[str] = []
    forbidden_terms = tuple(args.forbidden_term or list(DEFAULT_FORBIDDEN_COMPLETION_TERMS))
    generated_paths: set[Path] = set()
    if args.completion_language_report:
        generated_paths.add(Path(args.completion_language_report).resolve())

    for index, requirement in enumerate(raw_requirements):
        _verify_requirement(
            requirement,
            index=index,
            all_docs=set(docs_by_key),
            seen_ids=seen_ids,
            covered_docs=covered_docs,
            generated_paths=generated_paths,
            errors=errors,
        )

    missing_docs = sorted(set(docs_by_key) - covered_docs)
    if missing_docs:
        errors.append(
            "requirements file does not cover all develop docs: "
            + ", ".join(missing_docs)
        )

    completion_findings = _scan_forbidden_completion_language(
        docs_by_key=docs_by_key,
        forbidden_terms=forbidden_terms,
        errors=errors,
    )

    if args.completion_language_report:
        report_path = Path(args.completion_language_report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_payload = {
            "develop_root": str(develop_root.as_posix()),
            "forbidden_terms": list(forbidden_terms),
            "docs_scanned": len(docs_by_key),
            "finding_count": len(completion_findings),
            "findings": completion_findings,
        }
        report_path.write_text(
            json.dumps(report_payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    if errors:
        print("verification failed")
        for item in errors:
            print(f"- {item}")
        return 2

    print(
        "verified requirements: "
        f"requirements={len(raw_requirements)} docs_covered={len(covered_docs)} "
        f"docs_scanned={len(docs_by_key)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
