#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


_SECTION_PATTERN = re.compile(r"^##\s+`(configs/[^`]+)`\s*$")


def _normalize_rel(path: Path, base: Path) -> str:
    return path.relative_to(base).as_posix()


def _discover_config_files(repo_root: Path) -> set[str]:
    configs_root = repo_root / "configs"
    if not configs_root.exists():
        raise ValueError(f"missing configs directory: {configs_root}")

    discovered: set[str] = set()
    for path in configs_root.rglob("*"):
        if not path.is_file():
            continue
        rel = _normalize_rel(path, repo_root)
        if rel.endswith(":Zone.Identifier"):
            continue
        discovered.add(rel)
    return discovered


def _load_catalog_paths(catalog_path: Path, repo_root: Path) -> set[str]:
    if not catalog_path.exists():
        raise ValueError(f"missing catalog file: {catalog_path}")

    entries: set[str] = set()
    with catalog_path.open("r", encoding="utf-8") as fp:
        for line_no, line in enumerate(fp, start=1):
            match = _SECTION_PATTERN.match(line.strip())
            if match is None:
                continue
            rel = match.group(1)
            if rel.endswith(":Zone.Identifier"):
                raise ValueError(f"line {line_no}: Zone.Identifier entry is not allowed: {rel}")
            entries.add(rel)

    if not entries:
        raise ValueError("catalog has no section headings like: ## `configs/...`")

    # Ensure catalog references repo-local paths only.
    for rel in sorted(entries):
        if rel.startswith("/") or rel.startswith("../") or "/../" in rel:
            raise ValueError(f"catalog entry must be repo-relative: {rel}")
    return entries


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify docs/ops/config_catalog.md covers all effective files under configs/."
    )
    parser.add_argument("--repo-root", default=".", help="Repository root path")
    parser.add_argument(
        "--catalog", default="docs/ops/config_catalog.md", help="Config catalog markdown path"
    )
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    catalog_path = Path(args.catalog)
    if not catalog_path.is_absolute():
        catalog_path = repo_root / catalog_path
    catalog_path = catalog_path.resolve()

    try:
        config_files = _discover_config_files(repo_root)
        catalog_entries = _load_catalog_paths(catalog_path, repo_root)
    except Exception as exc:
        print(f"[config-docs-coverage] ERROR: {exc}", file=sys.stderr)
        return 2

    missing_in_catalog = sorted(config_files - catalog_entries)
    missing_in_repo = sorted(catalog_entries - config_files)

    if missing_in_catalog or missing_in_repo:
        if missing_in_catalog:
            print("[config-docs-coverage] Missing in catalog:", file=sys.stderr)
            for item in missing_in_catalog:
                print(f"  - {item}", file=sys.stderr)
        if missing_in_repo:
            print("[config-docs-coverage] Missing in repository (stale doc entry):", file=sys.stderr)
            for item in missing_in_repo:
                print(f"  - {item}", file=sys.stderr)
        return 1

    print(
        f"[config-docs-coverage] OK: catalog covers {len(config_files)} config files under {repo_root / 'configs'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
