#!/usr/bin/env bash
set -euo pipefail

repo_root="."

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --repo-root PATH  Repository root to scan (default: .)
  -h, --help        Show help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-root)
      repo_root="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -d "${repo_root}" ]]; then
  echo "error: repo root not found: ${repo_root}" >&2
  exit 2
fi

cd "${repo_root}"

pattern='\bpython\b|\bpytest\b|\bstrategy_runner\b|\.py\b'

matches=""
if command -v rg >/dev/null 2>&1; then
  matches="$(rg -n --no-heading --color never -e "${pattern}" docs README.md \
    -g '!docs/archive/**' -g '!docs/archive_legacy/**' || true)"
else
  matches="$(
    {
      find docs -type f \
        -not -path 'docs/archive/*' \
        -not -path 'docs/archive_legacy/*' \
        -print
      [[ -f README.md ]] && echo "README.md"
    } | sort -u | xargs grep -nE "${pattern}" || true
  )"
fi

if [[ -n "${matches}" ]]; then
  echo "doc purity check failed: disallowed Python-era references detected" >&2
  echo "${matches}" >&2
  exit 1
fi

echo "doc purity check passed"
