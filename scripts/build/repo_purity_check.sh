#!/usr/bin/env bash
set -euo pipefail

repo_root="."

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --repo-root PATH  Repository root to scan (default: .)
  -h, --help        Show this help
EOF
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

declare -a violations=()

for asset in \
  "python" \
  "pyproject.toml" \
  "requirements.txt" \
  "requirements-dev.txt" \
  "requirements.lock" \
  "requirements-dev.lock"; do
  if [[ -e "${asset}" ]]; then
    violations+=("${asset}")
  fi
done

collect_git_tracked_python_assets() {
  if ! command -v git >/dev/null 2>&1; then
    return 1
  fi
  if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    return 1
  fi

  local file=""
  while IFS= read -r file; do
    [[ -z "${file}" ]] && continue
    [[ -e "${file}" ]] || continue
    violations+=("${file}")
  done < <(git ls-files '*.py')
}

collect_filesystem_python_assets() {
  local file=""
  while IFS= read -r file; do
    file="${file#./}"
    violations+=("${file}")
  done < <(find . -type f -name '*.py' -not -path './.git/*' | sort)
}

if ! collect_git_tracked_python_assets; then
  collect_filesystem_python_assets
fi

if [[ ${#violations[@]} -gt 0 ]]; then
  echo "repo purity check failed: Python assets detected" >&2
  mapfile -t unique_violations < <(printf '%s\n' "${violations[@]}" | sort -u)
  for path in "${unique_violations[@]}"; do
    echo " - ${path}" >&2
  done
  exit 1
fi

echo "repo purity check passed: no Python assets detected"
