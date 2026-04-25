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

declare -A allowed_python_assets=(
  ["scripts/analysis/backtest_analysis_report.py"]=1
  ["scripts/analysis/backtest_validation_report.py"]=1
  ["scripts/analysis/plot_sub_trace_plotly.py"]=1
  ["scripts/build/audit_contract_expiry_calendar.py"]=1
  ["scripts/build/verify_config_docs_coverage.py"]=1
  ["scripts/build/verify_products_info_sync.py"]=1
  ["tests/python/test_backtest_analysis_report.py"]=1
  ["tests/python/test_plot_sub_trace_plotly.py"]=1
)

is_allowed_python_asset() {
  local path="$1"
  [[ -n "${allowed_python_assets[${path}]:-}" ]]
}

is_git_repo() {
  command -v git >/dev/null 2>&1 && git rev-parse --is-inside-work-tree >/dev/null 2>&1
}

is_tracked_path() {
  local path="$1"
  git ls-files --error-unmatch -- "${path}" >/dev/null 2>&1
}

is_ignored_path() {
  local path="$1"
  git check-ignore -q -- "${path}" >/dev/null 2>&1
}

is_effective_repo_asset() {
  local path="$1"
  if [[ -d "${path}" ]]; then
    if [[ -n "$(git ls-files -- "${path}/")" ]]; then
      return 0
    fi
    if [[ -n "$(git ls-files --others --exclude-standard -- "${path}/")" ]]; then
      return 0
    fi
    return 1
  fi
  if is_tracked_path "${path}"; then
    return 0
  fi
  [[ -e "${path}" ]] || return 1
  ! is_ignored_path "${path}"
}

for asset in \
  "python" \
  "pyproject.toml" \
  "requirements.txt" \
  "requirements-dev.txt" \
  "requirements.lock" \
  "requirements-dev.lock"; do
  if is_git_repo; then
    if is_effective_repo_asset "${asset}"; then
      violations+=("${asset}")
    fi
  elif [[ -e "${asset}" ]]; then
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
    if is_allowed_python_asset "${file}"; then
      continue
    fi
    violations+=("${file}")
  done < <({ git ls-files '*.py'; git ls-files --others --exclude-standard '*.py'; } | sort -u)
}

collect_filesystem_python_assets() {
  local file=""
  while IFS= read -r file; do
    file="${file#./}"
    if is_allowed_python_asset "${file}"; then
      continue
    fi
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
