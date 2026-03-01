#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
cd "${repo_root}"

runs_root="${repo_root}/docs/results/backtest_runs"
mkdir -p "${runs_root}"

latest_before="$(ls -1dt "${runs_root}"/backtest-* 2>/dev/null | head -n 1 || true)"

echo "[step] 执行回测"
"${script_dir}/run_backtest_from_config.sh" "$@"

latest_after="$(ls -1dt "${runs_root}"/backtest-* 2>/dev/null | head -n 1 || true)"
if [[ -z "${latest_after}" ]]; then
  echo "error: 未找到回测输出目录: ${runs_root}" >&2
  exit 2
fi

if [[ -n "${latest_before}" && "${latest_before}" == "${latest_after}" ]]; then
  echo "[warn] 未检测到新回测目录，默认对最新目录做检测: ${latest_after}"
else
  echo "[step] 检测目录: ${latest_after}"
fi

echo "[step] 生成检测报告"
python3 "${repo_root}/scripts/analysis/backtest_validation_report.py" --run-dir "${latest_after}" --strict

echo "[done] 检测完成，报告路径: ${latest_after}/validation_report.md"
