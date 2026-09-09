#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
version="${1:?usage: package_nonhotpath_release.sh vX.Y.Z [output-dir] [build-dir]}"
output_dir="${2:-$repo_root/dist}"
build_dir="${3:-$repo_root/build}"
[[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][A-Za-z0-9]+)*$ ]] || {
    echo "invalid release version" >&2; exit 2;
}
test -s "$repo_root/dependencies.lock.json"
test -s "$build_dir/CMakeCache.txt"
bundle="quant-platform-hf-$version"
mkdir -p "$output_dir"
archive="$output_dir/$bundle.tar.gz"
[[ ! -e "$archive" && ! -e "$archive.sha256" ]] || {
    echo "release output already exists" >&2; exit 1;
}
stage="$(mktemp -d)"
trap 'rm -rf -- "$stage"' EXIT
payload="$stage/$bundle"
mkdir -p "$payload"/{bin,configs/deploy,configs/market,docs,infra/sql,scripts/ops}
for executable in core_engine quant_config_cli strategy_state_migrate_cli daily_settlement \
    wal_replay_tool simnow_probe runtime_paths_cli simnow_accounting_policy_check_cli \
    simnow_wal_export_cli simnow_dashboard_cli reconnect_evidence_cli \
    ops_health_report_cli ops_alert_report_cli dashboard_publish_cli simnow_flatten_positions; do
    test -x "$build_dir/$executable"
    cp "$build_dir/$executable" "$payload/bin/"
done
# Only public example inputs are packaged. No account credentials or runtime snapshots.
cp "$repo_root/README.md" "$repo_root/dependencies.lock.json" "$payload/"
cp "$repo_root/configs/deploy/instances.example.yaml" \
   "$repo_root/configs/deploy/connections.example.yaml" "$payload/configs/deploy/"
cp "$repo_root/docs/ops/three_project_migration.md" "$payload/docs/"
cp "$repo_root/docs/independent_strategy_books.md" "$payload/docs/"
cp "$repo_root/infra/timescale/init/"*.sql "$payload/infra/sql/"
cp "$repo_root/configs/trading_sessions.yaml" "$payload/configs/"
cp "$repo_root/configs/market/products.json" "$payload/configs/market/"
for script in run_simnow_preflight_check.sh run_account_deployment.sh run_account_supervisor.sh \
    run_packaged_account.sh run_packaged_supervisor.sh supervise_simnow_trading.sh \
    start_simnow_trading.sh runtime_path_defaults.sh run_daily_settlement.sh \
    export_simnow_trading_day.sh monitor_simnow_signal_execution.sh; do
    cp "$repo_root/scripts/ops/$script" "$payload/scripts/ops/"
done
cp "$repo_root/infra/systemd/quant-hft-account@.service" "$payload/infra/"
cp "$repo_root/infra/systemd/quant-hft-simnow-account@.service" "$payload/infra/"
git_commit="$(git -C "$repo_root" rev-parse HEAD)"
dirty=false
[[ -z "$(git -C "$repo_root" status --porcelain --untracked-files=normal)" ]] || dirty=true
real_api=false
if grep -q '^QUANT_HFT_ENABLE_CTP_REAL_API:BOOL=ON$' "$build_dir/CMakeCache.txt"; then
    real_api=true
fi
cat > "$payload/deploy_manifest.json" <<EOF
{
  "release_version": "$version",
  "git_commit": "$git_commit",
  "working_tree_dirty": $dirty,
  "language_runtime": "cpp-only",
  "ctp_real_api_compiled": $real_api,
  "simnow_acceptance": "functional_validation_required",
  "live_cutover_authorized": false,
  "runtime_requirements": "Install the locked strategy data package and matching system/CTP shared libraries. Materialize account deployment references outside the release tree."
}
EOF
(
    cd "$payload"
    find . -type f ! -name SHA256SUMS -print0 | LC_ALL=C sort -z | xargs -0 sha256sum > SHA256SUMS
)
tar -C "$stage" -czf "$archive" "$bundle"
sha256sum "$archive" > "$archive.sha256"
echo "$archive"
