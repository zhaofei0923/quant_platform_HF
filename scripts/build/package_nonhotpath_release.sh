#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RAW_VERSION="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
VERSION="${RAW_VERSION//\//-}"
VERSION="${VERSION//:/-}"
OUT_DIR="${2:-$ROOT_DIR/dist}"

if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][A-Za-z0-9]+)*$ ]] && \
   [[ ! "$VERSION" =~ ^[0-9]{8}T[0-9]{6}Z$ ]]; then
  echo "error: version must be semver (vX.Y.Z) or UTC timestamp (YYYYMMDDTHHMMSSZ)" >&2
  exit 2
fi

BUNDLE_NAME="quant-hft-cpp-${VERSION}"
STAGE_DIR="$(mktemp -d)"
PAYLOAD_DIR="$STAGE_DIR/$BUNDLE_NAME"
trap 'rm -rf "$STAGE_DIR"' EXIT

mkdir -p "$OUT_DIR"
mkdir -p "$PAYLOAD_DIR"/{bin,configs/sim,docs,scripts/build,scripts/ops}

cp "$ROOT_DIR/README.md" "$PAYLOAD_DIR/"
cp "$ROOT_DIR/configs/sim/ctp.yaml" "$PAYLOAD_DIR/configs/sim/"
cp "$ROOT_DIR/scripts/build/bootstrap.sh" "$PAYLOAD_DIR/scripts/build/"
cp "$ROOT_DIR/scripts/build/package_nonhotpath_release.sh" "$PAYLOAD_DIR/scripts/build/"
cp "$ROOT_DIR/scripts/ops/run_v3_acceptance.sh" "$PAYLOAD_DIR/scripts/ops/"

for bin in \
  core_engine \
  daily_settlement \
  hotpath_benchmark \
  wal_replay_tool \
  backtest_cli \
  factor_eval_cli \
  backtest_benchmark_cli \
  csv_parquet_compare_cli \
  simnow_compare_cli \
  simnow_weekly_stress_cli \
  reconnect_evidence_cli \
  ops_health_report_cli \
  ops_alert_report_cli \
  ctp_cutover_orchestrator_cli \
  verify_contract_sync_cli \
  verify_develop_requirements_cli; do
  if [[ -f "$ROOT_DIR/build/$bin" ]]; then
    cp "$ROOT_DIR/build/$bin" "$PAYLOAD_DIR/bin/"
  fi
done

BUILD_TS_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
if GIT_COMMIT="$(git -C "$ROOT_DIR" rev-parse --short=12 HEAD 2>/dev/null)"; then
  :
else
  GIT_COMMIT="unknown"
fi

cat >"$PAYLOAD_DIR/deploy_manifest.json" <<EOF
{
  "release_version": "$VERSION",
  "build_ts_utc": "$BUILD_TS_UTC",
  "git_commit": "$GIT_COMMIT",
  "bundle_name": "$BUNDLE_NAME",
  "language_runtime": "cpp-only"
}
EOF

ARCHIVE_PATH="$OUT_DIR/$BUNDLE_NAME.tar.gz"
tar -C "$STAGE_DIR" -czf "$ARCHIVE_PATH" "$BUNDLE_NAME"
sha256sum "$ARCHIVE_PATH" >"$ARCHIVE_PATH.sha256"

echo "$ARCHIVE_PATH"
echo "$ARCHIVE_PATH.sha256"
