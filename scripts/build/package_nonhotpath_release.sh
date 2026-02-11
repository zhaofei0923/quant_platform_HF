#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RAW_VERSION="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
VERSION="${RAW_VERSION//\//-}"
VERSION="${VERSION//:/-}"
OUT_DIR="${2:-$ROOT_DIR/dist}"
BUNDLE_NAME="quant-hft-nonhotpath-${VERSION}"
STAGE_DIR="$(mktemp -d)"
PAYLOAD_DIR="$STAGE_DIR/$BUNDLE_NAME"
trap 'rm -rf "$STAGE_DIR"' EXIT

mkdir -p "$OUT_DIR"
mkdir -p "$PAYLOAD_DIR"/{deploy,docs,python,scripts/{build,data_pipeline,ops},configs/sim}

PYTHONPATH="$ROOT_DIR/python${PYTHONPATH:+:$PYTHONPATH}" \
  python3 "$ROOT_DIR/scripts/ops/render_k8s_manifests.py" \
    --repo-root "$ROOT_DIR" \
    --output-dir "$PAYLOAD_DIR/deploy/k8s" \
    --image-tag "$VERSION" >/dev/null

PYTHONPATH="$ROOT_DIR/python${PYTHONPATH:+:$PYTHONPATH}" \
  python3 "$ROOT_DIR/scripts/ops/render_systemd_units.py" \
    --repo-root "$ROOT_DIR" \
    --output-dir "$PAYLOAD_DIR/deploy/systemd" >/dev/null

cp "$ROOT_DIR/pyproject.toml" "$PAYLOAD_DIR/"
cp "$ROOT_DIR/README.md" "$PAYLOAD_DIR/"
cp "$ROOT_DIR/docs/K8S_DEPLOYMENT_RUNBOOK.md" "$PAYLOAD_DIR/docs/"
cp "$ROOT_DIR/docs/SYSTEMD_DEPLOYMENT_RUNBOOK.md" "$PAYLOAD_DIR/docs/"
cp "$ROOT_DIR/docs/CTP_SIMNOW_RECONNECT_FAULT_INJECTION_RUNBOOK.md" "$PAYLOAD_DIR/docs/"
cp "$ROOT_DIR/docs/templates/RECONNECT_FAULT_INJECTION_RESULT.md" "$PAYLOAD_DIR/docs/"
cp "$ROOT_DIR/configs/sim/ctp.yaml" "$PAYLOAD_DIR/configs/sim/"
cp "$ROOT_DIR/configs/sim/ctp_trading_hours.yaml" "$PAYLOAD_DIR/configs/sim/"

cp "$ROOT_DIR/scripts/build/package_nonhotpath_release.sh" "$PAYLOAD_DIR/scripts/build/"
cp "$ROOT_DIR/scripts/data_pipeline/run_pipeline.py" "$PAYLOAD_DIR/scripts/data_pipeline/"
cp "$ROOT_DIR/scripts/ops/ctp_fault_inject.py" "$PAYLOAD_DIR/scripts/ops/"
cp "$ROOT_DIR/scripts/ops/ctp_preflight_check.py" "$PAYLOAD_DIR/scripts/ops/"
cp "$ROOT_DIR/scripts/ops/reconnect_slo_report.py" "$PAYLOAD_DIR/scripts/ops/"
cp "$ROOT_DIR/scripts/ops/render_k8s_manifests.py" "$PAYLOAD_DIR/scripts/ops/"
cp "$ROOT_DIR/scripts/ops/render_systemd_units.py" "$PAYLOAD_DIR/scripts/ops/"
cp "$ROOT_DIR/scripts/ops/run_reconnect_evidence.py" "$PAYLOAD_DIR/scripts/ops/"

cp -r "$ROOT_DIR/python/quant_hft" "$PAYLOAD_DIR/python/"
find "$PAYLOAD_DIR/python" -type f -name '*.pyc' -delete
find "$PAYLOAD_DIR/python" -type d -name '__pycache__' -empty -delete

ARCHIVE_PATH="$OUT_DIR/$BUNDLE_NAME.tar.gz"
tar -C "$STAGE_DIR" -czf "$ARCHIVE_PATH" "$BUNDLE_NAME"
sha256sum "$ARCHIVE_PATH" >"$ARCHIVE_PATH.sha256"

echo "$ARCHIVE_PATH"
echo "$ARCHIVE_PATH.sha256"
