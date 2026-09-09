#!/usr/bin/env bash
set -euo pipefail
umask 077
release_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
binary_dir="${QUANT_HFT_BIN_DIR:-$release_root/build}"
deployment="${QUANT_HFT_DEPLOYMENT_FILE:?set an account deployment manifest path}"
account_ref="${QUANT_HFT_DEPLOYMENT_ACCOUNT:?set the selected account reference}"
case "${1:-}" in
    --check-only)
        "$binary_dir/quant_config_cli" validate "$deployment"
        "$binary_dir/quant_config_cli" list "$deployment"
        exec "$binary_dir/quant_config_cli" supervise "$deployment" "$account_ref" --dry-run
        ;;
    '') exec "$binary_dir/quant_config_cli" supervise "$deployment" "$account_ref" ;;
    *) echo "usage: run_account_supervisor.sh [--check-only]" >&2; exit 2 ;;
esac
