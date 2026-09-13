#!/usr/bin/env bash
set -euo pipefail
umask 077
package_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
if [[ -n "${QUANT_HFT_DEPLOYMENT_FILE:-}" && "$QUANT_HFT_DEPLOYMENT_FILE" != /* ]]; then
    export QUANT_HFT_DEPLOYMENT_FILE="$PWD/$QUANT_HFT_DEPLOYMENT_FILE"
fi
# Resolve public default inputs against this release even with external account configuration.
cd "$package_root"
export QUANT_HFT_BIN_DIR="$package_root/bin"
export LD_LIBRARY_PATH="$package_root/lib/ctp${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
exec /bin/bash "$package_root/scripts/ops/run_account_supervisor.sh" "$@"
