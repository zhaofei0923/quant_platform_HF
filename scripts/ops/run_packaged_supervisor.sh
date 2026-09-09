#!/usr/bin/env bash
set -euo pipefail
umask 077
package_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
export QUANT_HFT_BIN_DIR="$package_root/bin"
export LD_LIBRARY_PATH="$package_root/lib/ctp${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
exec /bin/bash "$package_root/scripts/ops/run_account_supervisor.sh" "$@"
