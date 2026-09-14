#!/usr/bin/env bash
set -euo pipefail
umask 077
package_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
package_die() {
    printf 'packaged SimNow supervisor preflight failed: %s\n' "$*" >&2
    exit 1
}
release_verifier="$package_root/scripts/ops/verify_packaged_release.sh"
[[ -s "$release_verifier" ]] || package_die "missing packaged release verifier"
/bin/bash "$release_verifier" "$package_root" ||
    package_die "release integrity gate rejected $package_root"
if [[ -n "${QUANT_HFT_DEPLOYMENT_FILE:-}" && "$QUANT_HFT_DEPLOYMENT_FILE" != /* ]]; then
    export QUANT_HFT_DEPLOYMENT_FILE="$PWD/$QUANT_HFT_DEPLOYMENT_FILE"
fi
# Resolve public default inputs against this release even with external account configuration.
cd "$package_root"
export QUANT_HFT_BIN_DIR="$package_root/bin"
export LD_LIBRARY_PATH="$package_root/lib/ctp${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
exec /bin/bash "$package_root/scripts/ops/run_account_supervisor.sh" "$@"
