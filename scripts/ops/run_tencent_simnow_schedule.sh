#!/usr/bin/env bash
set -euo pipefail
# Compatibility command name; all runtime identities now come from the deployment manifest.
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
exec bash "$script_dir/run_account_deployment.sh" "$@"
