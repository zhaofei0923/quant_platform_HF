#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUANT_ROOT="${QUANT_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
UNIT_DIR="${SYSTEMD_USER_UNIT_DIR:-${HOME}/.config/systemd/user}"
SERVICE="quant-hft-simnow-contract-refresh.service"
TIMER="quant-hft-simnow-contract-refresh.timer"

mkdir -p "${UNIT_DIR}"
install -m 0644 "${QUANT_ROOT}/infra/systemd/${SERVICE}" "${UNIT_DIR}/${SERVICE}"
install -m 0644 "${QUANT_ROOT}/infra/systemd/${TIMER}" "${UNIT_DIR}/${TIMER}"
systemctl --user daemon-reload
systemctl --user enable --now "${TIMER}"
echo "[ok] installed and enabled ${TIMER}"
