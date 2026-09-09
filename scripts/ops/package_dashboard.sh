#!/usr/bin/env bash
set -euo pipefail
umask 077
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd -P)"
source "${REPO_ROOT}/infra/dashboard/lib.sh"
BUILD_DIR=''; WEB_DIR=''; AUTHELIA_BIN=''; OUTPUT=''
while (($#)); do
  case "$1" in
    --build-dir) BUILD_DIR="$2"; shift 2;;
    --web-dir) WEB_DIR="$2"; shift 2;;
    --authelia-bin) AUTHELIA_BIN="$2"; shift 2;;
    --output) OUTPUT="$2"; shift 2;;
    -h|--help) echo 'Usage: package_dashboard.sh --build-dir DIR --web-dir web/dashboard/dist --authelia-bin FILE --output FILE.tar.gz'; exit 0;;
    *) dashboard_die "unknown argument: $1";;
  esac
done
[[ -x "${BUILD_DIR}/dashboard_publish_cli" && -f "${WEB_DIR}/index.html" && -x "$AUTHELIA_BIN" && -n "$OUTPUT" ]] || dashboard_die 'compiled publisher, built frontend, Authelia binary and output are required'
[[ ! -e "$OUTPUT" ]] || dashboard_die 'output exists; choose a new package filename'
[[ -z "$(find "$WEB_DIR" -type l -print -quit)" ]] || dashboard_die 'frontend contains a symlink'
[[ -z "$(find "$WEB_DIR" -type f \( -name '*.map' -o -name '*.env' -o -name '*.pem' -o -name '*.key' -o -name '*.json' \) -print -quit)" ]] || dashboard_die 'frontend contains private, source-map, or fixture JSON files; remove them from the public build'
STAGE="$(mktemp -d)"
trap 'rm -rf -- "$STAGE"' EXIT
mkdir -p "$STAGE/bin" "$STAGE/web" "$STAGE/infra/dashboard" "$STAGE/scripts/ops" "$STAGE/docs/ops"
install -m 0755 "${BUILD_DIR}/dashboard_publish_cli" "$STAGE/bin/dashboard_publish_cli"
install -m 0755 "$AUTHELIA_BIN" "$STAGE/bin/authelia"
cp -a "${WEB_DIR}/." "$STAGE/web/"
cp -a "${REPO_ROOT}/infra/dashboard/." "$STAGE/infra/dashboard/"
install -m 0644 "$REPO_ROOT/docs/ops/tencent_dashboard.md" "$REPO_ROOT/docs/ops/dashboard_data_contract.md" "$STAGE/docs/ops/"
for item in preflight_dashboard.sh install_dashboard.sh rollback_dashboard.sh renew_dashboard_certificate.sh; do
  install -m 0755 "${SCRIPT_DIR}/${item}" "$STAGE/scripts/ops/${item}"
done
(cd "$STAGE" && find bin web infra scripts docs -type f -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS)
tar -C "$STAGE" -czf "$OUTPUT" .
printf 'Dashboard package created: %s\nNo private identity, users, certificates or runtime data included.\n' "$OUTPUT"
