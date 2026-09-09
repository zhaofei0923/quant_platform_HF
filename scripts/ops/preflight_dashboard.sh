#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
PACKAGE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd -P)"
source "${PACKAGE_ROOT}/infra/dashboard/lib.sh"
DOMAIN=''; SOURCE_DIR=''; IDENTITY=''; CERT=''; KEY=''; NGINX_MODE='dedicated'; NGINX_WORKER_USER=''
while (($#)); do
  case "$1" in
    --domain) DOMAIN="$2"; shift 2;;
    --source-dir) SOURCE_DIR="$2"; shift 2;;
    --identity-file) IDENTITY="$2"; shift 2;;
    --tls-cert) CERT="$2"; shift 2;;
    --tls-key) KEY="$2"; shift 2;;
    --nginx-mode) NGINX_MODE="$2"; shift 2;;
    --nginx-worker-user) NGINX_WORKER_USER="$2"; shift 2;;
    -h|--help) echo 'Usage: preflight_dashboard.sh --domain HOST --source-dir ROOT --identity-file FILE --tls-cert FILE --tls-key FILE [--nginx-mode dedicated|shared-system] [--nginx-worker-user USER]'; exit 0;;
    *) dashboard_die "unknown argument: $1";;
  esac
done
for item in jq realpath openssl setfacl getfacl nginx ldd ss sha256sum; do dashboard_require "$item"; done
dashboard_domain "$DOMAIN"
dashboard_nginx_mode "$NGINX_MODE"
dashboard_path "$SOURCE_DIR"
[[ -d "$SOURCE_DIR" && "$(realpath "$SOURCE_DIR")" == "$SOURCE_DIR" ]] || dashboard_die 'source directory must exist and have a canonical path'
[[ "$SOURCE_DIR" == */runtime/*/*/*/* ]] || dashboard_die 'source must be a selected runtime/environment/broker/account/instance directory'
dashboard_identity_check "$IDENTITY" "$SOURCE_DIR"
[[ -f "$CERT" && -f "$KEY" ]] || dashboard_die 'provide existing TLS certificate and private key; no plaintext fallback'
openssl x509 -in "$CERT" -checkhost "$DOMAIN" -noout >/dev/null || dashboard_die 'TLS certificate does not cover domain'
openssl x509 -in "$CERT" -checkend 86400 -noout >/dev/null || dashboard_die 'TLS certificate expires in less than one day'
openssl verify -CApath /etc/ssl/certs -untrusted "$CERT" "$CERT" >/dev/null || dashboard_die 'TLS chain is not trusted by the server system CA store'
[[ "$(openssl x509 -in "$CERT" -pubkey -noout | openssl sha256)" == "$(openssl pkey -in "$KEY" -pubout | openssl sha256)" ]] || dashboard_die 'TLS key does not match certificate'
[[ -f "$PACKAGE_ROOT/SHA256SUMS" ]] || dashboard_die 'run preflight from an extracted release package'
(cd "$PACKAGE_ROOT" && sha256sum --check --status SHA256SUMS) || dashboard_die 'release checksum mismatch'
[[ -z "$(find "$PACKAGE_ROOT/bin" "$PACKAGE_ROOT/web" -type l -print -quit)" ]] || dashboard_die 'release contains symlinks'
for item in dashboard_publish_cli authelia; do
  [[ -x "$PACKAGE_ROOT/bin/$item" ]] || dashboard_die "missing executable: $item"
  [[ "$(ldd "$PACKAGE_ROOT/bin/$item" 2>&1 || true)" != *'not found'* ]] || dashboard_die "unresolved shared library for $item"
done
nginx -V 2>&1 | grep -q -- --with-http_auth_request_module || dashboard_die 'Nginx requires http_auth_request_module'
if [[ "$NGINX_MODE" == dedicated ]]; then
  [[ -z "$NGINX_WORKER_USER" ]] || dashboard_die '--nginx-worker-user is only valid in shared-system mode'
  if ss -H -lnt '( sport = :443 or sport = :9091 )' | grep -q .; then
    dashboard_die 'dedicated mode requires ports 443 and 9091 to be free; do not stop existing services to make room'
  fi
else
  dashboard_require systemctl
  [[ "$EUID" == 0 ]] || dashboard_die 'shared-system Nginx preflight requires root to validate the active configuration'
  dashboard_nginx_worker_user "$NGINX_WORKER_USER"
  id "$NGINX_WORKER_USER" >/dev/null 2>&1 || dashboard_die 'configured Nginx worker user does not exist'
  [[ -d /etc/nginx/sites-available && -d /etc/nginx/sites-enabled ]] ||
    dashboard_die 'shared-system mode requires the Debian/Ubuntu Nginx sites-available layout'
  systemctl is-active --quiet nginx.service || dashboard_die 'shared-system mode requires the existing nginx.service to be active'
  NGINX_DUMP="$(mktemp)"
  trap 'rm -f -- "$NGINX_DUMP"' EXIT
  nginx -t >/dev/null 2>&1 || dashboard_die 'existing system Nginx configuration does not pass nginx -t'
  nginx -T >"$NGINX_DUMP" 2>&1 || dashboard_die 'unable to inspect the existing system Nginx configuration'
  grep -Eq '^[[:space:]]*include[[:space:]]+/etc/nginx/sites-enabled/\*;' "$NGINX_DUMP" ||
    dashboard_die 'system Nginx does not include /etc/nginx/sites-enabled/*'
  DECLARED_NGINX_USER="$(dashboard_nginx_declared_user "$NGINX_DUMP")"
  [[ -n "$DECLARED_NGINX_USER" && "$DECLARED_NGINX_USER" == "$NGINX_WORKER_USER" ]] ||
    dashboard_die "--nginx-worker-user does not match the active Nginx user directive (${DECLARED_NGINX_USER:-missing})"
  SITE_AVAILABLE=/etc/nginx/sites-available/quant-dashboard
  SITE_ENABLED=/etc/nginx/sites-enabled/quant-dashboard
  if [[ -e "$SITE_AVAILABLE" || -L "$SITE_AVAILABLE" || -e "$SITE_ENABLED" || -L "$SITE_ENABLED" ]]; then
    [[ -L /opt/quant-dashboard/current && -f /etc/quant-dashboard/nginx-mode &&
       "$(cat /etc/quant-dashboard/nginx-mode)" == shared-system ]] ||
      dashboard_die 'quant-dashboard Nginx site paths already exist but are not owned by this shared-mode installation'
    [[ -f "$SITE_AVAILABLE" && ! -L "$SITE_AVAILABLE" ]] ||
      dashboard_die 'managed sites-available entry must be one regular file'
    [[ -L "$SITE_ENABLED" && "$(realpath -e "$SITE_ENABLED")" == "$SITE_AVAILABLE" ]] ||
      dashboard_die 'managed sites-enabled entry must link only to the dashboard site'
  fi
  if CONFLICT="$(dashboard_nginx_domain_conflict "$NGINX_DUMP" "$DOMAIN" "$SITE_ENABLED" "$SITE_AVAILABLE")"; then
    dashboard_die "domain is already declared by another enabled Nginx source: $(printf '%s' "$CONFLICT" | head -n 1)"
  fi
  if ss -H -lnt '( sport = :9091 )' | grep -q .; then
    dashboard_die 'port 9091 is already occupied; shared mode never stops or replaces that listener'
  fi
fi
printf 'Preflight passed for %s Nginx mode. Domain DNS, mainland ICP/access requirements, server capacity and external reachability still require deployment review. No services changed.\n' "$NGINX_MODE"
