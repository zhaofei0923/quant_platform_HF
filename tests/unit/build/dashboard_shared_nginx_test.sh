#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd -P)"
source "$ROOT/infra/dashboard/lib.sh"
TMP="$(mktemp -d)"
trap 'rm -rf -- "$TMP"' EXIT

dashboard_render "$ROOT/infra/dashboard/nginx.shared-site.conf.in" "$TMP/site" \
  DOMAIN quant.easudata.com PORT_SUFFIX '' AUTH_PORT 9091 \
  TLS_CERT /etc/quant-dashboard/tls/fullchain.pem TLS_KEY /etc/quant-dashboard/tls/privkey.pem \
  WEB_ROOT /opt/quant-dashboard/releases/test/web DATA_ROOT /var/lib/quant-dashboard-public/data \
  AUTH_PROXY /etc/quant-dashboard/auth-proxy.conf ACCESS_LOG /var/log/quant-dashboard-web/access.log
[[ "$(grep -Ec '^[[:space:]]*listen 80;' "$TMP/site")" == 1 ]]
[[ "$(grep -Ec '^[[:space:]]*listen 443 ssl;' "$TMP/site")" == 1 ]]
grep -Fq 'server_name quant.easudata.com;' "$TMP/site"
grep -Fq 'proxy_pass http://127.0.0.1:9091/api/authz/auth-request;' "$TMP/site"
grep -Fq 'auth_request /internal/authz;' "$TMP/site"
[[ "$(grep -c 'limit_req_zone' "$TMP/site")" == 3 ]]
! grep -Eq '@[A-Z_]+@' "$TMP/site"

cat > "$TMP/dump-owned" <<'EOF'
# configuration file /etc/nginx/nginx.conf:
user www-data;
http { include /etc/nginx/sites-enabled/*; }
# configuration file /etc/nginx/sites-enabled/bid:
server { server_name bid.easudata.com; }
# configuration file /etc/nginx/sites-enabled/quant-dashboard:
server { server_name quant.easudata.com; }
EOF
[[ "$(dashboard_nginx_declared_user "$TMP/dump-owned")" == www-data ]]
! dashboard_nginx_domain_conflict "$TMP/dump-owned" quant.easudata.com \
  /etc/nginx/sites-enabled/quant-dashboard /etc/nginx/sites-available/quant-dashboard >/dev/null
cat >> "$TMP/dump-owned" <<'EOF'
# configuration file /etc/nginx/sites-enabled/navigator:
server { server_name quant.easudata.com; }
EOF
[[ "$(dashboard_nginx_domain_conflict "$TMP/dump-owned" quant.easudata.com \
  /etc/nginx/sites-enabled/quant-dashboard /etc/nginx/sites-available/quant-dashboard)" == "/etc/nginx/sites-enabled/navigator" ]]

bash -n "$ROOT/scripts/ops/preflight_dashboard.sh" "$ROOT/scripts/ops/install_dashboard.sh" \
  "$ROOT/scripts/ops/rollback_dashboard.sh" "$ROOT/scripts/ops/renew_dashboard_certificate.sh"
grep -Fq 'find "$RELEASE/bin" -type f -exec chmod 0755 {} +' \
  "$ROOT/scripts/ops/install_dashboard.sh" || {
    echo 'FAIL: installer does not normalize packaged executable modes'; exit 1;
  }
printf 'PASS: shared site render, HTTP redirect, HTTPS auth_request, exact-domain collision detection, worker-user detection and deployment script syntax.\n'
