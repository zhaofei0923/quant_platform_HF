#!/usr/bin/env bash
# Certbot deploy hook: invoked only after successful issuance/renewal. No credentials are printed.
set -euo pipefail
umask 077
[[ "$EUID" == 0 ]] || { echo 'error: root required' >&2; exit 1; }
DOMAIN="$(cat /etc/quant-dashboard/domain)"
LINEAGE="${RENEWED_LINEAGE:-}"
EXPECTED_LINEAGE="/etc/letsencrypt/live/$DOMAIN"
if [[ -f /etc/quant-dashboard/certificate-lineage ]]; then EXPECTED_LINEAGE="$(cat /etc/quant-dashboard/certificate-lineage)"; fi
[[ "$DOMAIN" =~ ^[a-z0-9.-]+$ && "$EXPECTED_LINEAGE" == /etc/letsencrypt/live/* && "$LINEAGE" == "$EXPECTED_LINEAGE" ]] || { echo 'error: unexpected certificate lineage' >&2; exit 1; }
openssl x509 -in "$LINEAGE/fullchain.pem" -checkhost "$DOMAIN" -checkend 86400 -noout >/dev/null
[[ "$(openssl x509 -in "$LINEAGE/fullchain.pem" -pubkey -noout | openssl sha256)" == "$(openssl pkey -in "$LINEAGE/privkey.pem" -pubout | openssl sha256)" ]] || exit 1
install -m 0640 -o root -g quant-dashboard-web "$LINEAGE/fullchain.pem" /etc/quant-dashboard/tls/fullchain.pem.next
install -m 0640 -o root -g quant-dashboard-web "$LINEAGE/privkey.pem" /etc/quant-dashboard/tls/privkey.pem.next
mv -f /etc/quant-dashboard/tls/fullchain.pem.next /etc/quant-dashboard/tls/fullchain.pem
mv -f /etc/quant-dashboard/tls/privkey.pem.next /etc/quant-dashboard/tls/privkey.pem
NGINX_MODE=dedicated
if [[ -f /etc/quant-dashboard/nginx-mode ]]; then NGINX_MODE="$(cat /etc/quant-dashboard/nginx-mode)"; fi
case "$NGINX_MODE" in
  shared-system)
    systemctl is-active --quiet nginx.service || { echo 'error: system nginx.service is not active' >&2; exit 1; }
    /usr/sbin/nginx -t
    systemctl reload nginx.service
    ;;
  dedicated)
    if systemctl is-active --quiet quant-dashboard-web.service; then
      /usr/sbin/nginx -t -c /etc/quant-dashboard/nginx.conf
      systemctl reload quant-dashboard-web.service
    fi
    ;;
  *) echo 'error: invalid stored Nginx mode' >&2; exit 1;;
esac
