#!/usr/bin/env bash
# Roll back dashboard code/config only. Preserve credentials, history and trading state.
set -euo pipefail
umask 077
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
source "${SCRIPT_DIR}/../../infra/dashboard/lib.sh"
[[ "$EUID" == 0 && ( "$#" == 2 || "$#" == 3 ) && "$1" == --backup-dir ]] ||
  dashboard_die 'Usage (root): rollback_dashboard.sh --backup-dir /var/lib/quant-dashboard-admin/RELEASE [--restore-source-permissions]'
RESTORE_PERMISSIONS=0
if [[ "$#" == 3 ]]; then
  [[ "$3" == --restore-source-permissions ]] || dashboard_die 'unknown rollback option'
  RESTORE_PERMISSIONS=1
fi
BACKUP="$(realpath -e "$2")"
[[ "$BACKUP" == /var/lib/quant-dashboard-admin/* && -f "$BACKUP/source-root" ]] || dashboard_die 'invalid dashboard backup'
INSTALLED_MODE=dedicated
if [[ -f "$BACKUP/installed-nginx-mode" ]]; then INSTALLED_MODE="$(cat "$BACKUP/installed-nginx-mode")"; fi
dashboard_nginx_mode "$INSTALLED_MODE"
PREVIOUS=''; CURRENT_ACL=''; CURRENT_ANCESTORS=''; CURRENT_SITE=''; CURRENT_PROXY=''
cleanup() { rm -f -- ${CURRENT_ACL:+"$CURRENT_ACL"} ${CURRENT_ANCESTORS:+"$CURRENT_ANCESTORS"} ${CURRENT_SITE:+"$CURRENT_SITE"} ${CURRENT_PROXY:+"$CURRENT_PROXY"}; }
trap cleanup EXIT
if [[ -f "$BACKUP/previous-release" ]]; then
  PREVIOUS="$(cat "$BACKUP/previous-release")"
  [[ "$PREVIOUS" == /opt/quant-dashboard/releases/* && -d "$PREVIOUS" && "$(realpath "$PREVIOUS")" == "$PREVIOUS" ]] || dashboard_die 'invalid previous release'
fi
if [[ "$RESTORE_PERMISSIONS" == 1 ]]; then
  SOURCE_DIR="$(cat "$BACKUP/source-root")"
  dashboard_path "$SOURCE_DIR"
  CURRENT_ACL="$(mktemp)"; CURRENT_ANCESTORS="$(mktemp)"
  getfacl -R -p "$SOURCE_DIR" > "$CURRENT_ACL"
  while IFS= read -r ancestor; do getfacl -p "$ancestor" >> "$CURRENT_ANCESTORS"; done < "$BACKUP/ancestor-paths.txt"
  cmp -s "$CURRENT_ACL" "$BACKUP/source-acl-after.txt" && cmp -s "$CURRENT_ANCESTORS" "$BACKUP/ancestor-acl-after.txt" ||
    dashboard_die 'source permissions or file set changed after installation; refusing to overwrite later changes (code-only rollback remains available)'
fi

if [[ "$INSTALLED_MODE" == shared-system ]]; then
  SITE_AVAILABLE=/etc/nginx/sites-available/quant-dashboard
  SITE_ENABLED=/etc/nginx/sites-enabled/quant-dashboard
  [[ -f "$BACKUP/installed-shared-site.sha256" && -f "$SITE_AVAILABLE" && ! -L "$SITE_AVAILABLE" &&
     "$(sha256sum "$SITE_AVAILABLE" | cut -d' ' -f1)" == "$(cat "$BACKUP/installed-shared-site.sha256")" ]] ||
    dashboard_die 'managed dashboard site changed after installation; refusing to overwrite later changes'
  [[ -L "$SITE_ENABLED" && "$(readlink "$SITE_ENABLED")" == "$(cat "$BACKUP/installed-shared-site-enabled-target")" &&
     "$(realpath -e "$SITE_ENABLED")" == "$SITE_AVAILABLE" ]] ||
    dashboard_die 'managed dashboard enable link changed after installation; refusing to overwrite it'
  CURRENT_SITE="$(mktemp)"; CURRENT_PROXY="$(mktemp)"
  cp -p "$SITE_AVAILABLE" "$CURRENT_SITE"
  cp -p /etc/quant-dashboard/auth-proxy.conf "$CURRENT_PROXY"
  CURRENT_RELEASE="$(readlink -f /opt/quant-dashboard/current)"
  restore_current_shared_site() {
    local restore_tmp
    restore_tmp="$(mktemp /etc/nginx/sites-available/.quant-dashboard.restore.XXXXXX)"
    install -m 0644 "$CURRENT_SITE" "$restore_tmp"
    mv -Tf "$restore_tmp" "$SITE_AVAILABLE"
    rm -f -- "$SITE_ENABLED"
    ln -s "$(cat "$BACKUP/installed-shared-site-enabled-target")" "$SITE_ENABLED"
    install -m 0644 "$CURRENT_PROXY" /etc/quant-dashboard/auth-proxy.conf
    ln -s "$CURRENT_RELEASE" /opt/quant-dashboard/current.rollback-current
    mv -Tf /opt/quant-dashboard/current.rollback-current /opt/quant-dashboard/current
  }
  for unit in quant-dashboard-publisher.service quant-dashboard-auth.service; do
    if systemctl cat "$unit" >/dev/null 2>&1; then systemctl stop "$unit"; fi
  done
else
  systemctl stop quant-dashboard-web.service quant-dashboard-publisher.service quant-dashboard-auth.service
fi

if [[ -n "$PREVIOUS" ]]; then
  for file in nginx.conf auth-proxy.conf domain nginx-mode nginx-worker-user certificate-lineage; do
    [[ ! -f "$BACKUP/$file" ]] || install -m 0644 "$BACKUP/$file" "/etc/quant-dashboard/$file"
  done
  install -m 0600 -o quant-dashboard-publisher -g quant-dashboard-read "$BACKUP/identity.json" /etc/quant-dashboard/identity.json
  install -m 0640 -o root -g quant-dashboard-auth "$BACKUP/authelia.yml" /etc/quant-dashboard/auth/configuration.yml
  for component in publisher auth web; do
    [[ ! -f "$BACKUP/$component.service" ]] || install -m 0644 "$BACKUP/$component.service" "/etc/systemd/system/quant-dashboard-$component.service"
  done
  [[ ! -f "$BACKUP/logrotate" ]] || install -m 0644 "$BACKUP/logrotate" /etc/logrotate.d/quant-dashboard
  ln -s "$PREVIOUS" /opt/quant-dashboard/current.rollback
  mv -Tf /opt/quant-dashboard/current.rollback /opt/quant-dashboard/current
  systemctl daemon-reload
else
  if [[ "$INSTALLED_MODE" == shared-system ]]; then
    systemctl disable quant-dashboard-publisher.service quant-dashboard-auth.service >/dev/null 2>&1 || true
  else
    systemctl disable quant-dashboard-web.service quant-dashboard-publisher.service quant-dashboard-auth.service
  fi
fi

if [[ "$INSTALLED_MODE" == shared-system ]]; then
  if [[ -n "$PREVIOUS" ]]; then
    [[ -f "$BACKUP/shared-site.conf" && -f "$BACKUP/shared-site-enabled-target" ]] || dashboard_die 'shared-mode previous site backup is incomplete'
    RESTORE_TMP="$(mktemp /etc/nginx/sites-available/.quant-dashboard.rollback.XXXXXX)"
    install -m 0644 "$BACKUP/shared-site.conf" "$RESTORE_TMP"
    mv -Tf "$RESTORE_TMP" "$SITE_AVAILABLE"
    rm -f -- "$SITE_ENABLED"
    ln -s "$(cat "$BACKUP/shared-site-enabled-target")" "$SITE_ENABLED"
  else
    rm -f -- "$SITE_ENABLED"
    rm -f -- "$SITE_AVAILABLE"
  fi
  if ! /usr/sbin/nginx -t; then
    restore_current_shared_site
    dashboard_die 'previous dashboard site failed nginx -t; current dashboard site restored and system Nginx not reloaded'
  fi
  if ! systemctl reload nginx.service; then
    restore_current_shared_site
    if /usr/sbin/nginx -t >/dev/null 2>&1; then systemctl reload nginx.service || true; fi
    dashboard_die 'system Nginx reload failed; current dashboard site restored'
  fi
fi
if [[ "$RESTORE_PERMISSIONS" == 1 ]]; then
  setfacl --restore="$BACKUP/source-acl-before.txt"
  setfacl --restore="$BACKUP/ancestor-acl-before.txt"
  echo 'Restored recorded pre-install source/ancestor permissions. Newly created directories were not deleted.'
fi
printf 'Dashboard rollback complete%s. Relevant dashboard services remain stopped. Credentials, SQLite, publisher history, trading state and unrelated Nginx sites were preserved.\n' "${PREVIOUS:+: $PREVIOUS}"
