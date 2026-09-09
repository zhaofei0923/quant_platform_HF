#!/usr/bin/env bash
# Installs an already reviewed release. Intentionally never enables/starts a service.
set -euo pipefail
umask 077
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
PACKAGE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd -P)"
source "${PACKAGE_ROOT}/infra/dashboard/lib.sh"
DOMAIN=''; SOURCE_DIR=''; IDENTITY=''; CERT=''; KEY=''; USERS=''; USERNAME=''
NGINX_MODE='dedicated'; NGINX_WORKER_USER=''
while (($#)); do
  case "$1" in
    --domain) DOMAIN="$2"; shift 2;;
    --source-dir) SOURCE_DIR="$2"; shift 2;;
    --identity-file) IDENTITY="$2"; shift 2;;
    --tls-cert) CERT="$2"; shift 2;;
    --tls-key) KEY="$2"; shift 2;;
    --users-file) USERS="$2"; shift 2;;
    --username) USERNAME="$2"; shift 2;;
    --nginx-mode) NGINX_MODE="$2"; shift 2;;
    --nginx-worker-user) NGINX_WORKER_USER="$2"; shift 2;;
    -h|--help) echo 'Usage: install_dashboard.sh --domain HOST --source-dir ROOT --identity-file FILE --tls-cert FILE --tls-key FILE --users-file PRIVATE_YAML --username USER [--nginx-mode dedicated|shared-system] [--nginx-worker-user USER]'; exit 0;;
    *) dashboard_die "unknown argument: $1";;
  esac
done
[[ "$EUID" == 0 ]] || dashboard_die 'installation requires root'
[[ "$USERNAME" =~ ^[a-z][a-z0-9_-]{2,31}$ && -f "$USERS" && ! -L "$USERS" ]] || dashboard_die 'provide a username and private Authelia users YAML file'
PREFLIGHT_ARGS=(--domain "$DOMAIN" --source-dir "$SOURCE_DIR" --identity-file "$IDENTITY" --tls-cert "$CERT" --tls-key "$KEY" --nginx-mode "$NGINX_MODE")
if [[ -n "$NGINX_WORKER_USER" ]]; then PREFLIGHT_ARGS+=(--nginx-worker-user "$NGINX_WORKER_USER"); fi
"${SCRIPT_DIR}/preflight_dashboard.sh" "${PREFLIGHT_ARGS[@]}"
if [[ -L /opt/quant-dashboard/current ]]; then
  EXISTING_NGINX_MODE=dedicated
  if [[ -f /etc/quant-dashboard/nginx-mode ]]; then EXISTING_NGINX_MODE="$(cat /etc/quant-dashboard/nginx-mode)"; fi
  [[ "$NGINX_MODE" == "$EXISTING_NGINX_MODE" ]] || dashboard_die 'Nginx mode migration requires a separate reviewed migration'
  [[ "$DOMAIN" == "$(cat /etc/quant-dashboard/domain)" ]] || dashboard_die 'domain migration requires a separate reviewed migration; this installer upgrades one site'
  grep -Fq "subject: 'user:$USERNAME'" /etc/quant-dashboard/auth/configuration.yml || dashboard_die 'username migration is outside this in-place upgrade'
  [[ "$(jq -cS '{identity,recovery_root}' "$IDENTITY")" == "$(jq -cS '{identity,recovery_root}' /etc/quant-dashboard/identity.json)" ]] || dashboard_die 'account or runtime migration requires a separate reviewed migration'
  if [[ "$NGINX_MODE" == shared-system ]]; then
    [[ -f /etc/quant-dashboard/nginx-worker-user && "$NGINX_WORKER_USER" == "$(cat /etc/quant-dashboard/nginx-worker-user)" ]] ||
      dashboard_die 'Nginx worker user migration requires a separate reviewed migration'
  fi
fi
RELEASE="/opt/quant-dashboard/releases/$(date -u +%Y%m%dT%H%M%S)-$(sha256sum "$PACKAGE_ROOT/SHA256SUMS" | cut -c1-12)"
[[ ! -e "$RELEASE" ]] || dashboard_die 'this release already exists'
install -d -m 0755 /opt/quant-dashboard/releases
install -d -m 0700 /var/lib/quant-dashboard-admin
BACKUP="/var/lib/quant-dashboard-admin/$(basename "$RELEASE")"
install -d -m 0700 "$BACKUP"
printf '%s\n' "$NGINX_MODE" > "$BACKUP/installed-nginx-mode"
for file in nginx.conf auth-proxy.conf identity.json domain nginx-mode nginx-worker-user certificate-lineage; do [[ ! -f "/etc/quant-dashboard/$file" ]] || cp -p "/etc/quant-dashboard/$file" "$BACKUP/$file"; done
[[ ! -f /etc/quant-dashboard/auth/configuration.yml ]] || cp -p /etc/quant-dashboard/auth/configuration.yml "$BACKUP/authelia.yml"
for component in publisher auth web; do
  [[ ! -f "/etc/systemd/system/quant-dashboard-$component.service" ]] || cp -p "/etc/systemd/system/quant-dashboard-$component.service" "$BACKUP/$component.service"
done
[[ ! -f /etc/logrotate.d/quant-dashboard ]] || cp -p /etc/logrotate.d/quant-dashboard "$BACKUP/logrotate"
if [[ -L /opt/quant-dashboard/current ]]; then readlink -f /opt/quant-dashboard/current > "$BACKUP/previous-release"; fi
if [[ "$NGINX_MODE" == shared-system ]]; then
  SITE_AVAILABLE=/etc/nginx/sites-available/quant-dashboard
  SITE_ENABLED=/etc/nginx/sites-enabled/quant-dashboard
  if [[ -f "$SITE_AVAILABLE" ]]; then
    cp -p "$SITE_AVAILABLE" "$BACKUP/shared-site.conf"
    sha256sum "$SITE_AVAILABLE" | cut -d' ' -f1 > "$BACKUP/shared-site-before.sha256"
  fi
  if [[ -L "$SITE_ENABLED" ]]; then readlink "$SITE_ENABLED" > "$BACKUP/shared-site-enabled-target"; fi
fi
for group in quant-dashboard-read quant-dashboard-web quant-dashboard-auth; do
  getent group "$group" >/dev/null || groupadd --system "$group"
done
for user in quant-dashboard-publisher quant-dashboard-web quant-dashboard-auth; do
  case "$user" in quant-dashboard-publisher) group=quant-dashboard-read;; *) group="$user";; esac
  if ! id "$user" >/dev/null 2>&1; then useradd --system --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin --gid "$group" "$user"; fi
done
usermod -a -G quant-dashboard-web quant-dashboard-publisher
install -d -m 0755 "$RELEASE"
cp -a "$PACKAGE_ROOT/bin" "$PACKAGE_ROOT/web" "$PACKAGE_ROOT/infra" "$PACKAGE_ROOT/scripts" "$RELEASE/"
chown -R root:root "$RELEASE"
find "$RELEASE" -type d -exec chmod 0755 {} +
find "$RELEASE/bin" -type f -exec chmod 0755 {} +
find "$RELEASE/web" "$RELEASE/infra" -type f -exec chmod 0644 {} +
install -d -m 0755 /etc/quant-dashboard
install -d -m 0750 -o root -g quant-dashboard-auth /etc/quant-dashboard/auth
install -d -m 0750 -o root -g quant-dashboard-web /etc/quant-dashboard/tls
install -d -m 0700 -o quant-dashboard-auth -g quant-dashboard-auth /var/lib/quant-dashboard-auth
install -d -m 0700 -o quant-dashboard-publisher -g quant-dashboard-read /var/lib/quant-dashboard-publisher
install -d -m 2750 -o quant-dashboard-publisher -g quant-dashboard-web /var/lib/quant-dashboard-public /var/lib/quant-dashboard-public/data /var/lib/quant-dashboard-public/data/v1
if [[ "$NGINX_MODE" == shared-system ]]; then
  # Give the configured system Nginx worker a named read-only ACL. This avoids
  # changing its supplementary groups or restarting the shared master process.
  setfacl -R -m "u:${NGINX_WORKER_USER}:rX" /var/lib/quant-dashboard-public
  while IFS= read -r -d '' directory; do
    setfacl -m "d:u:${NGINX_WORKER_USER}:rX" "$directory"
  done < <(find /var/lib/quant-dashboard-public -type d -print0)
  install -d -m 0750 -o quant-dashboard-web -g quant-dashboard-web /var/log/quant-dashboard-web
  touch /var/log/quant-dashboard-web/access.log
  chown quant-dashboard-web:quant-dashboard-web /var/log/quant-dashboard-web/access.log
  chmod 0640 /var/log/quant-dashboard-web/access.log
fi
install -m 0600 -o quant-dashboard-publisher -g quant-dashboard-read "$IDENTITY" /etc/quant-dashboard/identity.json
install -m 0640 -o root -g quant-dashboard-auth "$USERS" /etc/quant-dashboard/auth/users.yml
install -m 0640 -o root -g quant-dashboard-web "$CERT" /etc/quant-dashboard/tls/fullchain.pem
install -m 0640 -o root -g quant-dashboard-web "$KEY" /etc/quant-dashboard/tls/privkey.pem
if [[ ! -e /etc/quant-dashboard/auth/storage-key ]]; then
  openssl rand -hex 64 > /etc/quant-dashboard/auth/storage-key
  chown root:quant-dashboard-auth /etc/quant-dashboard/auth/storage-key
  chmod 0640 /etc/quant-dashboard/auth/storage-key
fi
getfacl -R -p "$SOURCE_DIR" > "$BACKUP/source-acl-before.txt"
printf '%s\n' "$SOURCE_DIR" > "$BACKUP/source-root"
if [[ "$NGINX_MODE" == dedicated ]]; then
  dashboard_render "$RELEASE/infra/dashboard/nginx.conf.in" /etc/quant-dashboard/nginx.conf \
    DOMAIN "$DOMAIN" HTTPS_PORT 443 PORT_SUFFIX '' AUTH_PORT 9091 \
    RUN_DIR /run/quant-dashboard-web LOG_DIR /var/log/quant-dashboard-web MIME_TYPES /etc/nginx/mime.types \
    TLS_CERT /etc/quant-dashboard/tls/fullchain.pem TLS_KEY /etc/quant-dashboard/tls/privkey.pem \
    WEB_ROOT "$RELEASE/web" DATA_ROOT /var/lib/quant-dashboard-public/data AUTH_PROXY /etc/quant-dashboard/auth-proxy.conf
else
  dashboard_render "$RELEASE/infra/dashboard/nginx.shared-site.conf.in" "$BACKUP/shared-site.candidate" \
    DOMAIN "$DOMAIN" PORT_SUFFIX '' AUTH_PORT 9091 \
    TLS_CERT /etc/quant-dashboard/tls/fullchain.pem TLS_KEY /etc/quant-dashboard/tls/privkey.pem \
    WEB_ROOT "$RELEASE/web" DATA_ROOT /var/lib/quant-dashboard-public/data \
    AUTH_PROXY /etc/quant-dashboard/auth-proxy.conf ACCESS_LOG /var/log/quant-dashboard-web/access.log
fi
dashboard_render "$RELEASE/infra/dashboard/auth-proxy.conf.in" /etc/quant-dashboard/auth-proxy.conf DOMAIN "$DOMAIN" PORT_SUFFIX '' AUTH_PORT 9091
dashboard_render "$RELEASE/infra/dashboard/authelia.yml.in" /etc/quant-dashboard/auth/configuration.yml \
  DOMAIN "$DOMAIN" PORT_SUFFIX '' AUTH_PORT 9091 USERNAME "$USERNAME" \
  USERS_FILE /etc/quant-dashboard/auth/users.yml AUTH_STATE /var/lib/quant-dashboard-auth
if [[ "$NGINX_MODE" == dedicated ]]; then chmod 0644 /etc/quant-dashboard/nginx.conf; fi
chmod 0644 /etc/quant-dashboard/auth-proxy.conf
chown root:quant-dashboard-auth /etc/quant-dashboard/auth/configuration.yml
chmod 0640 /etc/quant-dashboard/auth/configuration.yml
AUTHELIA_STORAGE_ENCRYPTION_KEY_FILE=/etc/quant-dashboard/auth/storage-key "$RELEASE/bin/authelia" config validate --config /etc/quant-dashboard/auth/configuration.yml
# Grant only this selected runtime identity. Never chmod its contents or grant the web user access.
setfacl -R -m g:quant-dashboard-read:rX "$SOURCE_DIR"
while IFS= read -r -d '' directory; do setfacl -m d:g:quant-dashboard-read:rX "$directory"; done < <(find "$SOURCE_DIR" -type d -print0)
WAL_FILE="$(jq -r '.paths.wal // (.recovery_root + "/wal/events.wal")' "$IDENTITY")"
dashboard_path "$WAL_FILE"
[[ "$(realpath -m "$WAL_FILE")" == "$SOURCE_DIR/"* && "$(realpath -ms "$WAL_FILE")" == "$(realpath -m "$WAL_FILE")" ]] || dashboard_die 'WAL input must stay inside selected source without symlinks'
WAL_DIR="$(dirname "$WAL_FILE")"
if [[ ! -d "$WAL_DIR" ]]; then
  install -d -m 2750 -o "$(stat -c %u "$SOURCE_DIR")" -g quant-dashboard-read "$WAL_DIR"
fi
# Explicit observer opt-in in core later validates this group on the opened WAL fd before fchmod(0640).
# Owner write access remains unchanged; the observer group must never gain directory write access.
chgrp quant-dashboard-read "$WAL_DIR"
chmod g=rx,g+s "$WAL_DIR"
setfacl -m g::r-x,g:quant-dashboard-read:r-x,d:g::r-x,d:g:quant-dashboard-read:r-x "$WAL_DIR"
if [[ -e "$WAL_FILE" ]]; then
  [[ -f "$WAL_FILE" && ! -L "$WAL_FILE" ]] || dashboard_die 'WAL must be a regular file'
  chgrp quant-dashboard-read "$WAL_FILE"
  setfacl -m g:quant-dashboard-read:r-- "$WAL_FILE"
fi
ancestor="$(dirname "$SOURCE_DIR")"
: > "$BACKUP/ancestor-acl-before.txt"
: > "$BACKUP/ancestor-paths.txt"
while [[ "$ancestor" != / ]]; do
  printf '%s\n' "$ancestor" >> "$BACKUP/ancestor-paths.txt"
  getfacl -p "$ancestor" >> "$BACKUP/ancestor-acl-before.txt"
  setfacl -m g:quant-dashboard-read:--x "$ancestor"
  ancestor="$(dirname "$ancestor")"
done
getfacl -R -p "$SOURCE_DIR" > "$BACKUP/source-acl-after.txt"
: > "$BACKUP/ancestor-acl-after.txt"
while IFS= read -r ancestor; do getfacl -p "$ancestor" >> "$BACKUP/ancestor-acl-after.txt"; done < "$BACKUP/ancestor-paths.txt"
dashboard_render "$RELEASE/infra/dashboard/quant-dashboard-publisher.service.in" /etc/systemd/system/quant-dashboard-publisher.service SOURCE_DIR "$SOURCE_DIR"
install -m 0644 "$RELEASE/infra/dashboard/quant-dashboard-auth.service" /etc/systemd/system/quant-dashboard-auth.service
if [[ "$NGINX_MODE" == dedicated ]]; then
  install -m 0644 "$RELEASE/infra/dashboard/quant-dashboard-web.service" /etc/systemd/system/quant-dashboard-web.service
fi
install -m 0644 "$RELEASE/infra/dashboard/logrotate.conf" /etc/logrotate.d/quant-dashboard
printf '%s\n' "$DOMAIN" > /etc/quant-dashboard/domain
chmod 0644 /etc/quant-dashboard/domain
if [[ "$(basename "$CERT")" == fullchain.pem && "$(basename "$KEY")" == privkey.pem &&
      "$(dirname "$CERT")" == "$(dirname "$KEY")" && "$(dirname "$CERT")" == /etc/letsencrypt/live/* ]]; then
  printf '%s\n' "$(dirname "$CERT")" > /etc/quant-dashboard/certificate-lineage
  chmod 0644 /etc/quant-dashboard/certificate-lineage
else
  rm -f -- /etc/quant-dashboard/certificate-lineage
fi
ln -s "$RELEASE" /opt/quant-dashboard/current.new
mv -Tf /opt/quant-dashboard/current.new /opt/quant-dashboard/current
systemctl daemon-reload
if [[ "$NGINX_MODE" == shared-system ]]; then
  restore_shared_site() {
    local restore_tmp
    if [[ -f "$BACKUP/shared-site.conf" ]]; then
      restore_tmp="$(mktemp /etc/nginx/sites-available/.quant-dashboard.restore.XXXXXX)"
      install -m 0644 "$BACKUP/shared-site.conf" "$restore_tmp"
      mv -Tf "$restore_tmp" "$SITE_AVAILABLE"
    else
      rm -f -- "$SITE_AVAILABLE"
    fi
    rm -f -- "$SITE_ENABLED"
    if [[ -f "$BACKUP/shared-site-enabled-target" ]]; then
      ln -s "$(cat "$BACKUP/shared-site-enabled-target")" "$SITE_ENABLED"
    fi
    if [[ -f "$BACKUP/auth-proxy.conf" ]]; then
      install -m 0644 "$BACKUP/auth-proxy.conf" /etc/quant-dashboard/auth-proxy.conf
    fi
  }
  if [[ -f "$BACKUP/shared-site-before.sha256" ]]; then
    [[ -f "$SITE_AVAILABLE" && ! -L "$SITE_AVAILABLE" &&
       "$(sha256sum "$SITE_AVAILABLE" | cut -d' ' -f1)" == "$(cat "$BACKUP/shared-site-before.sha256")" ]] ||
      dashboard_die 'managed Nginx site changed during installation; refusing to overwrite it'
  else
    [[ ! -e "$SITE_AVAILABLE" && ! -L "$SITE_AVAILABLE" ]] ||
      dashboard_die 'sites-available target appeared during installation; refusing to overwrite it'
  fi
  if [[ -L "$SITE_ENABLED" ]]; then
    [[ "$(realpath -e "$SITE_ENABLED")" == "$SITE_AVAILABLE" ]] ||
      dashboard_die 'sites-enabled target changed during installation; refusing to overwrite it'
  else
    [[ ! -e "$SITE_ENABLED" ]] || dashboard_die 'sites-enabled target appeared during installation; refusing to overwrite it'
  fi
  SITE_TMP="$(mktemp /etc/nginx/sites-available/.quant-dashboard.XXXXXX)"
  install -m 0644 "$BACKUP/shared-site.candidate" "$SITE_TMP"
  mv -Tf "$SITE_TMP" "$SITE_AVAILABLE"
  if [[ ! -L "$SITE_ENABLED" ]]; then ln -s "$SITE_AVAILABLE" "$SITE_ENABLED"; fi
  if ! /usr/sbin/nginx -t; then
    restore_shared_site
    /usr/sbin/nginx -t >/dev/null 2>&1 || true
    dashboard_die 'dashboard site failed nginx -t; prior dashboard site restored and system Nginx not reloaded'
  fi
  if ! systemctl reload nginx.service; then
    restore_shared_site
    if /usr/sbin/nginx -t >/dev/null 2>&1; then systemctl reload nginx.service || true; fi
    dashboard_die 'system Nginx reload failed; prior dashboard site restored'
  fi
  sha256sum "$SITE_AVAILABLE" | cut -d' ' -f1 > "$BACKUP/installed-shared-site.sha256"
  readlink "$SITE_ENABLED" > "$BACKUP/installed-shared-site-enabled-target"
  printf '%s\n' "$NGINX_WORKER_USER" > /etc/quant-dashboard/nginx-worker-user
  chmod 0644 /etc/quant-dashboard/nginx-worker-user
fi
printf '%s\n' "$NGINX_MODE" > /etc/quant-dashboard/nginx-mode
chmod 0644 /etc/quant-dashboard/nginx-mode
if [[ "$NGINX_MODE" == shared-system ]]; then
  printf 'Installed %s\nBackup: %s\nDashboard site passed nginx -t and system nginx.service was reloaded. Auth and publisher remain stopped/disabled; no quant-dashboard-web service was installed. Trading services and unrelated Nginx sites were not changed.\n' "$RELEASE" "$BACKUP"
else
  printf 'Installed %s\nBackup: %s\nDashboard services remain stopped/disabled. Complete review and smoke checks before explicitly enabling them. Trading services were not changed.\n' "$RELEASE" "$BACKUP"
fi
