#!/usr/bin/env bash
# Optional root-only test: transient, random systemd unit; no production state.
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd -P)"
source "$ROOT/infra/dashboard/lib.sh"
AUTHELIA_BIN="${DASHBOARD_TEST_AUTHELIA_BIN:-}"
NGINX_BIN="${DASHBOARD_TEST_NGINX_BIN:-}"
MIME_TYPES="${DASHBOARD_TEST_MIME_TYPES:-/etc/nginx/mime.types}"
if [[ "$EUID" != 0 || "$(cat /proc/1/comm)" != systemd || ! -x "$AUTHELIA_BIN" || ! -x "$NGINX_BIN" || ! -r "$MIME_TYPES" ]]; then
  echo 'SKIP: root, systemd and explicitly supplied Authelia/Nginx executables required'
  exit 77
fi
for tool in systemd-run systemctl setpriv curl jq openssl; do dashboard_require "$tool"; done
TMP="$(mktemp -d /tmp/quant-dashboard-session.XXXXXX)"
UNIT="quant-dashboard-fixture-$(openssl rand -hex 6).service"
NGINX_PID=''
cleanup() {
  systemctl stop "$UNIT" >/dev/null 2>&1 || true
  systemctl reset-failed "$UNIT" >/dev/null 2>&1 || true
  [[ -z "$NGINX_PID" ]] || { kill "$NGINX_PID" 2>/dev/null || true; wait "$NGINX_PID" 2>/dev/null || true; }
  rm -rf -- "$TMP"
}
trap cleanup EXIT
DOMAIN=dashboard.example.test
AUTH_PORT=19092
WEB_PORT=18444
FIXTURE_UID="$(id -u nobody)"
FIXTURE_GID="$(id -g nobody)"
for port in "$AUTH_PORT" "$WEB_PORT"; do
  ! ss -ltnH "sport = :$port" | grep -q . || dashboard_die 'fixture port is already occupied'
done
mkdir -p "$TMP/run" "$TMP/log" "$TMP/web" "$TMP/data/v1" "$TMP/state" "$TMP/bin"
# Executables may have been downloaded to a private directory. Copy only these
# verified executables into the isolated fixture; never relax the parent path.
install -m 0755 "$AUTHELIA_BIN" "$TMP/bin/authelia"
install -m 0755 "$NGINX_BIN" "$TMP/bin/nginx"
install -m 0644 "$MIME_TYPES" "$TMP/mime.types"
printf '<html>fixture</html>\n' > "$TMP/web/index.html"
printf '{"fixture":true}\n' > "$TMP/data/v1/current.json"
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$TMP/key.pem" -out "$TMP/cert.pem" -days 1 \
  -subj "/CN=$DOMAIN" -addext "subjectAltName=DNS:$DOMAIN" >/dev/null 2>&1
"$AUTHELIA_BIN" crypto hash generate argon2 --random --random.length 40 > "$TMP/generated-password"
PASSWORD="$(sed -n 's/^Random Password: //p' "$TMP/generated-password")"
DIGEST="$(sed -n 's/^Digest: //p' "$TMP/generated-password")"
[[ -n "$PASSWORD" && "$DIGEST" == '$argon2id$'* ]]
printf "users:\n  tester:\n    disabled: false\n    displayname: 'Temporary test user'\n    password: '%s'\n    email: 'fixture@example.test'\n    groups: []\n" "$DIGEST" > "$TMP/users.yml"
printf '%s' "$PASSWORD" > "$TMP/password"
unset PASSWORD DIGEST
openssl rand -hex 64 > "$TMP/storage-key"
jq -n --rawfile password "$TMP/password" '{username:"tester",password:$password,keepMeLoggedIn:false}' > "$TMP/login.json"
dashboard_render "$ROOT/infra/dashboard/authelia.yml.in" "$TMP/auth.yml" DOMAIN "$DOMAIN" PORT_SUFFIX ":$WEB_PORT" AUTH_PORT "$AUTH_PORT" USERNAME tester USERS_FILE "$TMP/users.yml" AUTH_STATE "$TMP/state"
dashboard_render "$ROOT/infra/dashboard/auth-proxy.conf.in" "$TMP/auth-proxy.conf" DOMAIN "$DOMAIN" PORT_SUFFIX ":$WEB_PORT" AUTH_PORT "$AUTH_PORT"
dashboard_render "$ROOT/infra/dashboard/nginx.conf.in" "$TMP/nginx.conf" DOMAIN "$DOMAIN" PORT_SUFFIX ":$WEB_PORT" HTTPS_PORT "127.0.0.1:$WEB_PORT" AUTH_PORT "$AUTH_PORT" RUN_DIR "$TMP/run" LOG_DIR "$TMP/log" MIME_TYPES "$TMP/mime.types" TLS_CERT "$TMP/cert.pem" TLS_KEY "$TMP/key.pem" WEB_ROOT "$TMP/web" DATA_ROOT "$TMP/data" AUTH_PROXY "$TMP/auth-proxy.conf"
chown -R "$FIXTURE_UID:$FIXTURE_GID" "$TMP"
systemd-run --quiet --collect --unit "$UNIT" \
  --property "User=$FIXTURE_UID" --property "Group=$FIXTURE_GID" \
  --property RuntimeMaxSec=8s --property TimeoutStopSec=1s \
  --property Restart=always --property RestartSec=1s \
  --property NoNewPrivileges=yes --property PrivateTmp=yes \
  --property "BindReadOnlyPaths=$TMP" --property "ReadWritePaths=$TMP/state" \
  --setenv "AUTHELIA_STORAGE_ENCRYPTION_KEY_FILE=$TMP/storage-key" \
  "$TMP/bin/authelia" --config "$TMP/auth.yml"
for ((i=0;i<100;i++)); do
  curl --noproxy '*' --max-time 1 -fsS "http://127.0.0.1:$AUTH_PORT/api/health" >/dev/null 2>&1 && break
  systemctl is-active --quiet "$UNIT" || dashboard_die 'transient auth unit failed to start'
  sleep 0.1
done
INITIAL_PID="$(systemctl show --value -p MainPID "$UNIT")"
[[ "$INITIAL_PID" != 0 ]]
setpriv --reuid "$FIXTURE_UID" --regid "$FIXTURE_GID" --clear-groups \
  "$TMP/bin/nginx" -p "$TMP/" -c "$TMP/nginx.conf" -g 'daemon off;' > "$TMP/nginx.log" 2>&1 & NGINX_PID=$!
CURL=(curl --noproxy '*' --cacert "$TMP/cert.pem" --resolve "$DOMAIN:$WEB_PORT:127.0.0.1" --silent --show-error --max-time 3)
URL="https://$DOMAIN:$WEB_PORT"
for ((i=0;i<50;i++)); do "${CURL[@]}" "$URL/auth/" >/dev/null 2>&1 && break; sleep 0.1; done
status() { "${CURL[@]}" -o "$TMP/body" -w '%{http_code}' "$@"; }
[[ "$(status -c "$TMP/cookies" -H 'Content-Type: application/json' --data-binary "@$TMP/login.json" "$URL/auth/api/firstfactor")" == 200 ]] || dashboard_die 'fixture login failed'
awk '$6 == "quant_dashboard_session" {print "Cookie: " $6 "=" $7}' "$TMP/cookies" > "$TMP/replayed-cookie-header"
[[ -s "$TMP/replayed-cookie-header" ]]
[[ "$(status -H "@$TMP/replayed-cookie-header" "$URL/data/v1/current.json")" == 200 ]]
authorized=0
rejected=0
unavailable=0
for ((i=0;i<80;i++)); do
  code="$(status -H "@$TMP/replayed-cookie-header" "$URL/data/v1/current.json")"
  case "$code" in
    200) ((authorized+=1));;
    401) rejected=1; break;;
    500) ((unavailable+=1));;
    *) dashboard_die "unexpected session lifecycle response: $code";;
  esac
  sleep 0.2
done
[[ "$rejected" == 1 && "$authorized" -gt 1 ]] || dashboard_die 'continuously used old cookie survived the bounded auth service lifecycle'
FINAL_PID="$(systemctl show --value -p MainPID "$UNIT")"
[[ "$FINAL_PID" != 0 && "$FINAL_PID" != "$INITIAL_PID" ]] || dashboard_die 'auth service did not actually restart'
for ((i=0;i<3;i++)); do [[ "$(status -H "@$TMP/replayed-cookie-header" "$URL/data/v1/current.json")" == 401 ]]; done
printf 'PASS: actual transient systemd Authelia service with numeric unprivileged UID, RuntimeMaxSec=8s, TimeoutStopSec=1s and Restart=always; continuously replayed cookie authorized %s requests then permanently rejected (401) after process replacement; %s fail-closed requests during restart. Production retains 8h cookie and 7h59min55s service bound + 5s stop timeout.\n' "$authorized" "$unavailable"
