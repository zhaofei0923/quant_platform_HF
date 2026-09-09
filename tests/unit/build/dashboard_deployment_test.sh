#!/usr/bin/env bash
# Optional real Nginx/Authelia protocol test. Binds loopback only; no root or production state.
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd -P)"
source "$ROOT/infra/dashboard/lib.sh"
grep -Fq -- '--watch-seconds 1' "$ROOT/infra/dashboard/quant-dashboard-publisher.service.in" ||
  dashboard_die 'publisher service must publish private schema v2 observations every second'
AUTHELIA_BIN="${DASHBOARD_TEST_AUTHELIA_BIN:-}"
NGINX_BIN="${DASHBOARD_TEST_NGINX_BIN:-}"
MIME_TYPES="${DASHBOARD_TEST_MIME_TYPES:-/etc/nginx/mime.types}"
if [[ ! -x "$AUTHELIA_BIN" || ! -x "$NGINX_BIN" || ! -f "$MIME_TYPES" ]]; then
  echo 'SKIP: set DASHBOARD_TEST_AUTHELIA_BIN, DASHBOARD_TEST_NGINX_BIN and DASHBOARD_TEST_MIME_TYPES'; exit 77
fi
for program in jq curl openssl; do dashboard_require "$program"; done
TMP="$(mktemp -d /tmp/quant-dashboard-http-test.XXXXXX)"
AUTH_PID=''; NGINX_PID=''
cleanup() {
  [[ -z "$NGINX_PID" ]] || kill "$NGINX_PID" 2>/dev/null || true
  [[ -z "$AUTH_PID" ]] || kill "$AUTH_PID" 2>/dev/null || true
  [[ -z "$NGINX_PID" ]] || wait "$NGINX_PID" 2>/dev/null || true
  [[ -z "$AUTH_PID" ]] || wait "$AUTH_PID" 2>/dev/null || true
  rm -rf -- "$TMP"
}
trap cleanup EXIT
AUTH_PORT="${DASHBOARD_TEST_AUTH_PORT:-19091}"
WEB_PORT="${DASHBOARD_TEST_WEB_PORT:-18443}"
DOMAIN=dashboard.example.test
mkdir -p "$TMP/run" "$TMP/log" "$TMP/web" "$TMP/data/v1" "$TMP/state"
printf '<html><body>PRIVATE DASHBOARD</body></html>\n' > "$TMP/web/index.html"
printf '{"schema_version":1,"sentinel":"authenticated-data"}\n' > "$TMP/data/v1/current.json"
printf 'PRIVATE_NOT_SERVED\n' > "$TMP/data/not-allowed.json"
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$TMP/key.pem" -out "$TMP/cert.pem" -days 1 \
  -subj "/CN=$DOMAIN" -addext "subjectAltName=DNS:$DOMAIN" >/dev/null 2>&1
"$AUTHELIA_BIN" crypto hash generate argon2 --random --random.length 40 > "$TMP/generated-password"
PASSWORD="$(sed -n 's/^Random Password: //p' "$TMP/generated-password")"
DIGEST="$(sed -n 's/^Digest: //p' "$TMP/generated-password")"
[[ -n "$PASSWORD" && "$DIGEST" == '$argon2id$'* ]] || dashboard_die 'unable to generate temporary fixture credentials'
printf "users:\n  tester:\n    disabled: false\n    displayname: 'Temporary test user'\n    password: '%s'\n    email: 'fixture@example.test'\n    groups: []\n" "$DIGEST" > "$TMP/users.yml"
printf '%s' "$PASSWORD" > "$TMP/password"
unset PASSWORD DIGEST
openssl rand -hex 64 > "$TMP/storage-key"
dashboard_render "$ROOT/infra/dashboard/authelia.yml.in" "$TMP/auth.yml" DOMAIN "$DOMAIN" PORT_SUFFIX ":$WEB_PORT" AUTH_PORT "$AUTH_PORT" USERNAME tester USERS_FILE "$TMP/users.yml" AUTH_STATE "$TMP/state"
dashboard_render "$ROOT/infra/dashboard/auth-proxy.conf.in" "$TMP/auth-proxy.conf" DOMAIN "$DOMAIN" PORT_SUFFIX ":$WEB_PORT" AUTH_PORT "$AUTH_PORT"
dashboard_render "$ROOT/infra/dashboard/nginx.conf.in" "$TMP/nginx.conf" DOMAIN "$DOMAIN" PORT_SUFFIX ":$WEB_PORT" HTTPS_PORT "127.0.0.1:$WEB_PORT" AUTH_PORT "$AUTH_PORT" RUN_DIR "$TMP/run" LOG_DIR "$TMP/log" MIME_TYPES "$MIME_TYPES" TLS_CERT "$TMP/cert.pem" TLS_KEY "$TMP/key.pem" WEB_ROOT "$TMP/web" DATA_ROOT "$TMP/data" AUTH_PROXY "$TMP/auth-proxy.conf"
export AUTHELIA_STORAGE_ENCRYPTION_KEY_FILE="$TMP/storage-key"
"$AUTHELIA_BIN" config validate --config "$TMP/auth.yml" > "$TMP/config-check.log" 2>&1 || { cat "$TMP/config-check.log"; exit 1; }
"$AUTHELIA_BIN" --config "$TMP/auth.yml" > "$TMP/auth.log" 2>&1 & AUTH_PID=$!
for ((i=0;i<100;i++)); do
  if curl --noproxy '*' -fsS "http://127.0.0.1:$AUTH_PORT/api/health" >/dev/null 2>&1; then break; fi
  kill -0 "$AUTH_PID" 2>/dev/null || { cat "$TMP/auth.log"; exit 1; }
  sleep 0.1
done
"$NGINX_BIN" -p "$TMP/" -t -c "$TMP/nginx.conf" > "$TMP/nginx-check.log" 2>&1 || { cat "$TMP/nginx-check.log"; exit 1; }
"$NGINX_BIN" -p "$TMP/" -c "$TMP/nginx.conf" -g 'daemon off;' > "$TMP/nginx.log" 2>&1 & NGINX_PID=$!
CURL=(curl --noproxy '*' --cacert "$TMP/cert.pem" --resolve "$DOMAIN:$WEB_PORT:127.0.0.1" --silent --show-error --max-time 10)
URL="https://$DOMAIN:$WEB_PORT"
for ((i=0;i<50;i++)); do "${CURL[@]}" "$URL/auth/" >/dev/null 2>&1 && break; sleep 0.1; done
status() { "${CURL[@]}" -o "$TMP/body" -D "$TMP/headers" -w '%{http_code}' "$@"; }
expect() { local wanted="$1"; shift; local got; got="$(status "$@")"; [[ "$got" == "$wanted" ]] || dashboard_die "expected HTTP $wanted, got $got for protocol test"; }
expect 302 "$URL/"
expect 401 "$URL/data/v1/current.json"
expect 200 "$URL/auth/"
# A normal Authelia login page loads more than ten JavaScript modules concurrently.
# Keep those requests below the general connection ceiling while the first-factor
# endpoint retains its separate, tighter brute-force controls.
"${CURL[@]}" "$URL/auth/" > "$TMP/auth-index.html"
auth_script="$(sed -n 's#.*src="\./\([^"]*\.js\)".*#\1#p' "$TMP/auth-index.html" | head -n 1)"
[[ -n "$auth_script" ]] || dashboard_die 'unable to locate the Authelia login script'
asset_pids=()
for ((i=0;i<16;i++)); do
  "${CURL[@]}" -H 'Range: bytes=0-65535' --limit-rate 32k -o /dev/null \
    -w '%{http_code}\n' "$URL/auth/$auth_script" > "$TMP/auth-asset-status.$i" &
  asset_pids+=("$!")
done
asset_request_failed=0
for asset_pid in "${asset_pids[@]}"; do
  wait "$asset_pid" || asset_request_failed=1
done
[[ "$asset_request_failed" == 0 ]] || dashboard_die 'Authelia login asset request failed'
cat "$TMP"/auth-asset-status.* > "$TMP/auth-asset-statuses"
if grep -Evq '^(200|206)$' "$TMP/auth-asset-statuses"; then
  dashboard_die 'Authelia login assets were throttled by the general connection limit'
fi
expect 200 "$URL/auth/logout"
{ printf 'Authorization: Basic '; { printf 'tester:'; cat "$TMP/password"; } | base64 -w0; printf '\n'; } > "$TMP/basic-header"
expect 401 -H "@$TMP/basic-header" "$URL/data/v1/current.json"
jq -n --rawfile password "$TMP/password" '{username:"tester",password:$password,keepMeLoggedIn:false}' > "$TMP/login.json"
expect 200 -c "$TMP/cookies" -H 'Content-Type: application/json' --data-binary "@$TMP/login.json" "$URL/auth/api/firstfactor"
grep -qi '^Set-Cookie:.*HttpOnly' "$TMP/headers"
grep -qi '^Set-Cookie:.*Secure' "$TMP/headers"
cookie_expiry="$(awk '$6 == "quant_dashboard_session" {print $5}' "$TMP/cookies")"
remaining="$((cookie_expiry - $(date +%s)))"
((remaining >= 28790 && remaining <= 28800)) || dashboard_die 'production cookie lifetime is not eight hours'
expect 200 -b "$TMP/cookies" "$URL/data/v1/current.json"
jq -e '.sentinel == "authenticated-data"' "$TMP/body" >/dev/null
grep -qi '^Cache-Control: no-store' "$TMP/headers"
expect 200 -b "$TMP/cookies" "$URL/"
grep -q 'PRIVATE DASHBOARD' "$TMP/body"
expect 404 -b "$TMP/cookies" "$URL/data/not-allowed.json"
ln -s "$TMP/data/not-allowed.json" "$TMP/data/v1/days.json"
expect 403 -b "$TMP/cookies" "$URL/data/v1/days.json"
expect 400 -b "$TMP/cookies" "$URL/data/v1/../../identity.json" --path-as-is
expect 400 -b "$TMP/cookies" "$URL/data/v1/%2e%2e/%2e%2e/identity.json" --path-as-is
expect 403 -b "$TMP/cookies" -X POST "$URL/data/v1/current.json"
expect 404 -b "$TMP/cookies" "$URL/internal/authz"
printf '{}' > "$TMP/logout.json"
expect 200 -b "$TMP/cookies" -H 'Content-Type: application/json' --data-binary "@$TMP/logout.json" "$URL/auth/api/logout"
expect 401 -b "$TMP/cookies" "$URL/data/v1/current.json"
openssl rand -hex 24 > "$TMP/incorrect-password"
jq -n --rawfile password "$TMP/incorrect-password" '{username:"tester",password:$password,keepMeLoggedIn:false}' > "$TMP/incorrect-login.json"
limited=0
for ((i=0;i<8;i++)); do
  [[ "$(status -H 'Content-Type: application/json' --data-binary "@$TMP/incorrect-login.json" "$URL/auth/api/firstfactor")" != 429 ]] || limited=1
done
[[ "$limited" == 1 ]] || dashboard_die 'login rate limit did not trigger'
# Authentication service failure must never serve previously published private JSON.
kill "$AUTH_PID"; wait "$AUTH_PID" || true; AUTH_PID=''
expect 500 -b "$TMP/cookies" "$URL/data/v1/current.json"
# Shorten only the private fixture inactivity threshold. Authelia expiration
# slides on requests; a separate systemd test verifies the absolute upper bound.
sed -i "s/inactivity: '15m'/inactivity: '2s'/" "$TMP/auth.yml"
kill "$NGINX_PID"; wait "$NGINX_PID" || true; NGINX_PID=''
"$AUTHELIA_BIN" --config "$TMP/auth.yml" > "$TMP/auth-short-ttl.log" 2>&1 & AUTH_PID=$!
for ((i=0;i<100;i++)); do
  if curl --noproxy '*' -fsS "http://127.0.0.1:$AUTH_PORT/api/health" >/dev/null 2>&1; then break; fi
  kill -0 "$AUTH_PID" 2>/dev/null || { cat "$TMP/auth-short-ttl.log"; exit 1; }
  sleep 0.1
done
"$NGINX_BIN" -p "$TMP/" -c "$TMP/nginx.conf" -g 'daemon off;' > "$TMP/nginx-ttl.log" 2>&1 & NGINX_PID=$!
for ((i=0;i<50;i++)); do "${CURL[@]}" "$URL/auth/" >/dev/null 2>&1 && break; sleep 0.1; done
expect 200 -c "$TMP/ttl-cookies" -H 'Content-Type: application/json' --data-binary "@$TMP/login.json" "$URL/auth/api/firstfactor"
awk '$6 == "quant_dashboard_session" {print "Cookie: " $6 "=" $7}' "$TMP/ttl-cookies" > "$TMP/replayed-cookie-header"
[[ -s "$TMP/replayed-cookie-header" ]]
expect 200 -H "@$TMP/replayed-cookie-header" "$URL/data/v1/current.json"
sleep 3
# Replay the expired cookie explicitly, so this checks server expiry rather than curl discarding it.
expect 401 -H "@$TMP/replayed-cookie-header" "$URL/data/v1/current.json"
printf 'PASS: actual Nginx + Authelia config, HTTPS, login/logout routes, Secure/HttpOnly eight-hour cookie, Basic bypass rejection, auth-gated JSON, no-store, symlink/traversal denial, write denial, logout invalidation, rate limit, fail-closed auth outage, server-side inactivity rejection of explicitly replayed cookies.\n'
