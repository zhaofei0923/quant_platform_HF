#!/usr/bin/env bash
# Helpers shared by explicit dashboard deployment operations. Never source an .env file.
dashboard_die() { printf 'error: %s\n' "$*" >&2; exit 1; }
dashboard_require() { command -v "$1" >/dev/null || dashboard_die "required program: $1"; }
dashboard_domain() {
  [[ "$1" =~ ^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$ && "$1" == *.* && "$1" != *..* && ! "$1" =~ ^[0-9.]+$ ]] ||
    dashboard_die 'use a dedicated lowercase DNS hostname (not an IP address)';
}
dashboard_path() {
  [[ "$1" =~ ^/[A-Za-z0-9_./-]+$ && "$1" != / && "$1" != */../* && "$1" != */.. ]] ||
    dashboard_die "unsafe or unsupported absolute path: $1 (spaces are not supported by deployment units)";
}
dashboard_nginx_mode() {
  case "$1" in
    dedicated|shared-system) ;;
    *) dashboard_die 'nginx mode must be dedicated or shared-system';;
  esac
}
dashboard_nginx_worker_user() {
  [[ "$1" =~ ^[a-z_][a-z0-9_-]{0,31}$ ]] ||
    dashboard_die 'shared-system mode requires a valid --nginx-worker-user';
}
# Return success when an enabled Nginx source other than this dashboard's
# managed site declares the exact dashboard hostname.
dashboard_nginx_domain_conflict() {
  local dump_file="$1" domain="$2" managed_enabled="$3" managed_available="$4"
  awk -v domain="$domain" -v managed_enabled="$managed_enabled" \
      -v managed_available="$managed_available" '
    /^# configuration file .*:$/ {
      source = $0
      sub(/^# configuration file /, "", source)
      sub(/:$/, "", source)
      next
    }
    source == managed_enabled || source == managed_available { next }
    {
      line = $0
      sub(/#.*/, "", line)
      while (match(line, /server_name[[:space:]]+[^;]+;/)) {
        directive = substr(line, RSTART, RLENGTH)
        gsub(/;/, " ", directive)
        count = split(directive, token, /[[:space:]]+/)
        for (i = 2; i <= count; ++i) {
          if (token[i] == domain) {
            print source
            found = 1
          }
        }
        line = substr(line, RSTART + RLENGTH)
      }
    }
    END { exit(found ? 0 : 1) }
  ' "$dump_file"
}
dashboard_nginx_declared_user() {
  awk '
    {
      line = $0
      sub(/#.*/, "", line)
      if (line ~ /^[[:space:]]*user[[:space:]]+/) {
        sub(/^[[:space:]]*user[[:space:]]+/, "", line)
        sub(/[[:space:];].*$/, "", line)
        print line
        exit
      }
    }
  ' "$1"
}
dashboard_render() {
  local input="$1" output="$2" content key value
  shift 2
  content="$(cat "$input")"
  while (($#)); do
    key="$1"; value="$2"; shift 2
    [[ "$key" =~ ^[A-Z_]+$ && "$value" != *$'\n'* && "$value" != *'@'* ]] || dashboard_die 'invalid template substitution'
    content="${content//@${key}@/$value}"
  done
  [[ ! "$content" =~ @[A-Z_]+@ ]] || dashboard_die "unresolved template token: $input"
  printf '%s\n' "$content" > "$output"
}
dashboard_identity_check() {
  local identity="$1" source="$2" candidate resolved
  [[ -f "$identity" && ! -L "$identity" ]] || dashboard_die 'identity descriptor must be a regular file'
  jq -e --arg root "$source" '.schema_version == 1 and .recovery_root == $root and
    ([.identity.environment,.identity.broker_id,.identity.account_id,.identity.instance_id] |
    all(type == "string" and length > 0)) and (.account_alias|type == "string")' "$identity" >/dev/null ||
    dashboard_die 'invalid identity descriptor or recovery_root mismatch'
  while IFS= read -r candidate; do
    dashboard_path "$candidate"
    resolved="$(realpath -m "$candidate")"
    [[ "$resolved" == "$source/"* ]] || dashboard_die 'descriptor input path escapes selected source directory'
    [[ "$(realpath -ms "$candidate")" == "$resolved" ]] || dashboard_die 'descriptor input path contains a symlink'
  done < <(jq -r '.paths // {} | .[]' "$identity")
}
