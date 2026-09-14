#!/usr/bin/env bash
set -euo pipefail
umask 077

package_die() {
    printf 'packaged release preflight failed: %s\n' "$*" >&2
    exit 1
}

package_root="${1:-}"
[[ -n "$package_root" ]] || package_die "usage: verify_packaged_release.sh <package-root>"
package_root="$(cd "$package_root" 2>/dev/null && pwd -P)" ||
    package_die "package root is not an accessible directory: $package_root"

for required_tool in sha256sum find sort xargs cmp sed mktemp; do
    command -v "$required_tool" >/dev/null 2>&1 ||
        package_die "required verification tool is unavailable: $required_tool"
done

deploy_manifest="$package_root/deploy_manifest.json"
checksum_manifest="$package_root/SHA256SUMS"
[[ -s "$deploy_manifest" ]] || package_die "missing deploy_manifest.json in $package_root"
[[ -s "$checksum_manifest" ]] || package_die "missing SHA256SUMS in $package_root"
unexpected_entry="$(cd "$package_root" && find . ! -type d ! -type f -print -quit)"
[[ -z "$unexpected_entry" ]] ||
    package_die "unsupported non-regular package entry: $unexpected_entry"
(cd "$package_root" && sha256sum --check --strict --status SHA256SUMS) ||
    package_die "SHA256SUMS verification failed in $package_root"

# sha256sum --check validates listed files but accepts an incomplete list. Rebuild the canonical
# manifest to also reject added or omitted release files.
rebuilt_checksums="$(mktemp "${TMPDIR:-/tmp}/quant-package-checksums.XXXXXX")" ||
    package_die "unable to create checksum verification file"
if ! (cd "$package_root" &&
    find . -type f ! -name SHA256SUMS -print0 | LC_ALL=C sort -z |
        xargs -0 sha256sum > "$rebuilt_checksums"); then
    rm -f -- "$rebuilt_checksums"
    package_die "unable to rebuild the complete package checksum manifest"
fi
if ! cmp -s "$checksum_manifest" "$rebuilt_checksums"; then
    rm -f -- "$rebuilt_checksums"
    package_die "SHA256SUMS does not exactly cover every packaged file"
fi
rm -f -- "$rebuilt_checksums"

manifest_boolean() {
    local key="$1"
    sed -nE \
        "s/^[[:space:]]*\"${key}\"[[:space:]]*:[[:space:]]*(true|false)[[:space:]]*,?[[:space:]]*$/\\1/p" \
        "$deploy_manifest"
}

[[ "$(manifest_boolean working_tree_dirty)" == "false" ]] ||
    package_die "deploy_manifest.json requires working_tree_dirty=false"
[[ "$(manifest_boolean ctp_real_api_compiled)" == "true" ]] ||
    package_die "deploy_manifest.json requires ctp_real_api_compiled=true"
