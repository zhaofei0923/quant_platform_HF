#!/usr/bin/env bash
# Copy offline state into a new review directory. This never promotes or rewrites live state.
set -euo pipefail
umask 077

source_root=""
staging_parent=""
environment=""
broker=""
account=""
instance="default"
usage() {
  cat <<'EOF'
Usage: stage_runtime_migration.sh --source DIR --staging-parent DIR
       --environment sim|simnow|prod --broker ID --account ID [--instance ID]
Copies stopped runtime state, compares source/copy SHA-256 inventories and identity.
Never creates an active identity manifest, changes the source, or converts legacy WAL.
Exit 0: stable copy and existing identity match. Exit 3: copied legacy identity is unverified.
Both outcomes require WAL/domain review before promotion. Staging must be outside source.
EOF
}
while [[ $# -gt 0 ]]; do
  case "$1" in
    --source) source_root="${2:?missing source}"; shift 2 ;;
    --staging-parent) staging_parent="${2:?missing staging parent}"; shift 2 ;;
    --environment) environment="${2:?missing environment}"; shift 2 ;;
    --broker) broker="${2:?missing broker}"; shift 2 ;;
    --account) account="${2:?missing account}"; shift 2 ;;
    --instance) instance="${2:?missing instance}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done
fail() { echo "error: $*" >&2; exit 2; }
[[ -d "${source_root}" && -n "${staging_parent}" ]] || fail "source and staging parent required"
[[ "${environment}" =~ ^(sim|simnow|prod)$ ]] || fail "invalid environment"
for component in "${broker}" "${account}" "${instance}"; do
  [[ "${component}" =~ ^[A-Za-z0-9_.-]+$ && "${component}" != . && "${component}" != .. ]] || fail "invalid identity component"
done
source_root="$(realpath -e -- "${source_root}")"
[[ "${source_root}" != / ]] || fail "source must be a runtime directory"
staging_parent="$(realpath -m -- "${staging_parent}")"
case "${staging_parent}/" in "${source_root}/"*) fail "staging must be outside source" ;; esac
[[ -z "$(find "${source_root}" -mindepth 1 ! -type f ! -type d -print -quit)" ]] || fail "source contains links or special files"

# Only open existing lock files; never add a new file to the source directory.
declare -a held_fds=()
while IFS= read -r -d '' lock_file; do
  exec {lock_fd}<"${lock_file}"
  flock -n -x "${lock_fd}" || fail "source runtime is active"
  held_fds+=("${lock_fd}")
done < <(find "${source_root}" -type f \( -name .lock -o -name '*.lock' \) -print0)
if [[ -f "${source_root}/../.account.lock" ]]; then
  exec {account_fd}<"${source_root}/../.account.lock"
  flock -n -x "${account_fd}" || fail "source account is active"
fi

mkdir -p -- "${staging_parent}"
review_dir="$(mktemp -d "${staging_parent}/runtime-review.XXXXXXXX")"
mkdir -- "${review_dir}/copy"
printf 'schema=1\nenvironment=%s\nbroker=%s\naccount=%s\ninstance=%s\n' \
  "${environment}" "${broker}" "${account}" "${instance}" > "${review_dir}/requested_identity.txt"
inventory() {
  (cd -- "$1" && find . -type f -print0 | LC_ALL=C sort -z | xargs -0 -r sha256sum --)
}
inventory "${source_root}" > "${review_dir}/source_before.sha256"
cp -a --reflink=never -- "${source_root}/." "${review_dir}/copy/"
inventory "${source_root}" > "${review_dir}/source_after.sha256"
inventory "${review_dir}/copy" > "${review_dir}/copy.sha256"
cmp -s "${review_dir}/source_before.sha256" "${review_dir}/source_after.sha256" || fail "source changed; quarantine retained at ${review_dir}"
cmp -s "${review_dir}/source_after.sha256" "${review_dir}/copy.sha256" || fail "copy mismatch; quarantine retained at ${review_dir}"
identity_status="unverified_legacy"
if [[ -f "${review_dir}/copy/identity.manifest" ]]; then
  cmp -s "${review_dir}/requested_identity.txt" "${review_dir}/copy/identity.manifest" || fail "identity mismatch; quarantine retained at ${review_dir}"
  identity_status="verified_existing_manifest"
fi
cat > "${review_dir}/review.txt" <<EOF
copy_sha256=matched
source_unchanged=true
identity_status=${identity_status}
promotion_allowed=false
reason=offline WAL schema, trade identity, baseline coverage and domain reconciliation still required
EOF
sync -f "${review_dir}"
printf 'review_directory=%s\nidentity_status=%s\npromotion_allowed=false\n' "${review_dir}" "${identity_status}"
[[ "${identity_status}" == verified_existing_manifest ]] || exit 3
