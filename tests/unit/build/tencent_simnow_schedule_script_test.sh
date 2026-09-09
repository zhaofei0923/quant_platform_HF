#!/usr/bin/env bash
# Exercise the real compatibility and account launch scripts with an isolated CLI
# boundary double. No credentials, running services or broker connections are used.
set -euo pipefail
umask 077

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd -P)"
WRAPPER="${REPO_ROOT}/scripts/ops/run_tencent_simnow_schedule.sh"
LAUNCHER="${REPO_ROOT}/scripts/ops/run_account_deployment.sh"
TEST_ROOT="$(mktemp -d /tmp/quant_hft_deployment_launcher_test.XXXXXX)"
trap 'rm -rf -- "${TEST_ROOT}"' EXIT
BIN_DIR="${TEST_ROOT}/binary directory"
MANIFEST="${TEST_ROOT}/deployment manifest.yaml"
CAPTURE="${TEST_ROOT}/arguments.bin"
mkdir -p "${BIN_DIR}"
printf 'fixture-valid\n' > "${MANIFEST}"

fail() { echo "[fail] $*" >&2; exit 1; }
assert_contains() {
  grep -Fq -- "$2" <<< "$1" || fail "expected output: $2"
}
expect_arguments() {
  local -a actual=()
  mapfile -d '' -t actual < "${CAPTURE}"
  [[ "${#actual[@]}" -eq "$#" ]] || fail "unexpected argument count: ${#actual[@]} vs $#"
  local index=0 expected
  for expected in "$@"; do
    [[ "${actual[${index}]}" == "${expected}" ]] || fail "argument ${index} was split or changed"
    index=$((index + 1))
  done
}

cat > "${BIN_DIR}/quant_config_cli" <<'CLI'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\0' "$@" >> "${TEST_CAPTURE:?}"
case "${1:-}" in
  validate)
    [[ $# -eq 2 && -f "$2" ]] || { echo 'fixture manifest missing' >&2; exit 21; }
    [[ "$(head -n 1 "$2")" == fixture-valid ]] || { echo 'fixture validation rejected' >&2; exit 22; }
    printf 'validated\n'
    ;;
  list)
    [[ $# -eq 2 ]] || exit 23
    printf 'simnow_a instance_a\nsimnow_b instance_b\n'
    ;;
  launch)
    [[ $# -eq 3 && -f "$2" ]] || exit 24
    printf 'delegated account=%s\n' "$3"
    ;;
  *) exit 25 ;;
esac
CLI
chmod 0700 "${BIN_DIR}/quant_config_cli"

invoke() {
  local entry="$1" account="$2"
  shift 2
  env QUANT_HFT_BIN_DIR="${BIN_DIR}" QUANT_HFT_DEPLOYMENT_FILE="${MANIFEST}" \
    QUANT_HFT_DEPLOYMENT_ACCOUNT="${account}" TEST_CAPTURE="${CAPTURE}" bash "${entry}" "$@"
}

# Check-only validates before showing the instance list and cannot launch.
: > "${CAPTURE}"
output="$(invoke "${WRAPPER}" simnow_b --check-only)" || fail 'compatibility check-only failed'
expect_arguments validate "${MANIFEST}" list "${MANIFEST}"
assert_contains "${output}" 'validated'
assert_contains "${output}" 'simnow_b instance_b'

# Both entry names delegate exactly the selected account, with a spaced path
# preserved as one argument. Account B never silently falls back to account A.
for entry in "${WRAPPER}" "${LAUNCHER}"; do
  for account in simnow_a simnow_b; do
    : > "${CAPTURE}"
    output="$(invoke "${entry}" "${account}")" || fail 'account launch delegation failed'
    expect_arguments launch "${MANIFEST}" "${account}"
    assert_contains "${output}" "delegated account=${account}"
  done
done

# A configured path which does not exist is rejected by validation, with no
# subsequent list or launch invocation.
mv "${MANIFEST}" "${MANIFEST}.saved"
: > "${CAPTURE}"
if output="$(invoke "${WRAPPER}" simnow_b --check-only 2>&1)"; then
  fail 'missing manifest file unexpectedly passed'
else
  [[ $? -eq 21 ]] || fail 'missing manifest exit status was not preserved'
fi
expect_arguments validate "${MANIFEST}"
assert_contains "${output}" 'fixture manifest missing'
mv "${MANIFEST}.saved" "${MANIFEST}"

# A failed validation stops before list/launch, preserving the backend status.
printf 'fixture-invalid\n' > "${MANIFEST}"
: > "${CAPTURE}"
if output="$(invoke "${WRAPPER}" simnow_b --check-only 2>&1)"; then
  fail 'invalid manifest unexpectedly passed'
else
  [[ $? -eq 22 ]] || fail 'validation exit status was not preserved'
fi
expect_arguments validate "${MANIFEST}"
assert_contains "${output}" 'fixture validation rejected'
printf 'fixture-valid\n' > "${MANIFEST}"

# Required identity references fail before any backend invocation.
for missing in QUANT_HFT_DEPLOYMENT_FILE QUANT_HFT_DEPLOYMENT_ACCOUNT; do
  : > "${CAPTURE}"
  if output="$(env QUANT_HFT_BIN_DIR="${BIN_DIR}" QUANT_HFT_DEPLOYMENT_FILE="${MANIFEST}" \
      QUANT_HFT_DEPLOYMENT_ACCOUNT=simnow_b TEST_CAPTURE="${CAPTURE}" \
      env -u "${missing}" bash "${WRAPPER}" --check-only 2>&1)"; then
    fail "missing ${missing} unexpectedly passed"
  fi
  [[ ! -s "${CAPTURE}" ]] || fail "backend ran without ${missing}"
  assert_contains "${output}" "${missing}"
done

: > "${CAPTURE}"
if output="$(invoke "${WRAPPER}" simnow_b --unsupported 2>&1)"; then
  fail 'unsupported argument unexpectedly passed'
fi
[[ ! -s "${CAPTURE}" ]] || fail 'backend ran with unsupported option'
assert_contains "${output}" 'usage: run_account_deployment.sh'

echo '[ok] account deployment launcher and compatibility delegation tests passed'
