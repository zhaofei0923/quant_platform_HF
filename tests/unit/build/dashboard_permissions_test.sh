#!/usr/bin/env bash
# Real kernel permission check in /tmp only. Uses numeric UIDs; creates no system accounts.
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd -P)"
if [[ "$EUID" != 0 ]]; then echo 'SKIP: isolated permission test requires root'; exit 77; fi
SETFACL="${DASHBOARD_TEST_SETFACL:-setfacl}"
command -v "$SETFACL" >/dev/null || { echo 'SKIP: setfacl unavailable'; exit 77; }
command -v setpriv >/dev/null || { echo 'SKIP: setpriv unavailable'; exit 77; }
[[ "$(getent group nogroup | cut -d: -f3)" == 65534 ]] || { echo 'SKIP: standard fixture group unavailable'; exit 77; }
for numeric_uid in 60321 60322 60323; do
  if getent passwd "$numeric_uid" >/dev/null; then echo 'SKIP: fixture UID is already in use'; exit 77; fi
done
STRATEGY_PREFIX="${QUANT_STRATEGIES_PREFIX:?Set the installed strategy package prefix for this test}"
[[ -f "$STRATEGY_PREFIX/include/quant_hft/contracts/types.h" ]] || {
  echo 'FAIL: installed strategy contracts are unavailable'; exit 1;
}
TMP="$(mktemp -d /tmp/quant-dashboard-permission-test.XXXXXX)"
trap 'rm -rf -- "$TMP"' EXIT
chmod 0755 "$TMP"
mkdir "$TMP/source" "$TMP/public" "$TMP/other-account"
chown 60321:60321 "$TMP/source" "$TMP/other-account"
chmod 0750 "$TMP/source" "$TMP/other-account"
chown 60322:60323 "$TMP/public"
chmod 2750 "$TMP/public"
"$SETFACL" -m g:65534:r-x,d:g:65534:r-x "$TMP/source"
cat > "$TMP/create_file.cpp" <<'CPP'
#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
int main(int argc, char** argv) {
    if (argc != 3) return 2;
    const int fd = open(argv[1], O_CREAT | O_EXCL | O_WRONLY, std::strtol(argv[2], nullptr, 8));
    if (fd < 0) return 3;
    const char value[] = "fixture\n";
    const bool ok = write(fd, value, sizeof(value) - 1) == sizeof(value) - 1;
    close(fd);
    return ok ? 0 : 4;
}
CPP
"${DASHBOARD_TEST_CXX:-c++}" "$TMP/create_file.cpp" -o "$TMP/create-file"
cat > "$TMP/create_wal.cpp" <<'CPP'
#include <string>
#include <iostream>
#include "quant_hft/core/local_wal_regulatory_sink.h"
#include "quant_hft/core/host_adapters/host_clock.h"
int main(int argc, char** argv) {
    quant_hft::BindOnlineHostClocks();
    if (argc != 2 && argc != 3) return 2;
    quant_hft::LocalWalRegulatorySink sink(argv[1]);
    std::string error;
    const bool enabled = argc == 2 || sink.EnableObserverReadAccess(argv[2], &error);
    if (!sink.LastError().empty()) { std::cerr << sink.LastError() << '\n'; return 3; }
    quant_hft::OrderEvent event;
    event.instrument_id = "test";
    const auto receipt = sink.CommitOrderEvent(event);
    if (!receipt.durable) { std::cerr << receipt.error << '\n'; return 4; }
    return enabled ? 0 : 5;
}
CPP
"${DASHBOARD_TEST_CXX:-c++}" -std=c++17 -pthread -I "$ROOT/include" -I "$STRATEGY_PREFIX/include" \
  "$TMP/create_wal.cpp" "$ROOT/src/core/regulatory/local_wal_regulatory_sink.cpp" -o "$TMP/create-wal"
chmod 0755 "$TMP/create-file"
chmod 0755 "$TMP/create-wal"
engine=(setpriv --reuid 60321 --regid 60321 --clear-groups)
publisher=(setpriv --reuid 60322 --regid 65534 --groups 60323)
web=(setpriv --reuid 60323 --regid 60323 --clear-groups)
"${engine[@]}" "$TMP/create-file" "$TMP/source/new-private.json" 0640
"${engine[@]}" "$TMP/create-wal" "$TMP/source/legacy-wal"
mkdir "$TMP/source/wal"
chown 60321:65534 "$TMP/source/wal"
chmod 2770 "$TMP/source/wal"
"$SETFACL" -m g::r-x,g:65534:r-x,d:g::r-x,d:g:65534:r-x "$TMP/source/wal"
"${engine[@]}" "$TMP/create-wal" "$TMP/source/wal/observer-wal" nogroup
if "${engine[@]}" "$TMP/create-wal" "$TMP/source/wrong-group-wal" nogroup; then echo 'FAIL: observer accepted the wrong fd group'; exit 1; fi
"${publisher[@]}" cat "$TMP/source/new-private.json" >/dev/null
"${publisher[@]}" cat "$TMP/source/wal/observer-wal" >/dev/null
if "${publisher[@]}" cat "$TMP/source/legacy-wal" >/dev/null 2>&1; then echo 'FAIL: 0600 input unexpectedly readable'; exit 1; fi
if "${publisher[@]}" cat "$TMP/source/wrong-group-wal" >/dev/null 2>&1; then echo 'FAIL: wrong-group input unexpectedly readable'; exit 1; fi
if "${publisher[@]}" touch "$TMP/source/wal/unauthorized-write" 2>/dev/null; then echo 'FAIL: publisher can write WAL directory'; exit 1; fi
if "${publisher[@]}" touch "$TMP/source/unauthorized-write" 2>/dev/null; then echo 'FAIL: publisher can write source'; exit 1; fi
if "${web[@]}" cat "$TMP/source/new-private.json" >/dev/null 2>&1; then echo 'FAIL: web can read private source'; exit 1; fi
if "${publisher[@]}" ls "$TMP/other-account" >/dev/null 2>&1; then echo 'FAIL: publisher can inspect other account'; exit 1; fi
umask 0027
"${publisher[@]}" "$TMP/create-file" "$TMP/public/current.json" 0640
"${web[@]}" cat "$TMP/public/current.json" >/dev/null
if "${web[@]}" touch "$TMP/public/unauthorized-write" 2>/dev/null; then echo 'FAIL: web can write published data'; exit 1; fi
printf 'PASS: real numeric-UID ACL isolation, private sources, new 0600 WAL + matching fd-group opt-in to 0640, wrong-group refusal, legacy 0600 preserved, WAL directory read-only, public setgid inheritance, web read-only, other-account denial.\n'
