# Tencent Cloud Dashboard deployment validation — 2026-09-07

Status: **DEPLOYED / CREDENTIAL ACTIVE / BROWSER LOGIN-LOGOUT PENDING / LIVE ACCOUNT SNAPSHOT PENDING NEXT CORE START**.

## Target and isolation

- The production site is `https://quant.easudata.com/` on the existing Ubuntu host.
- The existing system Nginx remains the only public web listener. Its master PID and activation timestamp were unchanged across deployment, and every pre-existing Nginx configuration checksum remained unchanged. Only the managed `quant-dashboard` site and enable link were added.
- Existing `bid`, `gxjn`, and `navigator` responses remained `307`, `200`, and `200` respectively.
- Authelia listens only on `127.0.0.1:9091`. No dedicated `quant-dashboard-web.service` was installed.
- The authentication and publisher services are enabled and active. Both have 25% CPU quota and 256 MiB memory maximum; the publisher additionally has a private network namespace and read-only source bind.

## HTTP, TLS, authentication, and renewal

- Public HTTPS trust and hostname verification passed with the existing `*.easudata.com` Let's Encrypt certificate, valid through 2026-12-05.
- HTTP redirects to HTTPS. `/auth/` returns 200; an anonymous `/` request redirects to the login portal; anonymous `/data/v1/current.json` returns 401.
- Stopping Authelia caused the private JSON request to return 500, then returned to 401 after authentication was restored. Authentication failure therefore remained fail-closed.
- The actual wildcard Certbot lineage was recorded. The root-owned deploy hook validated the hostname, certificate lifetime and key match, copied the renewed files, passed `nginx -t`, and smoothly reloaded the system Nginx.
- The user set the `owner` password through the root-owned interactive helper. The account is enabled, its file contains an Argon2id digest with mode 0640, and Authelia restarted healthy. No password or reusable secret passed through Codex or deployment evidence.

## Trading-data boundary

- The C++ core observer changes were built on the target in the existing real-CTP build directory during the off-session maintenance window. The trading scheduler was restored before deployment and remained active with the same PID during publisher/authentication fault tests.
- The selected source grants read-only access only to `quant-dashboard-publisher`. The publisher cannot write its source; the Nginx worker cannot read the private WAL; the Nginx worker can read only the filtered public directory.
- Public JSON contains no real account identifier, server path, `.env` marker, password marker, certificate, key, PID, or raw log.
- At deployment time the core process was outside its configured session and the new private account snapshot did not yet exist. The site therefore publishes `missing` for account, positions, health, orders, and trades. It does not report zero balance or flat position. The first real account/position snapshot and live latency/resource comparison remain pending the next controlled core start.
- The deployment does not assert that the trading instance is healthy. Earlier runtime evidence contains degraded/readiness warnings, and the strategy state was not flat; the dashboard must surface fresh runtime facts after the next start rather than infer health from the scheduler.

## Verification and release

- Final web release: `quant-dashboard-20260908-final-v4.tar.gz`, SHA256 `78095ad2e42f5a89feb06f7ac12a4c6074dc6902a52794c41b7a4cf71aa0369f`.
- Installed release: `/opt/quant-dashboard/releases/20260907T232748-e34643cdf19e` (UTC release timestamp).
- Per the user's preference, the root-owned interactive helper now enforces at least 8 characters and matching confirmation. It passes the secret to the local hasher through a private pseudo-terminal, so the password is not placed in process arguments, files, logs, or Codex.
- The package manifest passed before installation. Extraction with a restrictive umask produced mode 0700 executables; the final installer now normalizes packaged binaries to 0755, and the actual installed modes are 0755. Both services then started with zero restarts.
- The publisher's first-query-failure regression was fixed before this final release. Publisher scenarios are 21/21, snapshot tests 12/12, WAL validation 6/6, and WAL permission tests 3/3; optimized target builds emitted no warnings.

## Remaining acceptance

1. The user must log in and verify logout from their browser; Codex does not receive or enter the password.
2. After the next core start, reconcile displayed funds and broker positions against the CTP account/position callbacks and confirm source timestamps/quality.
3. Compare production core callback latency and Dashboard resource use after live snapshots begin. Local microbenchmarks do not close this live acceptance item.
