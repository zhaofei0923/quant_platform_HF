# Dashboard deployment validation — 2026-09-07

Status: **LOCAL_PROTOCOL_VERIFIED**. The later production deployment is recorded separately in [cloud_deployment_validation.md](cloud_deployment_validation.md).

## Tested with actual processes

- Authelia v4.39.22 Linux amd64, downloaded from its official release. Release archive SHA256 `15ea0538c3a795f8698fdee734e4f775446151d335e6daeb2f8e92063f865ddd` matches the official release asset metadata.
- Ubuntu Nginx 1.18.0-6ubuntu14.20, obtained with `apt-get download` and unpacked under `/tmp`; no system package was installed. A random transient systemd fixture service was started only for the isolated session lifetime test and stopped/collected on exit.
- Both test processes bound loopback only. Certificate, password, password hash, authentication SQLite and cookies were created in a private temporary directory and removed on test exit. No test credentials are committed or included in the release.
- Actual configuration validation, trusted test HTTPS, independent login/logout portal routes, Secure/HttpOnly and eight-hour cookies, anonymous JSON 401, Basic credential bypass rejection, authenticated JSON, no-store, non-allowlisted JSON denial, public symlink denial, raw/encoded traversal denial, POST denial on data, private auth endpoint denial, logout invalidation of the old cookie, login rate limit and authentication-outage fail-closed behavior passed. A temporary two-second inactivity setting rejected an explicitly replayed cookie after inactivity; the test does not rely on curl discarding expired cookies.
- Reproduction script: `tests/unit/build/dashboard_deployment_test.sh`. The concise successful run is saved in `deployment_http_test.log`. The optional test returns 77 when its external executable inputs are absent; that outcome is a skip, not a pass.
- All new deployment shell scripts passed Bash syntax checks. `git diff --check` reported no whitespace errors at validation time.
- `dashboard_permissions_test.sh` passed using actual Linux ACLs and distinct numeric process identities in a temporary directory, including compiling and exercising the real C++ WAL implementation. A new default 0600 WAL remained private; matching-group observer opt-in changed an opened WAL to 0640; wrong-group opt-in was refused without stopping durable commit. Publisher writes and web private-source reads were denied. See `deployment_permissions_test.log`.
- `dashboard_session_lifetime_test.sh` passed using actual systemd supervision of Authelia as an unprivileged identity, with a temporary eight-second service lifetime. The same raw cookie was repeatedly accepted before process replacement, temporarily refused while authentication restarted, and then rejected with 401 after a different PID started. See `deployment_session_lifetime_test.log`.

The test found and corrected a real compatibility issue: Authelia v4.39.22 requires numeric `remember_me: -1`; quoted `'-1'` fails duration parsing. Temporary Nginx paths were explicitly configured so the service runs without system-directory write access.

The short session test also found that Authelia v4.39.22's server-side expiration slides with requests and its memory provider removes expired state during periodic GC. Simply setting `expiration: 8h` does not enforce an absolute server-side deadline. The production auth unit therefore uses `RuntimeMaxSec=7h59min55s`, `TimeoutStopSec=5s`, and `Restart=always`: memory sessions are cleared within eight hours of each service start, even under continuous requests. This may require login earlier than eight hours and creates a brief fail-closed authentication interruption. No custom authentication code or Redis was introduced.

## Release package

- Packaged the actual `/tmp/quant-hft-root-build/dashboard_publish_cli` and `web/dashboard/dist` using `scripts/ops/package_dashboard.sh`, together with verified Authelia and deployment templates/scripts.
- Final package location and SHA256 are recorded in `release_manifest.txt`; the earlier `/tmp/quant-dashboard-release/quant-dashboard-20260907.tar.gz` was superseded and is not the final release.
- Per-file integrity manifest is inside the package as `SHA256SUMS`, including the deployment runbook and data contract. No `.env`, raw runtime data, identity descriptor, users YAML, password hash, TLS certificate/private key or source map is packaged. The trading engine is not in this package; it must be updated separately through the existing safe maintenance process before enabling its observer settings.
- Publisher links only standard C/C++ runtime libraries in this build; Authelia standard system libraries resolve locally. Neither binary has unresolved libraries in the checked build environment.
- The package is a local build artifact. Rebuild it if code, frontend, or deployment templates change; update the package checksum accordingly.

## Not claimed or performed by this local validation run

- No Tencent Cloud connection, install, port/security-group change, domain purchase, DNS update, ICP action, real certificate issuance or public deployment.
- No read of the user's `.env`, CTP credentials, SSH keys or existing TLS keys; no changes to existing trading units or running trading processes.
- Full root-level installation/upgrade/rollback, ACL behavior on the actual selected runtime, production systemd sandbox/resource enforcement, renewal timer/hooks, actual mobile-browser rendering and current trading performance still require target-host acceptance. Local fixture ACL and transient service lifecycle checks above do not claim those target-host checks.
- The default core snapshot remains disabled until the existing controlled maintenance workflow explicitly sets `QUANT_HFT_DASHBOARD_SNAPSHOT_ENABLE=1`, the selected `QUANT_HFT_DASHBOARD_SNAPSHOT_FILE`, and `QUANT_HFT_DASHBOARD_WAL_READ_GROUP=quant-dashboard-read`. Deployment installation does not enable it or restart the engine.
- A dedicated user-controlled domain, applicable mainland Tencent Cloud access/ICP readiness and target-server facts remain deployment prerequisites.

## Source references

- [Authelia release](https://github.com/authelia/authelia/releases/tag/v4.39.22)
- [Nginx authentication integration](https://www.authelia.com/integration/proxies/nginx/)
- [Session and no-Redis behavior](https://www.authelia.com/configuration/session/introduction/)
- [Server subpath configuration](https://www.authelia.com/configuration/miscellaneous/server/)
- [Authorization inactivity and save logic](https://github.com/authelia/authelia/blob/v4.39.22/internal/handlers/handler_authz_authn.go)
- [Memory session save and GC logic](https://github.com/authelia/authelia/blob/v4.39.22/internal/session/memory/provider.go)
- [fasthttp session default GC interval](https://github.com/fasthttp/session/blob/v2.5.9/const.go)
- [systemd RuntimeMaxSec documentation](https://github.com/systemd/systemd/blob/v249/man/systemd.service.xml)

The `/auth/logout` browser route was also checked against the v4.39.22 official frontend source: the router uses its configured base path, selects `SignOut`, then POSTs the logout API. This source inspection and the actual HTTP logout test do not claim a full browser-JavaScript logout test.
