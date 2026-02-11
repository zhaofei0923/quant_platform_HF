# Systemd Deployment Runbook (Single-Host Production Baseline)

Current production acceptance profile:
- single-host deployment on one PC host or one cloud VM
- release -> fault injection -> rollback -> recovery evidence chain
- multi-host backup and failover are handled in a later rollout phase

## 0) Verify release bundle integrity (if deploying from package)

```bash
python3 scripts/build/verify_nonhotpath_release.py \
  --bundle dist/quant-hft-nonhotpath-v0.2.0.tar.gz \
  --checksum dist/quant-hft-nonhotpath-v0.2.0.tar.gz.sha256 \
  --expect-version v0.2.0

python3 scripts/build/release_audit_summary.py \
  --bundle dist/quant-hft-nonhotpath-v0.2.0.tar.gz \
  --checksum dist/quant-hft-nonhotpath-v0.2.0.tar.gz.sha256 \
  --output docs/results/release_audit_summary.md \
  --json-output docs/results/release_audit_summary.json

python3 scripts/build/verify_release_audit_summary.py \
  --summary-json docs/results/release_audit_summary.json \
  --expect-version v0.2.0
```

## 1) Render unit bundle

```bash
.venv/bin/python scripts/ops/render_systemd_units.py \
  --repo-root . \
  --output-dir deploy/systemd \
  --service-user "$USER"
```

Generated files:
- `deploy/systemd/quant-hft-core-engine.service`
- `deploy/systemd/quant-hft-data-pipeline.service`
- `deploy/systemd/quant-hft-core-engine.env.example`
- `deploy/systemd/quant-hft-data-pipeline.env.example`

If using release bundle packaging:
- verify `deploy/release_manifest.json` exists in unpacked bundle
- verify `release_version` matches deployment ticket

## 2) Prepare env files

```bash
cp deploy/systemd/quant-hft-core-engine.env.example deploy/systemd/quant-hft-core-engine.env
cp deploy/systemd/quant-hft-data-pipeline.env.example deploy/systemd/quant-hft-data-pipeline.env
```

Set at least:
- `CTP_SIM_PASSWORD` in `deploy/systemd/quant-hft-core-engine.env`

## 3) Install and enable (system-level example)

```bash
sudo cp deploy/systemd/quant-hft-core-engine.service /etc/systemd/system/
sudo cp deploy/systemd/quant-hft-data-pipeline.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now quant-hft-core-engine.service
sudo systemctl enable --now quant-hft-data-pipeline.service
```

## 4) Health check

```bash
systemctl status quant-hft-core-engine.service --no-pager
systemctl status quant-hft-data-pipeline.service --no-pager
journalctl -u quant-hft-core-engine.service -n 200 --no-pager
journalctl -u quant-hft-data-pipeline.service -n 200 --no-pager
```

## Notes

- `quant-hft-data-pipeline.service` runs loop mode (`--iterations 0`).
- `quant-hft-core-engine.service` currently executes bootstrap path and restart policy is enabled.
- For production rollout, pair with fault-injection and reconnect SLO runbook validation.

## Rollback Drill (Mandatory Before Production)

### Drill Inputs

- `PREV_RELEASE`: previous known-good package path
- `NEW_RELEASE`: candidate package path
- `HOST`: target host

### Drill Procedure

1. Deploy `NEW_RELEASE` and start services:

```bash
sudo systemctl restart quant-hft-core-engine.service
sudo systemctl restart quant-hft-data-pipeline.service
```

2. Inject failure (example):

```bash
sudo systemctl stop quant-hft-core-engine.service
```

3. Roll back to `PREV_RELEASE` and restore units/env:

```bash
sudo systemctl stop quant-hft-core-engine.service quant-hft-data-pipeline.service
# restore previous package files and env
sudo systemctl daemon-reload
sudo systemctl start quant-hft-core-engine.service quant-hft-data-pipeline.service
```

### Validation Commands

```bash
systemctl is-active quant-hft-core-engine.service
systemctl is-active quant-hft-data-pipeline.service
journalctl -u quant-hft-core-engine.service -n 100 --no-pager
journalctl -u quant-hft-data-pipeline.service -n 100 --no-pager
```

Acceptance:
- both services active after rollback
- no repeated crash loop in latest 5 minutes logs

Evidence template:
- `docs/templates/DEPLOY_ROLLBACK_RESULT.md`

Machine verification:

```bash
python3 scripts/ops/verify_deploy_rollback_evidence.py \
  --evidence-file docs/results/deploy_rollback_result.env \
  --max-rollback-seconds 180
```

## Repository Rollout Orchestrator (Dry-Run First)

Use the repository orchestrator to produce machine-verifiable rollout evidence:

```bash
python3 scripts/ops/rollout_orchestrator.py \
  --env-config configs/deploy/environments/sim.yaml \
  --inject-fault \
  --output-file docs/results/rollout_result.env

python3 scripts/ops/verify_rollout_evidence.py \
  --evidence-file docs/results/rollout_result.env \
  --require-fault-step
```

For real command execution, append `--execute` explicitly.

## Repository Prodlike Infra Bootstrap

Use the repository-local prodlike stack bootstrap before host-level rollout drills:

```bash
bash scripts/infra/bootstrap_prodlike.sh \
  --compose-file infra/docker-compose.prodlike.yaml \
  --project-name quant-hft-prodlike \
  --dry-run \
  --output-file docs/results/prodlike_bootstrap_result.env

python3 scripts/infra/check_prodlike_health.py \
  --ps-json-file docs/results/prodlike_ps_snapshot.json \
  --report-json docs/results/prodlike_health_report.json
```

For real execution, replace `--dry-run` with `--execute`.
