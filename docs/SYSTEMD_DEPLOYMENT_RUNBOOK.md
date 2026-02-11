# Systemd Deployment Runbook (Stage Bootstrap)

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
