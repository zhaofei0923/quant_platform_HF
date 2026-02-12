# 一键部署与演练（PR-5.4）

通过统一入口脚本在单次命令中完成：

- 按环境自动选择 `rollout` 或 `failover` 编排；
- 生成证据文件（`rollout_result.env` 或 `failover_result.env`）；
- 自动调用对应 `verify_*_evidence.py` 校验；
- 生成统一汇总：`one_click_deploy_result.env`。

## 命令

```bash
python3 scripts/ops/one_click_deploy.py \
  --env-config configs/deploy/environments/sim.yaml \
  --output-dir docs/results \
  --inject-fault
```

生产样式多机故障切换演练：

```bash
python3 scripts/ops/one_click_deploy.py \
  --env-config configs/deploy/environments/prodlike_multi_host.yaml \
  --output-dir docs/results
```

真实执行命令（非 dry-run）时，追加 `--execute`。

## 输出

- `docs/results/one_click_deploy_result.env`
  - `DEPLOY_WORKFLOW=rollout|failover`
  - `DEPLOY_SUCCESS=true|false`
  - `DEPLOY_EVIDENCE_FILE=<path>`
- `docs/results/rollout_result.env` 或 `docs/results/failover_result.env`

## 自动选择规则

- `--mode auto`（默认）：
  - 环境 YAML 含 `backup_sync_check_cmd/demote_primary_cmd/promote_standby_cmd` 任一键时，走 `failover`；
  - 否则走 `rollout`。
- 可显式指定 `--mode rollout` 或 `--mode failover`。
