# PR-7.3 周压测采集（先采集不阻断）

## 范围
- 新增 `scripts/perf/run_simnow_weekly_stress.py`，按多次采样运行 SimNow compare 并输出聚合统计。
- 新增 `.github/workflows/simnow_weekly_stress.yml`，每周执行采集并上传产物。
- 默认 `--collect-only`，不作为 CI 阻断条件。

## 输出
- `delta_abs_mean`、`delta_abs_p95`
- `delta_ratio_mean`、`delta_ratio_p95`
- `all_within_threshold`
- `samples_detail`

## 示例
```bash
python scripts/perf/run_simnow_weekly_stress.py \
  --config configs/sim/ctp.yaml \
  --csv-path backtest_data/rb.csv \
  --max-ticks 1200 \
  --samples 5 \
  --dry-run \
  --collect-only \
  --result-json docs/results/simnow_weekly_stress.json
```
