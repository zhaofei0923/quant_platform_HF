# PR-7.1 SimNow 对比运行（首版）

## 范围
- 新增 `scripts/simnow/run_simnow_compare.py`，产出 SimNow 与回测基线对比 JSON 报告。
- 新增 `python/quant_hft/simnow` 运行模块，支持 `dry-run` 和配置加载。
- 新增日度 workflow：`.github/workflows/simnow_compare_daily.yml`。

## 执行方式
```bash
python scripts/simnow/run_simnow_compare.py \
  --config configs/sim/ctp.yaml \
  --csv-path backtest_data/rb.csv \
  --max-ticks 300 \
  --dry-run \
  --output-json docs/results/simnow_compare_report.json
```

## Secrets 与配置
- 建议在本地复制 `.env.example` 并填入 SimNow 账号配置。
- CI 中通过 `CTP_SIM_*` secrets 注入。
- 当缺失关键凭据时，脚本自动退化为 `dry-run`。

## 报告字段
- `simnow.intents_emitted`：SimNow 侧产生日志意图数。
- `backtest.intents_emitted`：同一 CSV 基线回测意图数。
- `delta.intents`：两侧差值（首版阈值为绝对 0）。
