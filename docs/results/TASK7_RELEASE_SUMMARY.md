# Task-7 发布摘要（PR-7.1 ~ PR-7.3）

- 发布分支：main
- 发布提交：47ff196e3ca6db41108bbdc0007a3db43111f643
- 提交标题：feat(task7): complete PR-7.1~7.3 simnow integration
- 远端同步状态：local HEAD == origin/main
- 发布时间：2026-02-13

## 变更范围

### PR-7.1 SimNow 对比执行链
- 新增 SimNow 对比运行模块：
  - python/quant_hft/simnow/config.py
  - python/quant_hft/simnow/runner.py
  - python/quant_hft/simnow/adapters.py
  - python/quant_hft/simnow/reporter.py
  - python/quant_hft/simnow/__init__.py
- 新增执行脚本：
  - scripts/simnow/run_simnow_compare.py
- 新增日度工作流：
  - .github/workflows/simnow_compare_daily.yml
- 新增凭据模板：
  - .env.example

### PR-7.2 归因与报告落盘
- 对比结果新增：
  - attribution（signal_parity / execution_coverage / threshold_stability）
  - risk_decomposition（model_drift / execution_gap / consistency_gap）
- 报告输出新增：
  - JSON：docs/results/simnow_compare_report.json
  - HTML：docs/results/simnow_compare_report.html
  - SQLite：runtime/simnow/simnow_compare.sqlite

### PR-7.3 周压测采集（先采集不阻断）
- 新增采集脚本：
  - scripts/perf/run_simnow_weekly_stress.py
- 新增每周工作流：
  - .github/workflows/simnow_weekly_stress.yml
- 非阻断策略：
  - 脚本支持 --collect-only
  - workflow 采集步骤设置 continue-on-error: true

## 测试证据
- /home/kevin0923/workspace/quant_platform_HF/.venv/bin/python -m pytest python/tests/test_simnow_compare_runner.py python/tests/test_run_simnow_compare_script.py python/tests/test_simnow_compare_reporter.py python/tests/test_run_simnow_weekly_stress_script.py -q
  - 结果：4 passed

## 文档更新
- docs/IMPLEMENTATION_PROGRESS.md（新增 Sprint R7）
- docs/research/PR7_1_SIMNOW_COMPARE.md
- docs/research/PR7_2_ATTRIBUTION_REPORTING.md
- docs/research/PR7_3_WEEKLY_STRESS_COLLECTION.md

## 提交文件清单（19）
- .env.example
- .github/workflows/simnow_compare_daily.yml
- .github/workflows/simnow_weekly_stress.yml
- docs/IMPLEMENTATION_PROGRESS.md
- docs/research/PR7_1_SIMNOW_COMPARE.md
- docs/research/PR7_2_ATTRIBUTION_REPORTING.md
- docs/research/PR7_3_WEEKLY_STRESS_COLLECTION.md
- python/quant_hft/__init__.py
- python/quant_hft/simnow/__init__.py
- python/quant_hft/simnow/adapters.py
- python/quant_hft/simnow/config.py
- python/quant_hft/simnow/reporter.py
- python/quant_hft/simnow/runner.py
- python/tests/test_run_simnow_compare_script.py
- python/tests/test_run_simnow_weekly_stress_script.py
- python/tests/test_simnow_compare_reporter.py
- python/tests/test_simnow_compare_runner.py
- scripts/perf/run_simnow_weekly_stress.py
- scripts/simnow/run_simnow_compare.py
