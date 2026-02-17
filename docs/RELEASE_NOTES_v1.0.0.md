# quant_platform_HF v1.0.0 Release Notes

发布日期：2026-02-17

## 版本摘要

v1.0.0 为纯 C++ 生产就绪版本。仓库和运行链路已移除 Python 运行依赖，核心交易、回测、运维证据链、门禁与 CI 已全部转为 C++ 可执行与 Shell 编排。

## 主要变更

1. 交易主链路纯 C++
- 新增并集成 `ILiveStrategy` / `StrategyRegistry` / `StrategyEngine`。
- `core_engine` 改为进程内策略分发，策略信号直接进入执行链路。
- 订单事件新增 `strategy_id`，支持定向路由与空值广播兼容。

2. 回测与一致性门禁
- 新增 `backtest_consistency_cli` 和一致性回归脚本。
- 固化 legacy baseline 与 provenance（可溯源）。
- 一致性门禁按固定容差校验关键指标字段。

3. 性能门禁与确定性样本
- `backtest_benchmark_cli` 增加硬失败语义与 `failure_reason`。
- 新增确定性大样本生成脚本，支持稳定复现实验结果。
- 采用 `new_p95_ms <= old_p95_ms * 1.10` 门禁规则。

4. 运维与文档纯度
- 健康/告警/重连证据/切换编排 CLI 全部 C++ 化。
- SLI 统一为 `strategy_engine_*`。
- 新增仓库纯度与文档纯度检查，历史混合栈文档归档到 `docs/archive_legacy/`。

5. 依赖与构建门禁
- 编译器门禁：GCC >= 11 或 Clang >= 14。
- PrometheusCpp 最低版本门禁：`find_package(PrometheusCpp 1.1 MODULE REQUIRED)`（按开关启用）。

## 验证结果

- 全量测试：`ctest --test-dir build --output-on-failure` 通过（262/262）。
- 一致性门禁：通过。
- 性能门禁：大样本下通过（`failure_reason=none`）。
- 纯度门禁：`repo_purity_check` 与 `doc_purity_check` 通过。

## 发布资产

- 主分支提交：`e5261f8`
- Git 标签：`v1.0.0`

## 备注

本机未安装 GitHub CLI（`gh`），因此未自动创建 GitHub Release 页面。安装后可执行：

```bash
gh release create v1.0.0 \
  --repo zhaofei0923/quant_platform_HF \
  --title "v1.0.0" \
  --notes-file docs/RELEASE_NOTES_v1.0.0.md
```
