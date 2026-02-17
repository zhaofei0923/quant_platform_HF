# PR-5.1 ~ PR-5.4 合并计划与模板

生成时间：2026-02-12

## 1) 推荐合并顺序（降低冲突）

1. PR-5.1 `pr-5-1-config-env`
2. PR-5.2 `pr-5-2-metrics`
3. PR-5.3 `pr-5-3-logging`
4. PR-5.4 `pr-5-4-deploy-oneclick`

原因：PR-5.1/5.2/5.3 在同一批配置与入口文件上重叠修改，PR-5.4 基本独立。

## 2) 重叠文件（需要重点关注）

### PR-5.1 与 PR-5.2 重叠
- configs/dev/ctp.yaml
- configs/prod/ctp.yaml
- configs/sim/ctp.yaml
- include/quant_hft/core/ctp_config.h
- src/apps/core_engine_main.cpp
- src/core/ctp/ctp_config_loader.cpp
- tests/unit/core/ctp_config_loader_test.cpp

### PR-5.1 与 PR-5.3 重叠
- configs/dev/ctp.yaml
- configs/prod/ctp.yaml
- configs/sim/ctp.yaml
- include/quant_hft/core/ctp_config.h
- src/apps/core_engine_main.cpp
- src/apps/daily_settlement_main.cpp
- src/apps/simnow_probe_main.cpp
- src/core/ctp/ctp_config_loader.cpp
- tests/unit/core/ctp_config_loader_test.cpp

### PR-5.2 与 PR-5.3 重叠
- configs/dev/ctp.yaml
- configs/prod/ctp.yaml
- configs/sim/ctp.yaml
- include/quant_hft/core/ctp_config.h
- src/apps/core_engine_main.cpp
- src/core/ctp/ctp_config_loader.cpp
- tests/unit/core/ctp_config_loader_test.cpp

## 3) 合并前建议动作

- 每个分支在打开 PR 前，先基于最新 main 执行 rebase。
- 若 PR-5.2 已合入，再处理 PR-5.3 时重点检查：
  - src/apps/core_engine_main.cpp（metrics + structured log 同时保留）
  - include/quant_hft/core/ctp_config.h（metrics 字段与 logging 字段同时保留）
  - src/core/ctp/ctp_config_loader.cpp（metrics/logging YAML 解析同时保留）
- 合并后执行定向验证：
  - ctp_config_loader_test
  - execution_engine_test
  - ctp_gateway_adapter_test
  - ctp_trader_adapter_test
  - daily_settlement_service_test
  - python/tests/test_one_click_deploy_script.py
  - python/tests/test_rollout_orchestrator_script.py
  - python/tests/test_failover_orchestrator_script.py

## 4) PR 描述模板

以下模板可直接复制到 GitHub PR 描述。

---

## 模板：PR-5.1（配置与环境变量治理）

标题建议：
feat(config): env-driven ctp/systemd path governance

描述：

### 背景
将 CTP 运行配置、systemd 启动路径与凭据注入统一为环境变量驱动，消除硬编码路径与明文敏感信息。

### 变更点
- CTP YAML 支持环境变量占位与解析。
- systemd unit 通过 EnvironmentFile 注入运行时变量。
- core_engine/daily_settlement/simnow_probe 默认配置路径改为环境变量驱动。
- 新增配置文档与单测覆盖。

### 风险与兼容性
- 旧环境若未注入必要环境变量会导致启动失败。
- 已在 loader 测试覆盖占位符解析与缺失场景。

### 验证
- ctp_config_loader_test 全通过。
- 关键配置路径与 systemd 启动参数可解析。

---

## 模板：PR-5.2（监控指标接入）

标题建议：
feat(monitoring): integrate metrics registry and exporter

描述：

### 背景
引入统一指标注册与导出能力，完成核心业务路径最小可观测性接入。

### 变更点
- 新增 metric registry 与 exporter。
- core_engine 启动阶段接入 metrics exporter。
- 业务打点覆盖：风险拒单、下单延迟、CTP 连接状态/重连、结算耗时与对账差异。
- 补充 metrics 配置项与运维文档。
- CMake 对 prometheus-cpp 缺失场景进行降级处理（告警但不阻断构建）。

### 风险与兼容性
- 无 prometheus-cpp 环境下 exporter 不启动，但业务功能不受影响。

### 验证
- 定向构建通过。
- ctp_config_loader_test / metric_registry_test / execution_engine_test / ctp_gateway_adapter_test / ctp_trader_adapter_test / daily_settlement_service_test 通过。

---

## 模板：PR-5.3（YAML 结构化日志）

标题建议：
feat(logging): add yaml-driven structured runtime logs

描述：

### 背景
日志配置统一转为 YAML 驱动，入口程序输出统一结构化格式，便于采集与检索。

### 变更点
- 新增 runtime 日志配置项：log_level、log_sink。
- loader 增加日志配置校验与解析。
- 新增 structured_log 工具。
- core_engine/daily_settlement/simnow_probe 切换为统一结构化日志输出。
- 补充 logging 运维文档与 loader 单测。

### 风险与兼容性
- 默认 log_level=info、log_sink=stderr，对现有部署兼容。
- 非法日志配置会在加载阶段失败（快速暴露配置错误）。

### 验证
- core_engine/daily_settlement/simnow_probe 构建通过。
- ctp_config_loader_test 通过（含 logging 新增用例）。

---

## 模板：PR-5.4（一键部署与运维闭环）

标题建议：
feat(ops): add one-click deploy orchestrator and docs

描述：

### 背景
提供统一入口命令，自动执行 rollout/failover 编排并完成证据校验，缩短演练路径。

### 变更点
- 新增脚本：scripts/ops/one_click_deploy.py
- 支持 mode=auto（按环境 YAML 键自动识别 rollout/failover）。
- 自动串联 orchestrator 与 verify 脚本，输出 one_click_deploy_result.env。
- 新增脚本测试覆盖 rollout/failover 双路径。
- 更新 runbook 与 docs/ops 文档。

### 风险与兼容性
- 依赖现有 orchestrator 与 verify 脚本协议（env key 契约）。
- 未改动底层编排逻辑，仅新增统一入口。

### 验证
- python/tests/test_one_click_deploy_script.py 通过。
- python/tests/test_rollout_orchestrator_script.py 通过。
- python/tests/test_failover_orchestrator_script.py 通过。
- python/tests/test_runbook_failover_commands.py 通过。

## 5) 建议评审顺序

- 先审 PR-5.1（配置治理基线）
- 再审 PR-5.2（可观测性）
- 再审 PR-5.3（日志格式统一）
- 最后审 PR-5.4（运维编排入口）

这样可以让配置模型先稳定，再叠加指标与日志，最后增加运维入口，降低 review 复杂度。
