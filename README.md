# quant_platform_HF — 在线交易宿主

本项目负责 SimNow/实盘连接、一个账户一个进程、执行与账户风控、策略经济账与柜台实际账、持久化恢复和只读监控。回测研究在独立 Git 项目 `quant_research`；算法、自研指标及纯计算规则由独立 Git 项目 `quant_strategies` 发布。迁移在隔离工作树进行，原运行服务未升级。

## 构建与依赖

在线宿主使用 C++17、GCC 11+、CMake 3.20+、OpenSSL、yaml-cpp。测试使用 GTest。先安装 `QuantStrategies 1.0.0` 静态包，构建会核对 `dependencies.lock.json` 和已安装文件校验和；不引用兄弟仓源码，不需要研究环境。

```bash
cmake -S . -B build -DQUANT_HFT_BUILD_TESTS=ON -DCMAKE_PREFIX_PATH=/path/to/quant-strategies
cmake --build build -j
ctest --test-dir build --no-tests=error --output-on-failure
bash scripts/build/three_project_boundary_check.sh --build-dir build
```

真实 CTP 使用 `-DQUANT_HFT_ENABLE_CTP_REAL_API=ON -DCTP_V6711_DIR=/external/sdk/path`，SDK和凭据不进入源码包。在线仓禁止启用 Arrow；相应构建目标在研究仓。

## 账户与策略配置

正式入口是部署清单。参数集属于策略库；账户、初始分配、风险限制及策略实例绑定属于本仓。请先根据 `configs/deploy/instances.example.yaml` 创建仓库外部署文件，填入真实账户引用、活动主机、安装包及参数集的校验和。示例不代表运行许可或当前账户。

```bash
build/quant_config_cli validate /restricted/deployment.yaml
build/quant_config_cli resolve /restricted/deployment.yaml /review/resolved.json
build/quant_config_cli list /restricted/deployment.yaml
build/quant_config_cli launch /restricted/deployment.yaml simnow_a
```

`launch` 是实际启动命令；本次实现验证不自动调用它。每次只启动一个明确账户，凭据文件须为当前用户所有且权限0600，内容只允许CTP环境变量赋值。工具不通过shell执行凭据文件。`validate/resolve/list`不读取凭据内容。

同一账户可运行同品种多个独立实例。意图、委托、成交、资金及状态以实例归属；组合内部component只标识信号来源。对手方向不净额合并，不自动撤销其他实例委托。未知归属、缺少资金或核算规则时开仓被阻止。

旧配置仅用于一次性迁移；`QUANT_HFT_ALLOW_LEGACY_CONFIG=1` 是显式兼容入口，不能与新部署入口混用为长期配置源。

## 模块

- `src/core/ctp`：行情、交易API与柜台查询。
- `src/core/runtime`：账户串行调度、运行身份与路径。
- `src/core/storage`：权威事务、资金划拨、两套持仓分配及成交去重。
- `src/services`：在线执行、持久化冻结、恢复及退出。
- `src/strategy`：宿主回调调度与状态持久化；没有具体算法的第二份实现。
- `src/core/config`：严格部署解析、引用检查、来源/哈希及一次性迁移。
- `dashboard`：独立只读展示，不控制交易进程。

当前迁移和发布约束见 [三项目迁移说明](docs/ops/three_project_migration.md)。完整旧设计保存在Git基线与 `docs/archive`，不能据此认定新版本已通过实盘验收。
