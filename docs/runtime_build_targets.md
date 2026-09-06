# 运行时与构建目标

仓库保留一份领域、策略和指标实现；回测使用独立研究运行时，SimNow/实盘共享在线
运行时并通过配置与稳定账户目录隔离。基础路径为 C++17，开启 Arrow/Parquet 时使用
C++20。CMake 使用显式源文件清单。

| CMake 目标 | 职责及主要依赖 |
|---|---|
| `quant_hft_contracts` | 公共类型、接口和头文件路径 |
| `quant_hft_common` | 指标、纯配置、时间与通用组件 |
| `quant_hft_storage` | 存储客户端、同连接事务、领域账本 |
| `quant_hft_wal_io` | WAL 接收、格式校验与只读访问 |
| `quant_hft_services` | 风控、订单、持仓、共享市场流水线；依赖 storage/common |
| `quant_hft_strategy` | 策略与投影状态；依赖 services/common |
| `quant_hft_ctp` | CTP SDK、查询、回调收据及可靠 inbox；依赖 wal_io/common |
| `quant_hft_backtest` | 历史数据、模拟成交、虚拟时钟与研究导出；依赖 strategy/services |
| `quant_hft_optim` | 参数优化；依赖 backtest |
| `quant_hft_rolling` | 滚动与选型验证；依赖 optim/backtest |
| `quant_hft_online_runtime` | 在线配置装配、账户调度、目录、恢复及结算查询 |
| `quant_hft_core` | 兼容的 CMake 聚合接口目标，不重复编译实现 |

`core_engine` 链接 online_runtime/ctp/strategy；研究 CLI 直接链接 backtest、optim
或 rolling。真实 SDK 构建中已核对 `core_engine` 的两份 CTP 动态库，同时核对
`backtest_cli` 不链接 CTP。领域执行依赖公共 `IExecutionGateway`，不直接依赖具体
CTP adapter。

旧 `include/quant_hft/apps/backtest_replay_support.h`、`backtest_metrics.h`、
`cli_support.h` 保留轻量兼容入口。研究实现迁入 `src/backtest/`，包括报告导出；
下层代码不再 include 应用层头。保留 CLI 名称和主要参数，新增语义元数据为附加字段。

构建与验证记录见 [质量验证](results/ctp_remediation_20260906/quality_validation.md)，
研究协议见 [回测运行语义](backtest_runtime_semantics.md)，运行目录、政策核验与
SimNow 验收见 [恢复与验收](ops/ctp_recovery_and_acceptance.md)。
