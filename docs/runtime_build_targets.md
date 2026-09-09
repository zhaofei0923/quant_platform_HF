# 在线构建目标

本仓使用C++17，依赖已安装且通过校验和验证的QuantStrategies静态包。回测、优化、滚动验证、Arrow和Python数据工具由quant_research维护。

| 目标 | 职责 |
|---|---|
| quant_hft_contracts | 本仓接口路径与Quant::Contracts导入目标 |
| quant_hft_common | 在线时钟/文件/等待适配器、事件调度、CTP配置、监控 |
| quant_hft_storage | SQL/Redis适配器、账户事务和独立策略账本 |
| quant_hft_wal_io | WAL接收和读取 |
| quant_hft_services | 在线执行、待退出、账户持仓查询、结算价格和行情录制 |
| quant_hft_strategy | StrategyEngine调度与宿主状态持久化 |
| quant_hft_ctp | CTP连接、查询和可靠回调 |
| quant_hft_deployment | 部署解析、参数物化和静态包身份验证 |
| quant_hft_state_migration | 旧状态到带版本封装的受控迁移 |
| quant_hft_online_runtime | 账户目录、进程锁、收据恢复和结算装配 |
| quant_hft_core | 兼容的在线聚合接口，不重复编译实现 |

具体策略、指标、时间与Bar纯计算、费用和通用风险规则只存在于Quant::Strategies、Quant::Indicators、Quant::Model、Quant::Contracts。应用启动时显式提供真实时钟与文件适配器；研究回放自行绑定模拟时钟。

共享包发布和宿主迁移见[迁移说明](ops/three_project_migration.md)，实际验证见[交付记录](results/three_project_split_validation.md)。
