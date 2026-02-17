# PR-6.1 数据层落地说明

## 目标

PR-6.1 提供可复现的回测数据基础能力：

- 将 `backtest_data/*.csv` 转换为分区化 Parquet。
- 对产出分区进行结构化校验并产出报告。
- 引入 DVC 远端配置模板，支持数据版本化。
- 提供 C++ `ParquetDataFeed` 元数据发现与时间窗口查询能力。

## 脚本

- 转换脚本：`scripts/data/convert_backtest_csv_to_parquet.py`
- 校验脚本：`scripts/data/validate_backtest_parquet_dataset.py`

## DVC

- 仓库配置：`.dvc/config`
- 使用说明：`infra/dvc/README.md`
- Pipeline：`dvc.yaml`

## C++ 数据接口

- 头文件：`include/quant_hft/backtest/parquet_data_feed.h`
- 实现：`src/core/backtest/parquet_data_feed.cpp`
- 单测：`tests/unit/backtest/parquet_data_feed_test.cpp`

## Notebook 模板（一期）

一期先提供最小模板建议（后续 PR-6.5 落地 notebook 文件）：

1. 数据准备（CSV -> Parquet）
2. 数据校验（分区/覆盖率）
3. 策略回放（样本集）
4. 绩效报表（empyrical + pyfolio）
