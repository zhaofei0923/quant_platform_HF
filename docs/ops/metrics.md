# 生产监控指标说明

本文档描述 `quant_hft` 的 Prometheus 指标导出方式、关键指标与排障步骤。

## 启用方式

在各环境 `configs/*/ctp.yaml` 中配置：

```yaml
metrics_enabled: true
metrics_port: 18080
```

并确保编译时开启：

```bash
cmake -S . -B build -DQUANT_HFT_BUILD_TESTS=ON -DQUANT_HFT_WITH_METRICS=ON
cmake --build build -j$(nproc)
```

## 指标端点

核心引擎启动后暴露：

- `http://127.0.0.1:${metrics_port}/metrics`

可用 `curl` 验证：

```bash
curl -s http://127.0.0.1:18080/metrics | head
```

## 关键业务指标

- `quant_hft_risk_reject_total`：风控拒单总数。
- `quant_hft_order_latency_seconds`：下单链路延迟分布（直方图）。
- `quant_hft_ctp_connected`：CTP 连接状态（0/1）。
- `quant_hft_ctp_reconnect_total`：CTP 重连次数。
- `quant_hft_settlement_duration_seconds`：日终结算耗时（直方图）。
- `quant_hft_settlement_reconcile_diff_total`：结算对账差异次数。

## 排障建议

- 若 `/metrics` 不可访问，先检查：
  - 配置文件中 `metrics_enabled` 是否为 `true`；
  - 进程日志中是否出现 exporter 启动失败信息；
  - 端口是否被占用：`ss -lntp | grep 18080`。
- 若指标为空，确认业务路径是否被触发（例如未发生下单、未触发重连则对应指标不会增长）。
