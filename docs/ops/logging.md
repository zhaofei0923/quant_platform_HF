# 结构化日志配置（YAML）

本文档描述 CTP 运行配置中的日志项，统一使用 YAML 配置，不使用 JSON 配置文件。

## 配置项

在 `configs/*/ctp.yaml` 中配置：

```yaml
log_level: "info"   # debug | info | warn | error
log_sink: "stderr"  # stderr | stdout
```

## 生效规则

- `log_level` 表示最小输出级别。
- `log_sink` 控制输出流：
  - `stderr`：输出到标准错误（默认，推荐用于服务进程）；
  - `stdout`：输出到标准输出（适合本地调试）。

## 日志格式

核心进程、日终结算进程与探针输出统一结构化格式：

```text
ts_ns=<epoch_ns> level=<level> app=<app_name> event=<event_name> key1="value1" ...
```

示例：

```text
ts_ns=1760000000000000000 level=error app=core_engine event=redis_client_unhealthy error="timeout"
```
