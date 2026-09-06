# 修复后运行时固定负载记录

本记录测量账户串行调度、逐记录同步 WAL 提交和完整性校验扫描。使用 WSL Ubuntu
22.04、GCC 11.4，探针及三个被测实现直接以 C++17 `-O2` 编译；无 SDK、网络或真实账户。
同机可能存在并行构建，结果是本机观测值，不是隔离硬件性能承诺。

固定负载为一个测试账户、一个执行线程、全部任务同一 deadline 的突发队列。
每个任务写一条约 787 字节的订单事件并 `fsync`，不使用分组提交、绑核或无锁结构。
完成后校验整份 WAL 并逐条访问。检查全部接收序号连续，访问数量等于提交数量。

| 指标 | 1,000 条 | 10,000 条 |
|---|---:|---:|
| WAL 大小 | 786,780 B | 7,887,780 B |
| 同步提交吞吐 | 474.34 条/s | 487.74 条/s |
| 单条 WAL 提交 P99 | 3.08 ms | 3.69 ms |
| 队列最高水位 | 1,000 | 10,000 |
| 相同 deadline 突发的执行迟到 P99 | 2.12 s | 20.33 s |
| 校验并访问全部 WAL | 59.15 ms | 483.94 ms |
| 进程峰值 RSS | 5,560 KiB | 26,396 KiB |

该突发场景显示逐条同步的吞吐与积压代价；不能把单条 WAL P99 当作排队后的业务
响应时间。校验扫描不包含领域事务、策略恢复或柜台对账，因而不是完整恢复耗时。
扫描器当前保存有上限的完整文件快照，其内存会随 WAL 大小增长；Arrow 输入游标
的有界性结论不适用于此路径。这里不据此放宽过载门控或减少同步次数。

```bash
g++ -std=c++17 -O2 -Iinclude \
  docs/results/ctp_remediation_20260906/runtime_benchmark.cpp \
  src/core/regulatory/local_wal_regulatory_sink.cpp \
  src/core/regulatory/wal_replay_loader.cpp \
  src/core/runtime/account_execution_scheduler.cpp -pthread \
  -o /tmp/quant-hft-runtime-benchmark
/tmp/quant-hft-runtime-benchmark 1000 /tmp/new-benchmark-1000.wal
/tmp/quant-hft-runtime-benchmark 10000 /tmp/new-benchmark-10000.wal
```

输出路径必须尚不存在。原始结果为 `runtime_benchmark_1000.json`、
`runtime_benchmark_10000.json`。探针最初误以为序号从 1 开始；按契约改为核对
`first_sequence + index` 后采集本表，未因此修改 WAL 实现。

阶段 4 的输入读取和逐 Tick 策略微基准见 [研究验证](research_validation.md)。
真实运行的网关时延、全部策略队列、领域存储延迟及端到端恢复仍需 SimNow 验收。
