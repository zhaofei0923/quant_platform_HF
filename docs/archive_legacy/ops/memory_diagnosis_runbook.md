# 混合回测内存根因诊断 Runbook

本 Runbook 用于定位“Python + C++ 混合架构下进程被杀”的根因，目标是先证据后修复。

## 1. 快速开始

```bash
chmod +x scripts/ops/run_memory_diagnosis.sh

# 最小复现（建议先跑）
scripts/ops/run_memory_diagnosis.sh --mode minimal

# 扩展 pytest 复现矩阵
scripts/ops/run_memory_diagnosis.sh --mode pytest

# 混合高压链路（带队列与吞吐特征）
scripts/ops/run_memory_diagnosis.sh --mode hotpath --hotpath-duration 600 --python-queue-size 200
```

输出目录默认在：

`runtime/memory_diag/<timestamp>/`

## 2. 关键产物说明

- `environment.env`：本次诊断参数与环境快照。
- `oom_before.log` / `oom_after.log`：内核 OOM 线索（若权限受限会标记不可访问）。
- `*.rss.log`：按秒采样的 `VmRSS/VmHWM` 时间线。
- `*.stderr.log`：`/usr/bin/time -v` 输出（含最大驻留内存、退出状态）。
- `*.meta`：每个命令的退出码。
- `hotpath_output.json`：hotpath 压测统计（若执行 hotpath 模式）。

## 3. 如何判定三类问题

### A. 真泄漏（对象释放失败）

特征：

- 同一小数据重复运行时，`VmRSS/VmHWM` 与进程峰值持续抬升。
- 负载下降后也不回落。

建议进一步补充：

```bash
PYTHONTRACEMALLOC=25 scripts/ops/run_memory_diagnosis.sh --mode minimal
```

### B. 背压积压（队列/容器增长）

特征：

- 高压时先出现处理延迟、丢弃、队列占用上升，再出现 RSS 抬升。
- 调小 `--python-queue-size` 或提高回调处理能力时，曲线明显变化。

建议对照：

```bash
# 基线
scripts/ops/run_memory_diagnosis.sh --mode hotpath --hotpath-duration 300 --python-queue-size 1000

# 强背压
scripts/ops/run_memory_diagnosis.sh --mode hotpath --hotpath-duration 300 --python-queue-size 200
```

### C. 碎片化 / RSS 不回收

特征：

- 业务对象规模趋稳后，RSS 仍保持高位。
- 更换分配器参数后 RSS 行为明显改变。

建议对照：

```bash
# 默认
scripts/ops/run_memory_diagnosis.sh --mode minimal

# 分配器对照
PYTHONMALLOC=malloc MALLOC_ARENA_MAX=2 scripts/ops/run_memory_diagnosis.sh --mode minimal
```

## 4. 建议执行顺序

1. `--mode minimal` 固化最小基线。
2. `--mode pytest` 扩展复现范围并确认路径稳定性。
3. `--mode hotpath` 做短时高压，观察背压特征。
4. 做 `PYTHONMALLOC` 对照，判定碎片化可能性。
5. 若确认 OOM，再进入代码级修复（队列策略、桥接复制、对象生命周期）。
