# 详细使用说明（开发者快速上手，生产主链路）

> 目标：从 0 到“能构建、能跑核心流程、能做基础验证”。

## 1. 环境准备

## 1.1 系统与依赖
- Linux（建议 Ubuntu）
- CMake >= 3.20，C++17 编译器，Python 3.10+
- 可选：Docker（用于 Redis/基础设施本地模拟）

## 1.2 Python 环境
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## 1.3 配置凭据
```bash
cp .env.example .env
set -a && source .env && set +a
env | grep '^CTP_SIM_'
```

## 2. 构建与测试

## 2.1 C++ 构建
```bash
cmake -S . -B build -DQUANT_HFT_BUILD_TESTS=ON
cmake --build build -j$(nproc)
```

如遇到 WSL/Windows 混合工具链污染（例如误用 `g++.exe`），建议使用隔离目录：
```bash
cmake -S . -B build-gcc \
  -DQUANT_HFT_BUILD_TESTS=ON \
  -DCMAKE_C_COMPILER=/usr/bin/gcc \
  -DCMAKE_CXX_COMPILER=/usr/bin/g++
cmake --build build-gcc -j$(nproc)
```

## 2.2 C++ 单元测试
```bash
ctest --test-dir build --output-on-failure
```

隔离目录对应：
```bash
ctest --test-dir build-gcc --output-on-failure
```

## 2.3 Python 测试（按需）
```bash
pytest python/tests -q
```

## 3. 运行核心链路

## 3.1 启动 Redis（桥接模式示例）
```bash
docker run --rm -p 6379:6379 redis:7-alpine
```

## 3.2 启动 core_engine
```bash
export QUANT_HFT_REDIS_MODE=external
export QUANT_HFT_REDIS_HOST=127.0.0.1
export QUANT_HFT_REDIS_PORT=6379
export CTP_SIM_PASSWORD='your_password'

./build/core_engine configs/sim/ctp.yaml --run-seconds 30
```

## 3.3 启动策略运行器
```bash
.venv/bin/python scripts/strategy/run_strategy.py \
  --config configs/sim/ctp.yaml \
  --strategy-id demo \
  --run-seconds 30
```

## 3.4 验证 Bar 分发闭环
```bash
.venv/bin/python -m pytest \
  python/tests/test_bar_dispatch_e2e.py \
  python/tests/test_strategy_runner.py -q
```

## 4. 回测重放

## 4.1 快速回放
```bash
scripts/backtest/replay_csv.py --csv backtest_data/rb.csv --max-ticks 5000
```

## 4.2 输出报告
```bash
scripts/backtest/replay_csv.py \
  --csv backtest_data/rb.csv \
  --scenario-template deterministic_regression \
  --emit-state-snapshots \
  --report-json runtime/backtest/report.json \
  --report-md runtime/backtest/report.md
```

## 5. WAL 恢复验证

## 5.1 离线重放工具
```bash
./build/wal_replay_tool runtime_events.wal
```

## 5.2 证据校验脚本（示例）
```bash
.venv/bin/python scripts/ops/verify_wal_recovery_evidence.py \
  --evidence-file docs/results/wal_recovery_result.env
```

## 6. Data Pipeline（导出与归档）

## 6.1 单次运行
```bash
.venv/bin/python scripts/data_pipeline/run_pipeline.py \
  --analytics-db runtime/analytics.duckdb \
  --export-dir runtime/exports \
  --archive-local-dir runtime/archive \
  --run-once
```

## 6.2 常用环境变量
- `QUANT_HFT_ARCHIVE_ENDPOINT`
- `QUANT_HFT_ARCHIVE_ACCESS_KEY`
- `QUANT_HFT_ARCHIVE_SECRET_KEY`
- `QUANT_HFT_ARCHIVE_BUCKET`
- `QUANT_HFT_ARCHIVE_LOCAL_DIR`
- `QUANT_HFT_ARCHIVE_PREFIX`

## 7. Systemd 部署（单机基线）

## 7.1 渲染 unit 与 env 模板
```bash
.venv/bin/python scripts/ops/render_systemd_units.py \
  --repo-root . \
  --output-dir deploy/systemd \
  --service-user "$USER"
```

## 7.2 复制并填写 env
```bash
cp deploy/systemd/quant-hft-core-engine.env.example deploy/systemd/quant-hft-core-engine.env
cp deploy/systemd/quant-hft-data-pipeline.env.example deploy/systemd/quant-hft-data-pipeline.env
```

## 7.3 安装与启动（系统级）
```bash
sudo cp deploy/systemd/quant-hft-core-engine.service /etc/systemd/system/
sudo cp deploy/systemd/quant-hft-data-pipeline.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now quant-hft-core-engine.service
sudo systemctl enable --now quant-hft-data-pipeline.service
```

## 7.4 健康检查
```bash
systemctl status quant-hft-core-engine.service --no-pager
systemctl status quant-hft-data-pipeline.service --no-pager
journalctl -u quant-hft-core-engine.service -n 200 --no-pager
journalctl -u quant-hft-data-pipeline.service -n 200 --no-pager
```

## 8. 常见故障排查
- CTP 连接失败：先跑 `scripts/ops/ctp_preflight_check.py` 检查配置、密码、端口可达性
- 策略无输出：检查 Redis 模式、键前缀、`strategy_runner` 日志与 `test_bar_dispatch_e2e` 是否通过
- 回测无事件：确认 CSV 字段符合 `docs/BACKTEST_REPLAY_HARNESS.md` 约定
- systemd 启动失败：优先看 `journalctl`，再核对 env 文件中的 `CTP_SIM_PASSWORD`

## 9. 推荐最小验收路径
1. `./scripts/build/bootstrap.sh`
2. `ctest --test-dir build --output-on-failure`
3. `pytest python/tests/test_strategy_runner.py -q`
4. 运行 `core_engine + run_strategy` 30 秒闭环
5. 运行一次 `scripts/backtest/replay_csv.py`
6. 运行一次 `run_pipeline.py --run-once`

### 9.1 主力连续回测深度集成（v3）快速验收（一键）
```bash
scripts/ops/run_v3_acceptance.sh
```

常用参数（可选）：
```bash
scripts/ops/run_v3_acceptance.sh --skip-configure
scripts/ops/run_v3_acceptance.sh --build-dir build-gcc --max-rto-seconds 10 --max-rpo-events 0
```

## 10. 参考文档
- `README.md`
- `docs/STRATEGY_BRIDGE_REDIS_PROTOCOL.md`
- `docs/BACKTEST_REPLAY_HARNESS.md`
- `docs/WAL_RECOVERY_RUNBOOK.md`
- `docs/SYSTEMD_DEPLOYMENT_RUNBOOK.md`
- `docs/K8S_DEPLOYMENT_RUNBOOK.md`
