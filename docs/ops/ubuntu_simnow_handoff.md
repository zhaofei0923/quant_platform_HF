# Ubuntu 服务器部署与首次 SimNow 联调

本指南用于把 `codex/ctp-correctness-runtime-parity` 分支部署到另一台 Ubuntu 服务器。
首次交易时段先确认登录、结算确认与行情，再观察引擎恢复和预热。填写 `.env` 并不
自动满足交易条件；已核验核算规则与当日查询证据仍是自动委托的必要输入。

## 1. 拉取代码

已有仓库时，在该仓库根目录执行；若有本地改动，先保留自己的改动，不使用强制重置：

```bash
git status --short
git fetch origin
git switch codex/ctp-correctness-runtime-parity
git pull --ff-only origin codex/ctp-correctness-runtime-parity
git log -1 --oneline
```

第一次拉取可使用下面的 SSH 地址；服务器需要已配置该 GitHub 仓库的读取权限：

```bash
git clone --branch codex/ctp-correctness-runtime-parity --single-branch \
  git@github.com:zhaofei0923/quant_platform_HF.git
cd quant_platform_HF
```

## 2. 单独准备 SDK 和私有配置

Git 不包含 `ctp_api/`、`.env` 或 `runtime/`。用已有的 SSH/SFTP 渠道把本机的
Linux SDK 目录单独传到服务器项目的同名位置：

```text
ctp_api/v6.7.11_20250617_api_traderapi_se_linux64/
  ThostFtdcMdApi.h
  ThostFtdcTraderApi.h
  ThostFtdcUserApiDataType.h
  ThostFtdcUserApiStruct.h
  thostmduserapi_se.so
  thosttraderapi_se.so
  error.xml
  error.dtd
```

使用完整的同一发行包。此包是 Linux x86_64；ARM 服务器不能直接加载它，Windows
P4 包也不能用于这个构建。来源及校验值见
[SDK 清单](../results/ctp_remediation_20260906/sdk_manifest.json)。

可以单独安全传输本机已填写的 `.env`，或在服务器创建并填写；已有文件不覆盖：

```bash
umask 077
cp -n .env.example .env
chmod 600 .env
nano .env
```

填写真实的 SimNow 用户编号、投资者编号和密码，确认认证信息、交易时段前置及
`CTP_SIM_ENABLE_REAL_API=true`。这里的真实 API 指连接 SimNow 柜台，使用
`configs/sim/ctp.yaml`，不切换到实盘配置。配置默认从 `c,hc` 动态选择主力；若使用
静态合约配置，订阅合约必须仍有效。密码含空格或 shell 特殊字符时使用合适的 shell
引号，因为启动脚本会 `source` 文件。不要将 `.env` 内容打印到共享日志。

## 3. 只构建在线入口

以下是 Ubuntu 22.04 x86_64 的最小在线构建：GCC ≥11、CMake ≥3.20，C++17。
OpenSSL 开发包是当前 CMake 配置的必需项。无需为这次在线联调安装 Arrow/Parquet，
也不重复运行完整测试矩阵。

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake git libssl-dev ca-certificates

cmake -S . -B build-real-server \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DQUANT_HFT_BUILD_TESTS=OFF \
  -DQUANT_HFT_ENABLE_CTP_REAL_API=ON \
  -DQUANT_HFT_ENABLE_ARROW_PARQUET=OFF \
  -DQUANT_HFT_ENABLE_REDIS_EXTERNAL=OFF \
  -DQUANT_HFT_ENABLE_TIMESCALE_EXTERNAL=OFF \
  -DQUANT_HFT_WITH_METRICS=OFF \
  -DCTP_V6711_DIR="$PWD/ctp_api/v6.7.11_20250617_api_traderapi_se_linux64"
cmake --build build-real-server --target core_engine simnow_probe runtime_paths_cli -j2
ldd build-real-server/core_engine
ldd build-real-server/simnow_probe
```

`ldd` 不应出现 `not found`。默认 `-j2` 控制编译内存，可按服务器资源增加。
保留 SDK 原目录，构建产物通过该路径加载共享库。服务器还需准确的系统时间、足够的
本地持久磁盘空间，以及到所选 SimNow 行情和交易前置的出站 TCP 网络。

该最小配置使用内存账本和持久 WAL。若服务器已有外部 PostgreSQL/Timescale 或 Redis
账本，继续使用原存储模式、连接信息和对应构建开关，不能用上述关闭外部存储的选项
替换它。外部数据库按现有升级顺序应用至 `008_verified_trade_fees.sql`，见
[核算规则说明](verified_accounting_policy.md)。

## 4. 交易时段先运行 probe

在项目根目录加载配置；`runtime_paths_cli` 只解析路径，不登录、不创建恢复目录：

```bash
set -a
source .env
set +a
./build-real-server/runtime_paths_cli --config configs/sim/ctp.yaml
timeout 180s ./build-real-server/simnow_probe configs/sim/ctp.yaml \
  --monitor-seconds 30 --instrument-timeout-seconds 45
```

Probe 会连接柜台、登录、确认结算、查询合约并订阅行情，不提交交易委托。
正常结束应看到 `event=probe_completed`，并检查行情健康状态。登录成功不等于行情
已经就绪。非交易时段超时不能作为策略或恢复通过的证据。

## 5. 启动引擎并区分观测与自动委托

已完成 probe 后，在同一个已加载 `.env` 的 shell 中直接前台运行，`Ctrl+C` 可结束：

```bash
./build-real-server/core_engine --config configs/sim/ctp.yaml --run-seconds 300
```

若需要现有启动脚本的日志与 PID 管理，可改用以下入口；它包含一次短 probe，
因此无需单独执行上一节的 probe，也不需额外运行整套 preflight 测试脚本：

```bash
bash scripts/ops/start_simnow_trading.sh \
  --env-file "$PWD/.env" --config configs/sim/ctp.yaml \
  --build-dir "$PWD/build-real-server" --foreground --run-seconds 300
```

以上两个入口任选一个，不并行运行同一账户。持续观测时将脚本的 `--run-seconds`
设为 `0`，并放在服务器已有的终端会话管理工具中运行。

缺少 `QUANT_HFT_ACCOUNTING_POLICY_FILE` 时，引擎保持
`trade_semantics_unverified`，仍可尝试连接、采集行情与查询对账，但不会开放新交易。
这不是一个永久的“只读模式”开关：一旦配置有效规则，且所有恢复/预热/权限条件满足，
策略就可能提交委托。未核验的新真实成交只落 WAL，不能将此状态当作完整入账成功。

自动委托前，按[核算规则说明](verified_accounting_policy.md)填写私有 JSON，并在
`.env` 中设置其服务器绝对路径，重启加载。规则须精确覆盖当前活动合约、账户、交易日
和套保范围，含可信的乘数、手续费日期分配和泛平仓顺序证据。程序还须在本会话完成
当日合约信息、成交手续费、报撤单手续费三类查询匹配并同步保存证据；仅把示例中的
`verified` 改为 `true` 不代表完成核验。非零报撤单费目前仍保持阻断。

查询费率快照可在解析出的 `recovery_root` 下的
`state/ctp_instruments/fee_rates.json` 查看；它帮助核对数值，不能代替平仓顺序和
费用日期归属的样本证据。日志中的恢复阻断或预热等待也需分别处理，不能只看进程存活。

## 6. 已有状态和验收记录

保留服务器已有的 `.env`、WAL、身份清单、策略状态、费率证据和外部账本。新版本使用
`runtime/<environment>/<broker>/<account>/<instance>/` 稳定恢复目录；不要通过删除
状态、改账户身份或更换空目录来绕过恢复失败。旧格式状态迁移按
[恢复与验收说明](ctp_recovery_and_acceptance.md)保留原件并核对身份及覆盖边界。
跨服务器接管同一账户时，先停止原主机的在线进程；文件锁不能阻止两台机器同时下单。

首次交易时段联调只是验收起点。连续五个完整交易日、至少两个夜盘及一次交易日切换的
验收仍需实际记录，当前 Git 提交中的离线测试不替代这些结果。
