# CTP 治理基线与 SDK 来源

基准提交为 `4aaa1b7b7f369b02fbc540fa648308fc0a6d6efd`，修复分支为
`codex/ctp-correctness-runtime-parity`。本目录记录离线验证；没有进行账户登录、报单或真实资金部署。

## 阶段 0 基线

基准源码在独立 worktree `/tmp/quant-hft-baseline-src`，源码无修改。
环境为 WSL Ubuntu 22.04 x86-64、GCC 11.4、CMake 3.22.1。使用已下载的 GoogleTest
源码以避免旧 CMake 下载选项的兼容问题。可重建命令如下；GoogleTest 路径按本机实际位置替换。

```bash
cmake -S /tmp/quant-hft-baseline-src -B /tmp/quant-hft-baseline-4aaa-build \
  -DQUANT_HFT_BUILD_TESTS=ON \
  -DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=/tmp/quant-hft-build/_deps/googletest-src
cmake --build /tmp/quant-hft-baseline-4aaa-build -j8
ctest --test-dir /tmp/quant-hft-baseline-4aaa-build --output-on-failure -j8
```

完整构建通过。CTest 共 797 项：776 通过、14 失败、7 跳过。
失败涉及源码/构建相对路径、缺少本地行情数据及关闭 Arrow 后仍运行的依赖测试。
这些是环境基线，不能当作交易缺陷已复现的证据。逐项名单见 `baseline_ctest.log`；
构建选项见 `baseline_build_options.txt`。针对原有订单恢复、生命周期、持久化缺陷的
新增测试先在旧实现上失败，证据分别保留为 `regression_*_red.log`。

初始环境未发现 Arrow/Parquet，CTP 配置目录为空。之后在用户缓存中准备了 Arrow/Parquet
16.1.0 C++ 库和隔离 PostgreSQL 14.24，用于研究构建及真实数据库事务测试。
这些依赖不改变策略执行语言，也没有连接既有数据库或已有运行账户。

## SDK 安装结果

用户提供的 Linux 包已完整复制到 CMake 默认目录：

```text
ctp_api/v6.7.11_20250617_api_traderapi_se_linux64/
```

该目录包含四个 API/数据结构头、行情及交易两个 `.so`、`error.xml` 和 `error.dtd`。
源文件与副本逐个 SHA-256 相等；两个库均为 ELF 64 位 x86-64，依赖解析成功。
只调用静态版本接口，输出为：

```text
trader=v6.7.11_20250617 16:22:00.10369
md=v6.7.11_20250617 16:22:00.10369
```

先前提供的 P4 20251120 包是 Windows 版本，其中 64 位文件另存于
`ctp_api/v6.7.11_P4_20251120_windows64/`，不参与 WSL 构建。
SDK 原件保留；供应商文件位于忽略目录，不加入 Git。各文件长度、哈希及来源见
[sdk_manifest.json](sdk_manifest.json)。真实 SDK 构建结果见后续质量验证记录。

```bash
cmake -S . -B build-real -DQUANT_HFT_BUILD_TESTS=ON \
  -DQUANT_HFT_ENABLE_CTP_REAL_API=ON
cmake --build build-real -j8
```

## 官方文档的适用范围

已阅读用户提供的《CTP客户端开发指南 V3.5-中文》与英文开发指南，原件保留在
`D:\20200922_documents`，文件哈希见 SDK 清单。中文指南是 2020 年版，不能替代
6.7.11 头文件及目标柜台的字段样本验证。

指南用于核对 RequestID/IsLast/ErrorID 查询完成语义、持仓日期桶及冻结方向。
第 40 页列出的平仓优先级和手续费减免情形说明通用 Close 不能对所有交易所统一
假设平今或平昨优先。实现保留明确的规则来源/版本及冲突处理；尚未取得目标柜台的
脱敏成交和费用样本，不把估算公式或既有知识文件当作已确认的柜台语义。

构建通过只证明目标 SDK 接口和链接兼容。连续五个完整交易日、至少两个夜盘、
交易日切换与故障演练仍需独立 SimNow 运行证据。
