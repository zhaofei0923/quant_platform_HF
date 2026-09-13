# 固定止盈发布记录（2026-09-09）

## 结果与适用范围

按用户要求将 KAMA/Trend 的 ATR 止盈改为持仓固定目标，发布共享策略包 1.1.0，并重新构建在线宿主与研究宿主。2026-09-09 23:29 CST 在交易核心按原会话调度自然停止后切换腾讯云单账户 SimNow 监督服务。新监督服务常驻，核心仍休市停机；下一个交易时段的恢复、实际行情触发和看板实时价格核验尚未发生。本记录不把离线测试或监督服务启动等同于在线交易验收。

首次确认非零持仓且有该合约有效 ATR 与开仓均价时，锁定 `均价 ± ATR × 止盈倍数`。后续行情、ATR、均价变化、加仓及部分平仓不移动已锁定目标；清仓、换向或实际持仓归属变化时重建。止盈判断和看板风险快照使用同一目标。跟踪止损继续沿用原规则。缓存不足时保留缺失状态，不跨合约取 ATR。成交去重完成后才更新策略归属。

已有 `hc2701` 空仓 1 手的均价为 3373.00，迁移沿用停机前最后保存的止盈值 `3257.2367647510764`（页面两位小数 3257.24）。历史开仓 ATR 不完整，因此没有声称重建原始开仓目标。持仓、成本、归属、非原子状态、水位和 WAL 均保持原事实。

## 版本和验证

| 项目 | 验证结果 |
|---|---|
| 共享策略包 | 1.1.0；本地提交 `719d55e39a098a6c1455a5aafe5804f00eb7d9f4`；50 个 CTest 组通过，安装消费者与依赖检查通过 |
| 在线宿主 | real CTP 与 PostgreSQL 编译；113 个相关测试通过，含 9 个正式版本迁移案例；依赖、三仓边界、源码及文档纯度检查通过 |
| 研究宿主 | 同一已安装 1.1.0 包；4 个 CLI 构建、20 个聚焦测试与依赖检查通过；675 根合成 Bar 回放、4 笔模拟成交；旧版本运行配置被拒绝 |
| 实际检查点迁移 | 新包 LoadState/SaveState 及 envelope 回读成功；目标价与方向保持；所有非原子事实哈希一致；其余检查点和元数据目录逐字复制 |
| 生产切换 | 新包配置与 `--check-only` 通过；监督进程实际命令、环境及 build-dir 均指向新包；核心未在休市意外启动 |
| 看板隔离 | Nginx、登录和发布器 PID 未变；未登录首页 302、登录页 200、JSON 401、私有元信息 404；公开字段脱敏和来源只读权限通过 |

固定止盈、动态 ATR、成交即锁定、部分平仓、加仓、清仓重开、换向、多个合约、归属切换、重复回报和重启恢复均有针对性测试。没有提交测试订单。当前看板发布器持续工作，但停机快照呈陈旧、运行块不完整；不将其标成实时正常。

关键 SHA256：

```text
strategy archive: 582747391b6d0a5a2213e9834cbe76589128c6141d79cf59f667d9d83b165d7d
strategy manifest: 5b5da646de1d0217cbc9388b892f547b026ca134bcbb5f32db0604d65dc9c1be
online archive: 30bf1260028ccd04baa7febb8bb559a3b2deeb4a35aeae0e84961834ef8a11ab
core_engine: af3c47c778fda6830068bd6b70a5b98a27d4c853130beed21e48461472ade364
state before: 295fa78f5054177e54b64455cc012a18001abf3cb2aabd4522d8fbc8ff94e38f
state after: 03c85a3131f5626ef26f7bead6d4f39afcfa43502c93b0d6f42ded55e6300138
facts before/after: bc0b951f6c74735e814d0c257e3f3329c02f206aecbec9b9239805d8b069d67f
WAL before/after: 8e6a2f5cf809e09e5c51f391e046cfb676255cc8404263d3a5d055b770d809f9
```

## 部署与回滚资料

- 发布目录：`/home/ubuntu/quant_releases/quant-platform-hf-v1.1.0-simnow.1`。
- 新部署配置目录：`/home/ubuntu/.config/quant/simnow_fixed_take_profit_20260909`；沿用旧连接及凭据文件引用，参数集新 ID 为 `hc_cloud_5m_v002`，参数语义、账户、资金与风险限制不变。
- 原状态、服务文件、迁移报告与私有验证日志：`/home/ubuntu/quant_hft_migration_archives/20260909-fixed-take-profit`；原发布包与原部署文件保留。
- 本机证据：`/home/kevin/quant_split_evidence/20260909/fixed_take_profit`；看板 HTTP 与脱敏检查为相邻 `tencent_simnow_deploy/dashboard_verification_fixed_take_profit_cutover.json`。

回滚必须匹配旧包、旧 deployment 和旧 envelope/checkpoint；先停监督服务并核实核心退出、检查点及 WAL 未推进。若新交易事实已产生，不得直接覆盖旧状态。切换脚本的失败路径先停止新监督服务，仅在核心不存在、检查点和 WAL 均未变化时恢复旧入口与状态。正式迁移方法见[三项目发布说明](../ops/three_project_migration.md)。
