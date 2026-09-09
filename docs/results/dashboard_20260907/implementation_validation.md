# 个人量化 Dashboard 本地实施与验证

日期：2026-09-07。状态：**本地实现已验证并部署腾讯云；浏览器登录/退出与下一次核心快照待验收**。

## 已交付

- `web/dashboard`：中文深色、电脑与手机布局，资金总览、券商持仓与策略净仓、订单成交筛选、策略与运行四个只读页面；2 秒串行刷新、来源独立时效、登录失效清屏及退出。
- C++ 有限快照缓存与后台原子写入，默认关闭；没有新增 CTP 登录或查询。只有完整成功的持仓查询才替换券商持仓，失败保留上次有效记录并标识不完整。
- 独立 C++17 发布器：身份绑定、字段筛选、增量 WAL 校验/去重/状态归并、断点恢复、每分钟真实权益采样、90 个自然日留存、公共数据范围标识与原子 JSON。
- Nginx / Authelia / systemd 配置、打包、预检、安装、证书更新和回滚脚本。详情见 [部署说明](../../ops/tencent_dashboard.md) 和 [接口说明](../../ops/dashboard_data_contract.md)。

## 验证结果

| 检查 | 实际结果与边界 |
| --- | --- |
| 完整 C++ 构建 | `/tmp/quant-hft-root-build` 成功；GCC 11.4、C++17、真实 CTP SDK 编译路径开启。没有由此宣称真实 CTP 登录验收。 |
| 完整 CTest | 注册 1011 项：988 通过、23 跳过、0 失败。20 项依赖未启用特性/外部环境；3 项部署测试需要显式工具/权限，在独立隔离运行中另行验证。 |
| 新增 C++ 数据验证 | 12 项快照测试、6 项 WAL 格式校验、3 项 WAL 权限测试以及单独发布器程序内 21 组集成场景通过；覆盖首次查询失败、空仓、多空/今昨、跨日/身份、坏文件、去重、重启、写入失败和源中断。 |
| Sanitizer | 快照 12 项、发布器 20 组通过 ASan / UBSan；发布器同时通过泄漏检查。此检查不是腾讯云性能验收。 |
| 前端单元与真实契约 | 12/12 通过，含 3 项消费真实 C++ Publisher 生成的明确测试身份数据。 |
| Chromium 浏览器 | 12/12 通过；电脑、390px 手机、过滤、有效空仓/未知、陈旧提示、跨日、跨账户历史拒绝、坏数据恢复、登录跳转和退出清屏。界面测试中的登录目标由测试路由模拟。 |
| 实际认证协议 | 使用实际 Nginx + Authelia 进程验证 HTTPS、登录、Cookie、直接 JSON 保护、限速、退出、失效及认证故障。另用真实临时 systemd 服务验证持续访问旧 Cookie 在进程替换后被拒绝。见 [部署验证](deployment_validation.md)。 |
| 仓库检查 | dependency audit、repo purity、doc purity、配置文档覆盖和 `git diff --check` 通过。 |
| 前端构建与依赖 | 生产构建成功，运行依赖审计 0 项已知漏洞；演示数据、源码映射和私有数据不进入前端发布目录。 |

## 修复与实现边界

全量测试发现已有新配置未进入配置目录文档，已补齐 4 项。Preflight 夹具补齐受控日历与固定测试时间，并将它设为串行运行，避免并行 supervisor 测试进程触发其宿主级进程检查。没有删除原安全断言或放松生产检查。

Authelia v4.39.22 的服务端会话 TTL 会随请求延长。为落实最长 8 小时，内存会话服务配置 `RuntimeMaxSec=7h59min55s` 和 5 秒停止期限，周期重启时旧会话失效；用户可能提前重新登录，短暂重启期间拒绝数据访问。隔离测试验证了旧 Cookie 持续重放也不能跨越进程替换。

核心快照观察入口的本机微基准在 20000 组账户与两笔持仓回调下未观察到动态分配；中位数 187ns、P99 232ns、最大 41.9μs。这是 WSL 单机测试结果，不能用于承诺实际交易延迟或腾讯云负载。

腾讯云部署与主机隔离检查见 [云端部署验证](cloud_deployment_validation.md)。没有读取用户密码、私钥内容或 `.env`。交易核心观察功能已在休市维护窗口编译并配置，调度器已恢复；首份真实账户快照、浏览器登录/退出和接入前后实时延迟/资源比较仍需在下一次核心启动后完成。

## 重现

```bash
cmake --build /tmp/quant-hft-root-build -j8
ctest --test-dir /tmp/quant-hft-root-build --output-on-failure -j8
bash scripts/build/dependency_audit.sh --build-dir /tmp/quant-hft-root-build
bash scripts/build/repo_purity_check.sh --repo-root .
bash scripts/build/doc_purity_check.sh --repo-root .
python3 scripts/build/verify_config_docs_coverage.py
cd web/dashboard
npm ci
npm run build
npm test
npm run test:e2e
```

真实发布器契约样本测试需要显式提供 `DASHBOARD_CONTRACT_FIXTURE` 路径；普通 `npm test` 缺少该目录时会跳过对应 3 项，不能将跳过报告成实际契约验证。隔离认证、权限和会话测试的输入及重现方式见部署文档。

本次完整构建、CTest、前端测试日志与电脑/手机/无数据截图保存在工作区 `runtime/dashboard/validation/20260907`。截图使用明确标记的示例数据，不代表真实账户资金或收益。
