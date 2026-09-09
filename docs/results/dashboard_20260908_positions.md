# 持仓页单合约列表发布记录

2026-09-08 21:56 CST 完成静态前端发布。

## 变更

- 原三个面板合并为一个持仓列表，按具体合约归并并排序。
- 券商原始记录汇总一次，多空不相抵；多策略止损、止盈分别显示。
- 每行可展开券商与策略原始明细，手机使用单合约卡片。
- 保留独立来源时间、质量、未绑定和待核对提示；无对应合约行情时显示缺失。
- 仅修改前端及其测试；接口、发布器和交易核心未变。

## 验证

- Vitest：38 passed；3 个依赖外部发布器样本的 contract 测试按既有条件跳过。
- Playwright：21/21 passed，包含多合约、多策略、多空及今昨仓、未绑定、缺失、休市、两秒刷新、详情保持及实例切换、退出和会话到期。
- 1548px 桌面与 390px 手机截图已目检；手机展开详情后无横向溢出。
- TypeScript 与 Vite 生产构建通过，仓库纯度和差异空白检查通过。
- 新版 index、CSS 和两个 JS 文件的服务器 SHA256 与本地构建完全一致；网页用户可读取。
- 公网匿名 JSON 返回 401，首页跳转登录门户，登录门户返回 200。
- 已登录的用户浏览器自动化选择超时；登录后的新版页面以本地受控数据浏览器测试验收，未声称已在用户线上会话中目检。

## 发布与回滚

- 新静态发布：`/opt/quant-dashboard/web-releases/20260908T135547-single-contract-b1d8a34b/web`。
- 新版源码包：同一发布目录上一级的 `source.tar.gz`，不在公开站点根目录内。
- 备份：`/var/backups/quant-dashboard/20260908T135547-single-contract-b1d8a34b-frontend`，含旧网页、站点配置和进程核对记录。
- 快速回滚：以管理员身份运行备份目录内的 `rollback.sh`。脚本先确认站点配置没有后续修改，仅恢复原静态根目录并平滑加载 Nginx。
- Nginx 站点仅更改一处 `root`，保留原版 assets 供已打开页面使用；登录并发修复保持原值。
- `current` 链接仍指向原完整发布。当前网页实际版本以 Nginx 站点的 `root` 为准，不能用 `current/web` 推断。
- Auth PID 2813485、Publisher PID 2813486、Nginx 主 PID 1114、core_engine PID 2911252，发布前后均相同；交易用户服务 PID 3832353 及启动时间保持不变。

## 构建标识

静态包 SHA256：`b1d8a34b8b187d7bd6cf961eda3a37a77826320a4ff2cb898342efaf16cf7754`。

index.html SHA256：`5af4663703eb88c78560b36cd63f0ecc9ca7247e1331bf01540acd2d69aaf147`。

浏览器截图保留在本机 WSL `/tmp/quant-dashboard-browser-tests/positions-*.png`。

## 后续更新：数值固定两位小数

2026-09-08 按用户纠正，仅调整持仓列表和详情的数字显示精度，没有新增开仓价列。最新价、止损、止盈、开仓均价、距离、百分比与持仓数量均固定两位小数；金额原已使用两位小数。合约代码、来源时间和刷新周期等标识与文案保持原样。计算及原始接口数据不进行舍入。

- 生产构建通过；更新既有浏览器断言后 21/21 passed，桌面与 390px 手机截图目检通过。
- 当前静态版本：`/opt/quant-dashboard/web-releases/20260908T142340-two-decimals-9057ce5c/web`。
- 本次备份及回滚脚本：`/var/backups/quant-dashboard/20260908T142340-two-decimals-9057ce5c-frontend/rollback.sh`。
- 静态包 SHA256：`9057ce5c188cd08c954fa53467f93366dcbc1899dffe4b452c9e1139b221af29`。
- index.html SHA256：`4af2d12990a4d0fd5db0b9c4170429307d551db07ec988d520a11217d525d103`。
- 再次仅切换静态根目录并平滑加载 Nginx；认证、发布器和交易核心 PID 保持上述原值。
