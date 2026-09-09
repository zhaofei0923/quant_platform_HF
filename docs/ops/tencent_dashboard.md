# 腾讯云个人只读 Dashboard 部署

本页面用于已有腾讯云交易主机上的独立网站。交易进程仍为 C++，网站只读取指定账户实例的观察数据。网站、认证服务故障不会触发下单、撤单或交易进程重启。安装脚本不会启动服务，也不会修改已有交易 unit。当前未提供域名、备案和远端环境事实，因此本地验证通过不等于腾讯云已经上线。

## 组成与访问边界

公网数据仅通过专用域名的 HTTPS 443 访问；共享模式的 HTTP 80 只把该域名重定向到 HTTPS。`/auth/` 是 Authelia 独立登录页；Nginx 使用内部授权子请求保护静态网页与 JSON。Authelia 仅监听 `127.0.0.1:9091`。禁止将交易程序、WAL、日志目录、认证服务端口或整个仓库设置为站点根目录。

| 组件 | 身份与权限 | 默认限制 |
| --- | --- | --- |
| `quant-dashboard-publisher` | 独立用户；选定 recovery root 的只读 ACL；只能写自己的游标和发布目录；无网络 | 单核 25% CPU、128 MiB 软限、256 MiB 硬限、低 IO 权重 |
| `quant-dashboard-auth` | 独立用户；单用户 Argon2id 文件、SQLite、内存会话；不访问交易目录 | 单核 25% CPU、192 MiB 软限、256 MiB 硬限 |
| 系统 `nginx.service`（共享模式） | 现有 Nginx worker 通过命名 ACL 只读前端和公共快照 | 复用现有 80/443，不新建 Web unit 或监听端口 |
| `quant-dashboard-web`（独占模式） | 独立用户；只读前端、公共快照及网站 TLS 文件 | 仅适用于 443 空闲的独立主机 |

这些是初始资源预算，不是当前腾讯云主机的实测值。服务器必须还有足够余量；上线前后比较原交易进程延迟、行情陈旧率、内存和磁盘 IO。构建在开发机进行，服务器不运行前端开发服务器。

JSON 仅允许以下路径，均要求登录且 `Cache-Control: no-store`：

```text
/data/v1/current.json
/data/v1/days.json
/data/v1/days/YYYYMMDD/equity.json
/data/v1/days/YYYYMMDD/orders.json
/data/v1/days/YYYYMMDD/trades.json
```

这些数据接口只接受 GET/HEAD。认证接口保留登录、注销所需的 POST。未登录数据请求得到 401；网页导航跳转登录页；认证服务故障拒绝返回数据。Nginx 禁止符号链接和非白名单数据文件访问。

## 上线前必须具备的输入

1. 实际腾讯云地域、系统版本/架构、资源余量、固定公网地址，以及现有 80/443/9091 端口使用情况。
2. 用户控制的专用域名，例如 `quant.example.com`；完成域名解析以及腾讯云大陆地域适用的备案、接入要求。不得把换 IP 或换端口当作豁免。
3. 有效且与域名匹配的可信 TLS 证书。网站没有 HTTP 明文或自签生产回退。
4. 当前交易实例的私有身份描述文件和只读运行目录。必须对应当前 instance，不得复用旧账户、旧目录或旧初始资金基线。
5. 本机私下准备的 Authelia 用户文件；密码、哈希、密钥、`.env` 不放入 Git、包、聊天或诊断输出。

当前实施使用 Authelia **v4.39.22** 进行了本地协议测试。下载官方对应架构发行包后核对其 GitHub release 资产 SHA256；不要把示例密码或上游默认配置直接部署。Linux amd64 发行包在本次验证中的 SHA256 为 `15ea0538c3a795f8698fdee734e4f775446151d335e6daeb2f8e92063f865ddd`，其他版本和架构必须使用各自的校验值。[官方发行](https://github.com/authelia/authelia/releases/tag/v4.39.22)

## 开发机生成发布包

前端输出位于 `web/dashboard/dist`，发布器为 `dashboard_publish_cli`。打包脚本收集发布器、Authelia、前端构建输出、部署模板、脚本和两份运维文档，并生成 `SHA256SUMS`，不会打包 `.env`、runtime、身份文件、证书或用户文件。前端构建中不能混入演示 JSON、source maps 或私有文件。

本包不包含 `core_engine`。核心观察快照与 WAL 只读权限的新代码必须按既有交易发布流程，在安全维护窗口独立更新并验证；网站安装不能代替交易引擎升级或擅自重启正在运行的实例。

升级与回滚必须遵守以下兼容矩阵：

| 交易核心私有快照 | 发布器 | 结果与允许操作 |
| --- | --- | --- |
| schema v1 | 当前兼容 v1/v2 版本 | 账户、持仓和历史可用；行情与策略风控显示缺失 |
| schema v2 | 当前兼容 v1/v2 版本 | 完整支持；这是启用 v2 核心时的最低发布器版本 |
| schema v2 | 更早的仅 v1 发布器 | 不兼容，禁止单独回滚到该组合 |

前端可独立回滚。核心从 v2 回滚到 v1 后，当前发布器仍可工作，但实时行情与策略风控会显示缺失。发布器若要回滚到更早的仅 v1 版本，必须先回滚核心，或把核心与发布器作为经过验证的配对版本在同一维护窗口回滚。

```bash
bash scripts/ops/package_dashboard.sh \
  --build-dir "$PWD/build-real-server" \
  --web-dir "$PWD/web/dashboard/dist" \
  --authelia-bin /absolute/path/to/verified/authelia \
  --output /absolute/path/to/quant-dashboard-release.tar.gz
```

通过现有可信传输渠道把包传到服务器的私有 staging 目录。只解压自己生成并核验的包，不直接在已有安装目录覆盖解压。包内 publisher 仍可能依赖编译主机链接的共享库，预检会检查 `ldd`；缺库时重建兼容产物，不盲目替换交易 SDK。

## 私有身份与观察源

在既有受控交易启动环境中使用现有 `runtime_paths_cli` 获取当前路径，并核对当前运行实例。网站安装脚本不会加载 `.env`、推断凭证或调用 CTP。身份描述文件由用户在本机私有目录中填写，样式如下（示例值不代表真实账户）：

```json
{
  "schema_version": 1,
  "identity": {
    "environment": "simnow",
    "broker_id": "BROKER",
    "account_id": "ACCOUNT",
    "instance_id": "INSTANCE"
  },
  "account_alias": "个人账户",
  "instance_alias": "腾讯云主实例",
  "recovery_root": "/home/USER/workspace/quant_platform_HF/runtime/simnow/BROKER/ACCOUNT/INSTANCE"
}
```

发布器支持 descriptor 的 `paths` 覆盖，但本部署首版要求所有覆盖仍在选定 recovery root 内，且不含符号链接。文件名默认是 `monitor/dashboard_private.json`、`monitor/readiness.json`、`monitor/pipeline_health.json` 和 `wal/events.wal`。持仓最新价和策略风控只读取 `dashboard_private.json` schema v2 的回调缓存，不扫描行情 CSV 或策略持久化目录。旧的 schema v1 仍可发布账户、持仓和历史，但实时行情与策略风控明确显示缺失。旧实例的指标或缺失快照应显示未知/陈旧，不得用零补齐资金或把进程存活等同于交易就绪。

发布器每 1 秒原子更新公共快照，浏览器每 2 秒读取一次。行情与策略风控各自使用源时间，超过 5 秒标记陈旧；公共文件更新时间不能替代源时间。交易核心仍须在休市维护窗口升级到私有 schema v2，网站和发布器升级不应触发核心重启。

核心私有快照默认关闭。代码就绪并安排受控维护后，在原交易服务的既有环境管理流程中设置：

```text
QUANT_HFT_DASHBOARD_SNAPSHOT_ENABLE=1
QUANT_HFT_DASHBOARD_SNAPSHOT_FILE=/absolute/selected/recovery_root/monitor/dashboard_private.json
QUANT_HFT_DASHBOARD_WAL_READ_GROUP=quant-dashboard-read
```

本安装脚本不会替用户加入这些变量或重启核心。修改输出路径时必须同步 descriptor，不能让发布器读取其他账户目录。私有快照包含真实账户身份，不能公开。核心快照目录应保持 0750，文件 0640，临时文件的原子替换也必须保留只读 ACL 掩码。

安装时只给 `quant-dashboard-read` 组授予选定实例下文件的只读权限，以及父目录的穿越权限；给该实例内目录设置默认只读 ACL。网页用户不加入此组。ACL 修改前的证据写入管理员私有备份目录。需要外部覆盖目录或现有复杂 ACL 时，先完成该目录权限方案，不得递归 chmod 整个 runtime。

WAL 的默认新建模式仍是 0600，单靠默认 ACL 无法让新 WAL 可读。安装脚本因此将 descriptor 指定的 WAL 父目录设为 `quant-dashboard-read` 组且 setgid、组只读/穿越，并给已有 WAL 同组只读 ACL。只有显式开启快照且设置第三项 `QUANT_HFT_DASHBOARD_WAL_READ_GROUP` 后，核心才对已打开的普通 WAL fd 核验实际组匹配并改为 0640；核验失败只记录观察功能警告，不放松或阻断交易许可。未开启时保持原 0600 行为。这样首次创建/重建也使用确定的只读组，不需要额外 root 权限刷新守护进程。运行时权限不满足会显示源缺失，不能报告成交覆盖完整。

## 准备认证与 HTTPS

认证使用 Authelia 自带门户。单用户、Argon2id、8 小时 Cookie、15 分钟无请求失效，禁用“记住我”。持续轮询属于活动请求，不等于用户鼠标操作。此版本会在请求时滑动延长服务端 TTL，不能单凭 `expiration: 8h` 宣称绝对有效期；内存后端过期清理另有约一分钟 GC 周期。[会话配置](https://www.authelia.com/configuration/session/introduction/)、[实际授权代码](https://github.com/authelia/authelia/blob/v4.39.22/internal/handlers/handler_authz_authn.go)、[内存后端](https://github.com/authelia/authelia/blob/v4.39.22/internal/session/memory/provider.go)

因此独立认证 unit 使用 `RuntimeMaxSec=7h59min55s`、`TimeoutStopSec=5s`、`Restart=always`。认证进程固定周期重启并清空全部内存会话，使持续使用或重放的旧 Cookie 也无法无限延长；从一次服务启动起，最迟 8 小时内清空。用户可能提前被要求重新登录，重启期间数秒请求拒绝访问，交易进程不受此操作影响。此限制依赖安装的 systemd unit，不能换成无限期前台运行；不使用 Redis，也不宣称高可用。授权入口只接受 Cookie 会话，禁止用 HTTP Basic 绕过网页登录限速。[systemd 官方服务生命周期](https://github.com/systemd/systemd/blob/v249/man/systemd.service.xml)、[授权策略配置](https://www.authelia.com/configuration/miscellaneous/server-endpoints-authz/)

在私有终端执行 `authelia crypto hash generate argon2`，通过交互式隐藏输入设置至少 8 位密码。不要用命令行 `--password` 暴露密码。把生成的哈希写入权限 0600 的私有用户 YAML，只有一个与安装参数 `--username` 一致的用户。安装仅开放该用户，不提供公开注册、找回密码或在线修改密码。配置样式参见[官方用户文件](https://www.authelia.com/reference/guides/passwords/#user--password-file)。

Nginx 登录接口每 IP 每分钟 6 次、短突发 3 次，同时设站点总计每分钟 12 次、最多 2 个并发密码校验以约束哈希内存；Authelia 对 2 分钟内 5 次失败封禁 IP 15 分钟。不要信任公网传入的伪造 `X-Forwarded-For`，模板在唯一入口用实际对端地址覆盖它。[官方限速](https://www.authelia.com/configuration/security/regulation/)

域名 HTTPS 可采用腾讯云托管证书或 ACME。使用 Certbot standalone 时，首先确认 80 端口空闲且安全组允许 ACME HTTP 校验；域名已解析到本机后申请：

```bash
sudo certbot certonly --standalone --cert-name quant.example.com -d quant.example.com
```

此方式仅在签发/续期时临时占用 80，Dashboard 仍只监听 443。若 80 已有服务，使用现有 webroot 或 DNS 验证流程，不停止其他业务来迁就模板。ACME 域名 DNS 凭证若需要，单独按其最小权限配置，不写入本项目。

## 预检与安装（默认不启动）

主机需要 Nginx（包含 `http_auth_request_module`）、`jq`、`acl`、OpenSSL、systemd 和对应二进制运行库。已有系统 Nginx 承载其他网站时使用 `shared-system`；预检会验证现有配置、`sites-enabled` 布局、worker 用户、域名无重复以及 9091 空闲，不要求 80/443 空闲，也不会停止其他网站。默认 `dedicated` 模式保留给 443 和 9091 都空闲的独立主机。

在解压包目录运行：

```bash
sudo bash scripts/ops/preflight_dashboard.sh \
  --domain quant.example.com \
  --source-dir /absolute/runtime/simnow/BROKER/ACCOUNT/INSTANCE \
  --identity-file /private/dashboard-identity.json \
  --tls-cert /etc/letsencrypt/live/quant.example.com/fullchain.pem \
  --tls-key /etc/letsencrypt/live/quant.example.com/privkey.pem

sudo bash scripts/ops/install_dashboard.sh \
  --domain quant.example.com \
  --source-dir /absolute/runtime/simnow/BROKER/ACCOUNT/INSTANCE \
  --identity-file /private/dashboard-identity.json \
  --tls-cert /etc/letsencrypt/live/quant.example.com/fullchain.pem \
  --tls-key /etc/letsencrypt/live/quant.example.com/privkey.pem \
  --users-file /private/dashboard-users.yml --username owner
```

当前腾讯云主机的共享 Nginx 示例（通配符证书谱系可以与站点域名不同）：

```bash
sudo bash scripts/ops/preflight_dashboard.sh \
  --nginx-mode shared-system --nginx-worker-user www-data \
  --domain quant.easudata.com \
  --source-dir /absolute/runtime/simnow/BROKER/ACCOUNT/INSTANCE \
  --identity-file /private/dashboard-identity.json \
  --tls-cert /etc/letsencrypt/live/easudata.com/fullchain.pem \
  --tls-key /etc/letsencrypt/live/easudata.com/privkey.pem

sudo bash scripts/ops/install_dashboard.sh \
  --nginx-mode shared-system --nginx-worker-user www-data \
  --domain quant.easudata.com \
  --source-dir /absolute/runtime/simnow/BROKER/ACCOUNT/INSTANCE \
  --identity-file /private/dashboard-identity.json \
  --tls-cert /etc/letsencrypt/live/easudata.com/fullchain.pem \
  --tls-key /etc/letsencrypt/live/easudata.com/privkey.pem \
  --users-file /private/dashboard-users.yml --username owner
```

共享安装只创建 `/etc/nginx/sites-available/quant-dashboard` 及指向它的同名 `sites-enabled` 链接。若路径已被非本安装占用或其他启用配置已声明相同域名，安装拒绝覆盖。新配置通过 `nginx -t` 后才平滑 reload `nginx.service`；失败会恢复原 Dashboard 站点。`bid`、`gxjn`、`navigator`、catchall 等其他站点文件不在脚本写入范围内。

安装在 `/opt/quant-dashboard/releases` 创建版本目录，私有认证配置放 `/etc/quant-dashboard/auth`，身份文件放 `/etc/quant-dashboard/identity.json`，公开 JSON 放 `/var/lib/quant-dashboard-public/data/v1`。源路径目前限制为不含空格的规范绝对路径。它不会启用服务、打开安全组或接触现有交易 unit。

完成域名、备案接入、证书信任、身份和权限确认后，共享模式只启动认证与发布器：

```bash
sudo systemctl enable --now quant-dashboard-auth quant-dashboard-publisher
```

独占模式才启动三个 Dashboard unit：

```bash
sudo systemctl enable --now quant-dashboard-auth quant-dashboard-publisher quant-dashboard-web
```

网站退出按钮使用 Authelia `/auth/logout` 门户流程销毁服务端会话，不只清除前端页面。注销成功和请求 401 时应停止轮询并清空页面敏感数据。

证书续期成功后的 deploy hook 为 `scripts/ops/renew_dashboard_certificate.sh`。主安装脚本不会自动改动 Certbot hooks；管理员审核安装结果后显式安装：

```bash
sudo install -m 0755 /opt/quant-dashboard/current/scripts/ops/renew_dashboard_certificate.sh \
  /etc/letsencrypt/renewal-hooks/deploy/quant-dashboard
```

安装会记录实际 Certbot lineage，因此 `quant.easudata.com` 可使用 `/etc/letsencrypt/live/easudata.com` 通配符证书。hook 只接受记录的 lineage，验证域名、有效期和密钥匹配后复制证书；共享模式执行系统 `nginx -t` 并 reload `nginx.service`，独占模式 reload 自己的 Web unit。先运行 `certbot renew --dry-run` 验证挑战；续期失败或证书剩余不足 14 天应进入现有主机告警流程。

## 验证、升级与回滚

本机协议测试不需要 root，不读取用户凭证，不接触云端，所有测试凭证/密钥仅在临时目录生成并清理。提供预先核验的可执行文件：

```bash
DASHBOARD_TEST_AUTHELIA_BIN=/absolute/verified/authelia \
DASHBOARD_TEST_NGINX_BIN=/absolute/nginx \
DASHBOARD_TEST_MIME_TYPES=/absolute/mime.types \
  bash tests/unit/build/dashboard_deployment_test.sh
```

测试实际通过：Authelia 配置加载、HTTPS、独立登录、Secure/HttpOnly 及 8 小时 Cookie、未认证 JSON 401、Basic 绕过拒绝、已认证读取、no-store、符号链接/路径穿越拒绝、写操作拒绝、注销后旧 Cookie 失效、登录限速、认证服务停止时拒绝数据。仅把临时配置的无活动期限改成 2 秒，显式重放旧 Cookie 也得到 401，验证了服务端失效，而非只靠浏览器删除 Cookie。

另有两个需 root 的隔离测试：`dashboard_permissions_test.sh` 使用临时数字 UID 验证真实内核 ACL、新 WAL 的默认 0600 与显式观察模式 0640、发布器只读及网页无私有目录权限；`dashboard_session_lifetime_test.sh` 创建随机名字的临时 systemd unit，持续重放 Cookie 跨越缩短为 8 秒的进程生命周期，实际换 PID 后旧 Cookie 得到 401，重启窗口拒绝数据，结束即删除临时状态并停止该 unit。所有测试仅使用现场生成的临时凭证。

实际组合为 Authelia v4.39.22 + Ubuntu Nginx 1.18.0；这些测试不替代手机浏览器、真实证书和完整生产 systemd 沙箱/资源限制在目标服务器上的验收。没有所需权限或可执行文件时测试退出 77（跳过），不能报告通过。

上线检查还应覆盖：实际账户/实例身份、资金来源及日期、断连与休市展示、快照停止刷新、重启认证后的重新登录、公开目录无原始日志/路径/账户真值、从外部访问仅 443，以及交易性能前后对比。Nginx 日志只记录普通访问字段，不记录 Cookie、请求体或 Authorization；独立 access log 按日/10 MiB 轮转、保留 14 份，认证 journal 保留按主机现有策略。

共享模式升级只停止认证与发布器，再运行新包预检和安装；系统 Nginx 与其他网站持续运行。独占模式仍停止三个 Dashboard unit。此升级脚本仅支持同 Nginx 模式、域名、worker 用户、登录用户名、账户及 recovery root；迁移另行审查。安装会输出管理员备份目录。共享回滚校验当前 Dashboard 站点未被后来修改，只恢复其旧站点与配置（首装则移除这一个站点），执行 `nginx -t` 后 reload 系统 Nginx，不读取或改写其他站点。

```bash
# shared-system 模式
sudo systemctl stop quant-dashboard-publisher quant-dashboard-auth

# dedicated 模式
sudo systemctl stop quant-dashboard-web quant-dashboard-publisher quant-dashboard-auth
```

```bash
sudo bash scripts/ops/rollback_dashboard.sh \
  --backup-dir /var/lib/quant-dashboard-admin/RELEASE
```

默认回滚还原网站代码、身份描述、认证配置、三个网站 unit 和日志轮转，保持服务停止待验证；保留最新密码、TLS 证书、认证数据库、发布器历史和交易状态。确需撤销源权限变更时，为同一命令追加 `--restore-source-permissions`。脚本将当前文件集合及 ACL 与安装后记录逐项比较；只有完全一致才恢复安装前的组、模式和 ACL，发现新文件或后续权限修改就拒绝覆盖，管理员须结合记录处理。它不会删除后来创建的目录。首装没有前版本时，回滚停止并禁用这三个网站服务，同样支持受检查的权限恢复。重新启用前验证前版本兼容当前认证数据库，不能盲目跨 Authelia 数据库格式回退。
