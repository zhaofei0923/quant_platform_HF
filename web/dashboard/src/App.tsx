import { lazy, Suspense, useState } from "react";
import {
  BlockNote,
  Empty,
  HealthSummary,
  Icon,
  Logo,
  Metric,
  Panel,
  TradeTable,
} from "./components";
import { dayLabel, effectiveQuality, qualityLabel, time } from "./format";
import { useDashboard } from "./useDashboard";
import { useHistory } from "./history";
import PositionsPage from "./PositionsPage";
import TradingPage from "./TradingPage";
import StrategyPage from "./StrategyPage";

const EquityChart = lazy(() => import("./EquityChart"));
const navigation = [
  {
    id: "overview",
    name: "总览",
    title: "账户总览",
    subtitle: "当前账户的资金与交易动态",
  },
  {
    id: "positions",
    name: "持仓",
    title: "账户持仓",
    subtitle: "按合约查看持仓、实时价格与风控",
  },
  {
    id: "orders",
    name: "订单与成交",
    title: "交易记录",
    subtitle: "每一笔委托与成交，清晰可查",
  },
  {
    id: "strategy",
    name: "策略与运行",
    title: "策略与运行",
    subtitle: "关注交易链路和风控状态",
  },
];
export default function App() {
  const { snapshot, error, loading, now, endSession } = useDashboard();
  const [page, setPage] = useState("overview");
  const [range, setRange] = useState(1);
  const history = useHistory(
    range,
    snapshot?.data_scope_id ?? "",
    snapshot?.trading_day ?? "",
    endSession,
  );
  const active = navigation.find((item) => item.id === page)!;
  const account = snapshot?.account.data;
  const quality = effectiveQuality(snapshot?.account, now);
  const env = snapshot?.environment;
  const envLabel =
    env === "demo"
      ? "示例数据"
      : env === "simnow" || env === "simulation" || env === "sim"
        ? "模拟环境"
        : env === "prod" ||
            env === "live" ||
            env === "production" ||
            env === "real"
          ? "实盘环境"
          : "环境待确认";
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <Logo />
        <nav aria-label="主导航">
          {navigation.map((item) => (
            <button
              key={item.id}
              className={`nav-item ${page === item.id ? "selected" : ""}`}
              aria-current={page === item.id ? "page" : undefined}
              onClick={() => setPage(item.id)}
            >
              <Icon name={item.id} />
              <span>{item.name}</span>
            </button>
          ))}
        </nav>
        <div className="sidebar-bottom">
          <div className="account-name">
            <Icon name="user" />
            <div>
              <span>{snapshot?.account_alias || "个人账户"}</span>
              <small>只读访问</small>
            </div>
          </div>
          <button className="logout" onClick={() => endSession(true)}>
            <Icon name="logout" />
            <span>退出登录</span>
          </button>
        </div>
      </aside>
      <main data-page={page}>
        <header className="page-header">
          <div>
            <h1>{active.title}</h1>
            <p>{active.subtitle}</p>
          </div>
          <div className="header-status">
            {env === "demo" && (
              <strong className="demo-label">界面预览 · 示例数据</strong>
            )}
            <div className="header-meta">
              <span
                className={`environment ${env === "prod" || env === "live" || env === "production" ? "environment-live" : ""}`}
              >
                {envLabel}
              </span>
              <span className="header-day">
                交易日 <strong>{dayLabel(snapshot?.trading_day)}</strong>
              </span>
              <button
                className="mobile-logout icon-button"
                aria-label="退出登录"
                onClick={() => endSession(true)}
              >
                <Icon name="logout" size={18} />
              </button>
              <span className="last-update">
                更新时间 <strong>{time(snapshot?.generated_at_ms)}</strong>
              </span>
            </div>
          </div>
        </header>
        {(error || (!loading && quality !== "fresh")) && (
          <div className="notice" role="status">
            <Icon name="alert" size={18} />
            <div>
              <strong>{error || qualityLabel[quality]}</strong>
              <span>
                {snapshot
                  ? "当前保留最近一次有效记录，请关注各项更新时间。"
                  : "等待服务器发布经过核验的账户数据。"}
              </span>
            </div>
          </div>
        )}
        {page === "overview" && (
          <>
            <div className="metrics">
              <Metric
                label="账户权益"
                value={account?.balance}
                quality={quality}
              />
              <Metric
                label="可用资金"
                value={account?.available}
                quality={quality}
              />
              <Metric
                label="占用保证金"
                value={account?.curr_margin}
                quality={quality}
              />
              <Metric
                label="持仓盈亏"
                value={account?.position_profit}
                signed
                quality={quality}
              />
              <Metric
                label="平仓盈亏"
                value={account?.close_profit}
                signed
                quality={quality}
              />
              <Metric
                label="手续费"
                value={account?.commission}
                quality={quality}
              />
            </div>
            <div className="overview-grid">
              <Panel
                title="账户权益"
                subtitle="券商权益 · 按采样点排列，虚线表示无采样间隔"
                tools={
                  <div className="segments" aria-label="权益曲线范围">
                    {[
                      [1, "今日"],
                      [7, "7天"],
                      [30, "30天"],
                    ].map(([days, label]) => (
                      <button
                        key={days}
                        className={range === days ? "active" : ""}
                        aria-pressed={range === days}
                        onClick={() => setRange(Number(days))}
                      >
                        {label}
                      </button>
                    ))}
                  </div>
                }
                className="chart-panel"
              >
                {history.error && (
                  <div className="inline-notice">{history.error}</div>
                )}
                {history.points.length ? (
                  <Suspense fallback={<Empty>正在加载曲线</Empty>}>
                    <EquityChart points={history.points} range={range} />
                  </Suspense>
                ) : (
                  <div className="chart-placeholder">
                    <Empty detail="从上线后的第一笔有效数据开始记录，不补造历史。">
                      {history.loading
                        ? "正在读取权益记录"
                        : "权益曲线等待首笔采样"}
                    </Empty>
                  </div>
                )}
              </Panel>
              <Panel
                title="运行状态"
                tools={
                  <button
                    className="icon-button"
                    title="查看策略与运行"
                    aria-label="查看策略与运行"
                    onClick={() => setPage("strategy")}
                  >
                    <Icon name="arrow" />
                  </button>
                }
              >
                <HealthSummary
                  block={snapshot?.health}
                  now={now}
                  tradingDay={snapshot?.trading_day}
                />
                <div className="health-foot">
                  <BlockNote block={snapshot?.health} now={now} ttl={10_000} />
                </div>
              </Panel>
            </div>
            <Panel
              title="最新成交"
              tools={
                <button
                  className="text-button"
                  onClick={() => setPage("orders")}
                >
                  全部记录 <Icon name="arrow" size={16} />
                </button>
              }
            >
              <TradeTable
                compact
                rows={(snapshot?.trades.data ?? [])
                  .slice()
                  .sort((a, b) => b.as_of_ms - a.as_of_ms)
                  .slice(0, 5)}
              />
              <div className="table-footer">
                <BlockNote block={snapshot?.trades} now={now} />
                <span>交易日 {dayLabel(snapshot?.trading_day)}</span>
              </div>
            </Panel>
          </>
        )}
        {page === "positions" && (
          <PositionsPage snapshot={snapshot} now={now} />
        )}
        {page === "orders" && (
          <TradingPage
            key={snapshot?.data_scope_id}
            snapshot={snapshot}
            days={history.days}
            now={now}
            onExpired={endSession}
          />
        )}
        {page === "strategy" && <StrategyPage snapshot={snapshot} now={now} />}
        <footer className="page-footer">
          <span>数据来自券商回报，权益变化不等同于投资收益。</span>
          <span>北京时间 · 仅供账户查看</span>
        </footer>
      </main>
    </div>
  );
}
