import {
  BlockNote,
  Empty,
  HealthSummary,
  Panel,
  sourceBlock,
} from "./components";
import { effectiveQuality, qualityLabel, quantity } from "./format";
import type { Snapshot } from "./types";

const label: Record<string, string> = {
  healthy: "正常",
  inactive: "非交易时段",
  degraded: "需关注",
  unhealthy: "异常",
  blocked: "已拦截",
  waiting: "等待",
  missing: "等待数据",
  stale: "已过期",
  ready: "已就绪",
  market: "行情",
  ticks: "行情接收",
  tick: "行情接收",
  bars: "周期数据",
  bar: "周期数据",
  strategy: "策略评估",
  execution: "委托执行",
  ctp: "交易回报",
  fills: "成交回报",
  settlement: "结算确认",
  recovery: "恢复对账",
  warmup: "策略预热",
};
export default function StrategyPage({
  snapshot,
  now,
}: {
  snapshot: Snapshot | null;
  now: number;
}) {
  const pipeline = snapshot?.health.data?.pipeline;
  const readiness = snapshot?.health.data?.readiness;
  const readinessBlock = sourceBlock(readiness, snapshot?.health);
  const pipelineBlock = sourceBlock(pipeline, snapshot?.health);
  const rq = effectiveQuality(readinessBlock, now, 10_000);
  return (
    <div className="page-stack">
      <div className="strategy-grid">
        <Panel
          title="运行状态"
          tools={<BlockNote block={snapshot?.health} now={now} ttl={10_000} />}
        >
          <HealthSummary
            block={snapshot?.health}
            now={now}
            tradingDay={snapshot?.trading_day}
          />
        </Panel>
        <Panel
          title="交易许可与恢复"
          subtitle="连接成功后仍须完成对账、结算与预热"
          tools={<BlockNote block={readinessBlock} now={now} ttl={10_000} />}
        >
          <div className="health-list">
            <div className="health-row">
              <span>恢复对账</span>
              <strong>
                {rq !== "fresh"
                  ? qualityLabel[rq]
                  : readiness?.recovery_complete === true
                    ? "已完成"
                    : "等待确认"}
              </strong>
            </div>
            <div className="health-row">
              <span>结算确认</span>
              <strong>
                {rq !== "fresh"
                  ? qualityLabel[rq]
                  : readiness?.settlement_confirmed === true
                    ? "已完成"
                    : "等待确认"}
              </strong>
            </div>
            <div className="health-row">
              <span>待退出任务</span>
              <strong>{quantity(readiness?.pending_exit_count)}</strong>
            </div>
            <div className="health-row">
              <span>未解决映射</span>
              <strong>{quantity(readiness?.unresolved_mapping_count)}</strong>
            </div>
          </div>
        </Panel>
      </div>
      <Panel
        title="策略与行情链路"
        subtitle="仅展示当前订阅合约和已有监控结果"
        tools={<BlockNote block={pipelineBlock} now={now} ttl={10_000} />}
      >
        {pipeline?.products?.length ? (
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>品种 / 合约</th>
                  <th>状态</th>
                  <th className="numeric">采集时行情年龄</th>
                  <th className="numeric">策略评估</th>
                  <th className="numeric">候选信号</th>
                  <th className="numeric">允许信号</th>
                  <th className="numeric">跟踪中</th>
                </tr>
              </thead>
              <tbody>
                {pipeline.products.map((row, i) => (
                  <tr key={`${row.product_id}-${i}`}>
                    <td>
                      <strong>{row.instrument_id || row.product_id}</strong>
                      <small>{row.product_id}</small>
                    </td>
                    <td>{label[row.status] ?? row.status}</td>
                    <td className="numeric">
                      {quantity(row.tick_age_seconds)} 秒
                    </td>
                    <td className="numeric">
                      {quantity(row.strategy_evaluations)}
                    </td>
                    <td className="numeric">{quantity(row.candidates)}</td>
                    <td className="numeric">{quantity(row.allowed)}</td>
                    <td className="numeric">{quantity(row.pending_traces)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <Empty detail="运行监控尚未发布有效的合约链路状态。">
            等待策略与行情数据
          </Empty>
        )}
      </Panel>
      <Panel title="风控与异常" subtitle="状态与阻断原因独立于网页连接状态">
        <div className="incident-list">
          {readiness?.reasons?.map((reason, i) => (
            <div key={`${reason}-${i}`} className="incident">
              <i className="dot warn" />
              <span>{label[reason] ?? reason}</span>
              <small>交易许可</small>
            </div>
          ))}
          {pipeline?.stages
            ?.filter(
              (stage) =>
                !["healthy", "inactive", "ready"].includes(stage.status),
            )
            .map((stage, i) => (
              <div key={`${stage.name}-${i}`} className="incident">
                <i className="dot warn" />
                <span>
                  {label[stage.name] ?? stage.name} ·{" "}
                  {label[stage.status] ?? stage.status}
                </span>
                <small>{stage.reason}</small>
              </div>
            ))}
          {!readiness?.reasons?.length &&
            !pipeline?.stages?.some(
              (stage) =>
                !["healthy", "inactive", "ready"].includes(stage.status),
            ) && (
              <Empty detail="数据陈旧或缺失时，仍需以上方状态提示为准。">
                暂无已记录的阻断原因
              </Empty>
            )}
        </div>
      </Panel>
    </div>
  );
}
