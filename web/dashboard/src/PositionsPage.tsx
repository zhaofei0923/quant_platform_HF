import { Fragment, useMemo, useState } from "react";
import { BlockNote, Empty, Panel } from "./components";
import { cash, effectiveQuality, finite, pnlClass, qualityLabel, time } from "./format";
import {
  brokerDirection,
  buildContractPositionRows,
  directionalDistance,
  directionLabel,
  isExplicitClosedSession,
  marketQuality,
  riskQuality,
  type ContractPositionRow,
  type PriceDistance,
  type RiskMonitorRow,
} from "./positionRisk";
import type { Quality, Snapshot, StrategyPosition } from "./types";

const positionNumberFormat = new Intl.NumberFormat("zh-CN", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});
const displayNumber = (value: unknown) =>
  finite(value) ? positionNumberFormat.format(value) : "—";
const displayPrice = (value: number | null | undefined) =>
  finite(value) && value > 0 ? displayNumber(value) : "—";

const stopKindLabel = (value: StrategyPosition["stop_kind"]) =>
  value === "trailing" ? "跟踪止损" : value === "initial" ? "初始止损" : "类型待确认";

const qualityPriority: Record<Quality, number> = {
  fresh: 0, historical: 1, stale: 2, catching_up: 3, missing: 4, incomplete: 5, invalid: 6,
};

function aggregateQuality(values: Quality[]): Quality {
  return values.reduce<Quality>(
    (current, value) => qualityPriority[value] > qualityPriority[current] ? value : current,
    values.length ? "fresh" : "missing",
  );
}

function SourceStamp({ name, quality, asOf, closed = false }: {
  name: string;
  quality: Quality;
  asOf: number | null | undefined;
  closed?: boolean;
}) {
  return (
    <span className="source-stamp">
      <span>{name}</span>
      <span className={`quality quality-${quality}`}><i />{closed ? "休市最后值" : qualityLabel[quality]}</span>
      <time>{time(asOf)}</time>
    </span>
  );
}

function ReconciliationTag({ row }: { row: RiskMonitorRow }) {
  const label = row.reconciliation === "unbound" ? "未绑定策略"
    : row.reconciliation === "mismatch" ? "待核对"
    : row.multipleStrategies ? "多策略合计一致" : "已核对";
  return <span className={`reconciliation reconciliation-${row.reconciliation}`}>{label}</span>;
}

function distanceText(value: PriceDistance | null) {
  if (!value) return "距离 —";
  const percent = displayNumber(Math.abs(value.percent));
  return `${value.crossed ? "已越过" : "距离"} ${displayNumber(Math.abs(value.points))} · ${percent}%`;
}

function PositionQuantity({ row }: { row: ContractPositionRow }) {
  const known = row.brokerLong !== null && row.brokerShort !== null && row.brokerUnknown !== null;
  const amounts = known ? [
    row.brokerLong! > 0 ? `多 ${displayNumber(row.brokerLong)}` : "",
    row.brokerShort! > 0 ? `空 ${displayNumber(row.brokerShort)}` : "",
    row.brokerUnknown! > 0 ? `方向未知 ${displayNumber(row.brokerUnknown)}` : "",
  ].filter(Boolean) : [];
  const mismatch = row.riskRows.some((risk) => risk.reconciliation === "mismatch") ||
    row.strategies.some((strategy) => !finite(strategy.net));
  const unbound = row.riskRows.some((risk) => risk.reconciliation === "unbound");
  return (
    <div className="contract-quantity">
      <strong>{known ? amounts.join("／") || "0.00 手" : "待确认"}</strong>
      {(mismatch || unbound) && <span className="contract-reconciliation">
        {mismatch && <span className="reconciliation reconciliation-mismatch">待核对</span>}
        {unbound && <span className="reconciliation reconciliation-unbound">未绑定策略</span>}
      </span>}
    </div>
  );
}

function TargetPrices({ row, target }: { row: ContractPositionRow; target: "stop" | "take_profit" }) {
  if (!row.riskRows.length) return <span className="contract-target-value risk-target-value">—</span>;
  return (
    <div className="contract-target-list">
      {row.riskRows.map((risk) => {
        const value = target === "stop" ? risk.strategy?.effective_stop : risk.strategy?.take_profit;
        const distance = directionalDistance(row.quote?.last_price, value, risk.direction, target);
        return (
          <div className="contract-target" data-risk-binding={risk.strategy ? "strategy" : "unbound"} key={risk.id}>
            <span className="contract-target-label">
              {risk.strategy?.strategy_id || "未绑定策略"} · {directionLabel(risk.direction)}
              {!risk.strategy && ` ${displayNumber(risk.brokerQuantity)} 手`}
            </span>
            <strong className="contract-target-value risk-target-value">{displayPrice(value)}</strong>
            <span className={`contract-distance ${distance?.crossed ? "distance-crossed" : ""}`}>
              {distanceText(distance)}
            </span>
          </div>
        );
      })}
    </div>
  );
}

function ContractSources({ row, snapshot, now, closed }: {
  row: ContractPositionRow;
  snapshot: Snapshot | null;
  now: number;
  closed: boolean;
}) {
  const quoteState = marketQuality(row.quote, snapshot?.markets, now, closed);
  const riskState = row.strategies.some((strategy) => !finite(strategy.net)) ? "invalid" : aggregateQuality(row.riskRows.map((risk) =>
    riskQuality(risk.strategy, snapshot?.strategy_positions, now, closed),
  ));
  const times = row.riskRows.map((risk) => risk.strategy?.as_of_ms);
  const riskAsOf = times.length && times.every((value) => finite(value) && value > 0)
    ? Math.min(...times as number[]) : null;
  return (
    <div className="contract-sources row-sources">
      <SourceStamp name="持仓" quality={row.brokerQuality} asOf={snapshot?.positions.as_of_ms} />
      <SourceStamp name="行情" quality={quoteState} asOf={row.quote?.as_of_ms}
        closed={closed && quoteState === "historical"} />
      <SourceStamp name={times.length > 1 ? "风控最早" : "风控"} quality={riskState} asOf={riskAsOf}
        closed={closed && riskState === "historical"} />
    </div>
  );
}

function ContractDetails({ row, snapshot, now, closed }: {
  row: ContractPositionRow;
  snapshot: Snapshot | null;
  now: number;
  closed: boolean;
}) {
  return (
    <div className="contract-details" data-contract-details={row.instrumentId}>
      <div className="contract-detail-section">
        <h3>券商记录</h3>
        <div className="contract-detail-records">
          {row.brokerPositions.length ? row.brokerPositions.map((position, index) => (
            <div className="contract-detail-record" key={`${position.posi_direction}-${position.hedge_flag}-${position.position_date}-${index}`}>
              <h4>{directionLabel(brokerDirection(position.posi_direction))} · {
                ({ "1": "投机", "2": "套利", "3": "套保" } as Record<string, string>)[position.hedge_flag] || position.hedge_flag || "投保待确认"
              } · {({ "1": "今仓", "2": "昨仓" } as Record<string, string>)[position.position_date] || "合并"}</h4>
              <dl>
                <div><dt>总持仓</dt><dd>{displayNumber(position.position)} 手</dd></div>
                <div><dt>今仓／昨仓</dt><dd>{displayNumber(position.today_position)}／{displayNumber(position.yd_position)}</dd></div>
                <div><dt>多冻／空冻</dt><dd>{displayNumber(position.long_frozen)}／{displayNumber(position.short_frozen)}</dd></div>
                <div><dt>保证金</dt><dd>{cash(position.use_margin)}</dd></div>
                <div><dt>持仓盈亏</dt><dd className={pnlClass(position.position_profit)}>{cash(position.position_profit, true)}</dd></div>
                <div><dt>开仓成本</dt><dd>{cash(position.open_cost)}</dd></div>
                <div><dt>持仓成本</dt><dd>{cash(position.position_cost)}</dd></div>
                <div><dt>平仓盈亏</dt><dd className={pnlClass(position.close_profit)}>{cash(position.close_profit, true)}</dd></div>
              </dl>
              <SourceStamp name="持仓" quality={row.brokerQuality} asOf={snapshot?.positions.as_of_ms} />
            </div>
          )) : <p className="contract-detail-empty">{row.brokerQuality === "fresh" ? "券商完整查询确认该合约持仓为 0.00。" : "券商持仓待确认。"}</p>}
        </div>
      </div>
      <div className="contract-detail-section">
        <h3>策略与风控</h3>
        <div className="contract-detail-records">
          {row.riskRows.map((risk) => {
            const strategy = risk.strategy;
            const state = riskQuality(strategy, snapshot?.strategy_positions, now, closed);
            return (
              <div className="contract-detail-record" key={risk.id}>
                <h4>{strategy?.strategy_id || "未绑定策略"} · {directionLabel(risk.direction)}</h4>
                <ReconciliationTag row={risk} />
                <dl>
                  <div><dt>{strategy ? "策略持仓" : "未绑定数量"}</dt><dd>{displayNumber(strategy ? risk.strategyQuantity : risk.brokerQuantity)} 手</dd></div>
                  {strategy?.owner_strategy_id && <div><dt>持仓归属策略</dt><dd>{strategy.owner_strategy_id}</dd></div>}
                  <div><dt>开仓均价</dt><dd>{displayPrice(strategy?.avg_open)}</dd></div>
                  <div><dt>初始止损</dt><dd>{displayPrice(strategy?.initial_stop)}</dd></div>
                  <div><dt>跟踪止损</dt><dd>{displayPrice(strategy?.trailing_stop)}</dd></div>
                  <div><dt>当前生效止损</dt><dd>{displayPrice(strategy?.effective_stop)}</dd></div>
                  <div><dt>止损类型</dt><dd>{strategy ? stopKindLabel(strategy.stop_kind) : "—"}</dd></div>
                  <div><dt>止盈价</dt><dd>{displayPrice(strategy?.take_profit)}</dd></div>
                </dl>
                <SourceStamp name="风控" quality={state} asOf={strategy?.as_of_ms} closed={closed && state === "historical"} />
              </div>
            );
          })}
          {row.strategies.filter((strategy) => !row.riskRows.some((risk) => risk.strategy === strategy)).map((strategy, index) => (
            <div className="contract-detail-record" key={`unconfirmed-${strategy.strategy_id}-${index}`}>
              <h4>{strategy.strategy_id} · 方向待确认</h4>
              <span className="reconciliation reconciliation-mismatch">待核对</span>
              <dl>
                <div><dt>策略持仓</dt><dd>待确认</dd></div>
                {strategy.owner_strategy_id && <div><dt>持仓归属策略</dt><dd>{strategy.owner_strategy_id}</dd></div>}
                <div><dt>开仓均价</dt><dd>{displayPrice(strategy.avg_open)}</dd></div>
                <div><dt>初始止损</dt><dd>{displayPrice(strategy.initial_stop)}</dd></div>
                <div><dt>跟踪止损</dt><dd>{displayPrice(strategy.trailing_stop)}</dd></div>
                <div><dt>当前生效止损</dt><dd>{displayPrice(strategy.effective_stop)}</dd></div>
                <div><dt>止损类型</dt><dd>{stopKindLabel(strategy.stop_kind)}</dd></div>
                <div><dt>止盈价</dt><dd>{displayPrice(strategy.take_profit)}</dd></div>
              </dl>
              <SourceStamp name="风控" quality="invalid" asOf={strategy.as_of_ms} />
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function ContractList({ snapshot, now }: { snapshot: Snapshot | null; now: number }) {
  const rows = useMemo(() => buildContractPositionRows(snapshot, now), [snapshot, now]);
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set());
  const closed = isExplicitClosedSession(snapshot?.health, now);
  const positionsQuality = effectiveQuality(snapshot?.positions, now);
  const confirmedEmpty = positionsQuality === "fresh" &&
    effectiveQuality(snapshot?.strategy_positions, now, 5_000) === "fresh";
  const toggle = (id: string) => setExpanded((prior) => {
    const next = new Set(prior);
    if (next.has(id)) next.delete(id); else next.add(id);
    return next;
  });
  return (
    <Panel title="持仓列表" subtitle={`${rows.length} 个持仓合约 · 每 2 秒更新`}
      tools={<BlockNote block={snapshot?.positions} now={now} />} className="contract-position-panel">
      {rows.length ? <>
        <div className="contract-position-table">
          <table>
            <colgroup>{[8, 11, 11, 15, 15, 10, 10, 14, 6].map((width, index) => <col key={index} style={{ width: `${width}%` }} />)}</colgroup>
            <thead><tr>
              <th>合约</th><th>方向／持仓</th><th className="numeric">最新价</th>
              <th className="numeric">当前止损</th><th className="numeric">止盈价</th>
              <th className="numeric">持仓盈亏</th><th className="numeric">保证金</th>
              <th>数据状态</th><th>详情</th>
            </tr></thead>
            <tbody>{rows.map((row) => {
              const open = expanded.has(row.id);
              const detailId = `contract-desktop-${encodeURIComponent(row.id)}`;
              return <Fragment key={row.id}>
                <tr data-contract-row={row.instrumentId}>
                  <td><strong className="contract-symbol">{row.instrumentId}</strong><small>{row.exchangeId}</small></td>
                  <td><PositionQuantity row={row} /></td>
                  <td className="numeric"><strong className="contract-last-price risk-main-price">{displayPrice(row.quote?.last_price)}</strong></td>
                  <td className="numeric"><TargetPrices row={row} target="stop" /></td>
                  <td className="numeric"><TargetPrices row={row} target="take_profit" /></td>
                  <td className={`numeric ${pnlClass(row.brokerPositionProfit)}`}>{cash(row.brokerPositionProfit, true)}</td>
                  <td className="numeric">{cash(row.brokerMargin)}</td>
                  <td><ContractSources row={row} snapshot={snapshot} now={now} closed={closed} /></td>
                  <td><button className="contract-detail-toggle" aria-label={`${row.instrumentId} 详情`} aria-expanded={open}
                    aria-controls={open ? detailId : undefined} onClick={() => toggle(row.id)}>{open ? "收起" : "详情"}<span aria-hidden="true">{open ? "−" : "+"}</span></button></td>
                </tr>
                {open && <tr className="contract-detail-row"><td colSpan={9} id={detailId}>
                  <ContractDetails row={row} snapshot={snapshot} now={now} closed={closed} />
                </td></tr>}
              </Fragment>;
            })}</tbody>
          </table>
        </div>
        <div className="contract-position-cards">{rows.map((row) => {
          const open = expanded.has(row.id);
          const detailId = `contract-mobile-${encodeURIComponent(row.id)}`;
          return <article className="contract-position-card" key={row.id} data-contract={row.instrumentId}>
            <div className="contract-card-head">
              <div><strong className="contract-symbol">{row.instrumentId}</strong><small>{row.exchangeId}</small><PositionQuantity row={row} /></div>
              <div className="contract-card-last"><span>最新价</span><strong className="contract-last-price risk-main-price">{displayPrice(row.quote?.last_price)}</strong></div>
            </div>
            <div className="contract-card-targets">
              <div><h3>当前止损</h3><TargetPrices row={row} target="stop" /></div>
              <div><h3>止盈价</h3><TargetPrices row={row} target="take_profit" /></div>
            </div>
            <dl className="contract-card-money">
              <div><dt>持仓盈亏</dt><dd className={pnlClass(row.brokerPositionProfit)}>{cash(row.brokerPositionProfit, true)}</dd></div>
              <div><dt>保证金</dt><dd>{cash(row.brokerMargin)}</dd></div>
            </dl>
            <ContractSources row={row} snapshot={snapshot} now={now} closed={closed} />
            <button className="contract-detail-toggle" aria-label={`${row.instrumentId} 详情`} aria-expanded={open}
              aria-controls={open ? detailId : undefined} onClick={() => toggle(row.id)}>{open ? "收起详情" : "查看详情"}<span aria-hidden="true">{open ? "−" : "+"}</span></button>
            {open && <div id={detailId}><ContractDetails row={row} snapshot={snapshot} now={now} closed={closed} /></div>}
          </article>;
        })}</div>
      </> : <Empty detail={confirmedEmpty
        ? "券商完整持仓查询已返回，当前没有持仓合约。"
        : positionsQuality === "fresh" ? "券商当前为空仓，等待有效策略持仓快照确认。"
        : "等待有效的全账户持仓查询；当前无法确认是否空仓。"}>
        {confirmedEmpty ? "当前空仓" : "持仓尚未确认"}
      </Empty>}
    </Panel>
  );
}

export default function PositionsPage({ snapshot, now }: { snapshot: Snapshot | null; now: number }) {
  return <ContractList key={JSON.stringify([snapshot?.data_scope_id, snapshot?.instance_id])} snapshot={snapshot} now={now} />;
}
