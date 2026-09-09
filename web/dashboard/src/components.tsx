import type { ReactNode } from "react";
import {
  cash,
  dayLabel,
  effectiveQuality,
  offsetLabel,
  pnlClass,
  qualityLabel,
  quantity,
  sideLabel,
  statusLabel,
  time,
  price,
} from "./format";
import type { Block, Health, Quality, SourceStatus, Trade } from "./types";

export function Icon({ name, size = 20 }: { name: string; size?: number }) {
  const paths: Record<string, ReactNode> = {
    overview: (
      <>
        <rect x="3" y="3" width="7" height="7" rx="1" />
        <rect x="14" y="3" width="7" height="7" rx="1" />
        <rect x="3" y="14" width="7" height="7" rx="1" />
        <rect x="14" y="14" width="7" height="7" rx="1" />
      </>
    ),
    positions: (
      <>
        <path d="M3 7h18v13H3zM3 7V4h7l2 3" />
      </>
    ),
    orders: (
      <>
        <path d="M6 3h8l4 4v14H6zM14 3v5h4M9 12h6M9 16h6" />
      </>
    ),
    strategy: (
      <>
        <path d="M3 3v18h18M6 15l4-5 4 3 6-8M16 5h4v4" />
      </>
    ),
    user: (
      <>
        <circle cx="12" cy="7" r="4" />
        <path d="M4 22v-3a8 8 0 0116 0v3" />
      </>
    ),
    logout: (
      <>
        <path d="M9 4H4v16h5M13 7l5 5-5 5M8 12h13" />
      </>
    ),
    search: (
      <>
        <circle cx="10" cy="10" r="6" />
        <path d="m15 15 6 6" />
      </>
    ),
    arrow: <path d="M5 12h14m-5-5 5 5-5 5" />,
    alert: (
      <>
        <path d="m12 3 10 18H2zM12 9v5" />
        <path d="M12 17v1" />
      </>
    ),
  };
  return (
    <svg
      aria-hidden="true"
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      {paths[name] ?? paths.overview}
    </svg>
  );
}
export function Logo() {
  return (
    <div className="brand">
      <svg width="43" height="46" viewBox="0 0 43 46" aria-hidden="true">
        <path
          d="M21.5 3 39 13v20l-8 4.5-6.5-5L32 28V17L21.5 11 11 17v12l10.5 6 4 2.5-4 5L4 33V13Z"
          fill="currentColor"
        />
      </svg>
      <div>
        <strong>QUANT</strong>
        <span>量化交易看板</span>
      </div>
    </div>
  );
}
export function QualityTag({ value }: { value: Quality }) {
  return (
    <span className={`quality quality-${value}`}>
      <i />
      {qualityLabel[value]}
    </span>
  );
}
export function BlockNote({
  block,
  now,
  ttl,
}: {
  block?: Block<unknown>;
  now: number;
  ttl?: number;
}) {
  const quality = effectiveQuality(block, now, ttl);
  return (
    <span className="block-note">
      <QualityTag value={quality} />
      <span>更新于 {time(block?.as_of_ms)}</span>
    </span>
  );
}
export function Empty({
  children,
  detail,
}: {
  children: ReactNode;
  detail?: string;
}) {
  return (
    <div className="empty">
      <span className="empty-icon">
        <Icon name="overview" size={25} />
      </span>
      <strong>{children}</strong>
      {detail && <p>{detail}</p>}
    </div>
  );
}
export function Panel({
  title,
  subtitle,
  tools,
  children,
  className = "",
}: {
  title: string;
  subtitle?: ReactNode;
  tools?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <section className={`panel ${className}`}>
      <div className="panel-head">
        <div>
          <h2>{title}</h2>
          {subtitle && <div className="panel-subtitle">{subtitle}</div>}
        </div>
        {tools}
      </div>
      {children}
    </section>
  );
}
export function Metric({
  label,
  value,
  signed = false,
  quality,
}: {
  label: string;
  value: unknown;
  signed?: boolean;
  quality: Quality;
}) {
  return (
    <article className={`metric ${quality !== "fresh" ? "metric-old" : ""}`}>
      <span>{label}</span>
      <strong className={signed ? pnlClass(value) : ""}>
        {cash(value, signed)}
      </strong>
      <small>
        {quality === "fresh" ? "券商返回口径" : qualityLabel[quality]}
      </small>
    </article>
  );
}
export function sourceBlock(
  source: SourceStatus | undefined,
  parent: Block<Health> | undefined,
): Block<unknown> | undefined {
  if (!parent) return undefined;
  return {
    ...parent,
    quality: source?.quality ?? parent.quality,
    as_of_ms: source?.as_of_ms ?? parent.as_of_ms,
  };
}
function subQuality(
  source: SourceStatus | undefined,
  parent: Block<Health> | undefined,
  now: number,
) {
  return effectiveQuality(
    source?.quality && source.as_of_ms
      ? { quality: source.quality, as_of_ms: source.as_of_ms }
      : parent,
    now,
    10_000,
  );
}
export function HealthSummary({
  block,
  now,
  tradingDay,
}: {
  block?: Block<Health>;
  now: number;
  tradingDay?: string;
}) {
  const readiness = block?.data?.readiness;
  const pipeline = block?.data?.pipeline;
  const rq = subQuality(readiness, block, now),
    pq = subQuality(pipeline, block, now);
  const ready = rq === "fresh";
  const pipelineFresh = pq === "fresh";
  const inactive = pipeline?.overall_status === "inactive";
  const statuses = [
    {
      label: "交易连接",
      value: ready
        ? readiness?.trader_ready === true &&
          readiness?.gateway_healthy === true
          ? "正常"
          : "未就绪"
        : qualityLabel[rq],
      ok:
        ready &&
        readiness?.trader_ready === true &&
        readiness?.gateway_healthy === true,
    },
    {
      label: "行情链路",
      value: pipelineFresh
        ? inactive
          ? "休市 / 非交易时段"
          : pipeline?.overall_status === "healthy"
            ? "正常"
            : "需关注"
        : qualityLabel[pq],
      ok: pipelineFresh && (inactive || pipeline?.overall_status === "healthy"),
    },
    {
      label: "交易许可",
      value: ready
        ? readiness?.mode === "Ready" ||
          readiness?.mode === "open_allowed" ||
          readiness?.mode === "ready"
          ? "允许交易"
          : readiness?.mode === "CloseOnly" || readiness?.mode === "close_only"
            ? "只允许平仓"
            : "等待就绪"
        : qualityLabel[rq],
      ok:
        ready &&
        ["Ready", "open_allowed", "ready"].includes(readiness?.mode ?? ""),
    },
    { label: "当前交易日", value: dayLabel(tradingDay), ok: null },
  ];
  return (
    <div className="health-list">
      {statuses.map((item) => (
        <div className="health-row" key={item.label}>
          <span>
            <i
              className={`dot ${item.ok === null ? "neutral" : item.ok ? "good" : "warn"}`}
            />
            {item.label}
          </span>
          <strong className={item.ok === false ? "warning-text" : ""}>
            {item.value}
          </strong>
        </div>
      ))}
    </div>
  );
}
export function TradeTable({
  rows,
  empty = "暂无成交记录",
  compact = false,
}: {
  rows: Trade[];
  empty?: string;
  compact?: boolean;
}) {
  if (!rows.length)
    return <Empty detail="仅显示实际记录，未产生数据时不补零。">{empty}</Empty>;
  return (
    <div className={compact ? "compact-trades" : "trades"}>
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th>合约</th>
              <th>方向</th>
              <th>开平</th>
              <th className="numeric">成交价</th>
              <th className="numeric">数量</th>
              <th>时间 · 北京</th>
              {!compact && <th>策略</th>}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, index) => (
              <tr key={`${row.trade_id}-${index}`}>
                <td>
                  <strong>{row.instrument_id}</strong>
                  {!compact && <small>{row.exchange_id}</small>}
                </td>
                <td
                  className={
                    row.side === "buy"
                      ? "positive"
                      : row.side === "sell"
                        ? "negative"
                        : ""
                  }
                >
                  {sideLabel(row.side)}
                </td>
                <td>{offsetLabel(row.offset)}</td>
                <td className="numeric">{price(row.price)}</td>
                <td className="numeric">{quantity(row.volume)} 手</td>
                <td className="tabular">{time(row.as_of_ms)}</td>
                {!compact && (
                  <td>
                    {row.attribution === "strategy" && row.strategy_id ? (
                      row.strategy_id
                    ) : (
                      <span className="muted">未归因</span>
                    )}
                  </td>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {compact && (
        <div className="trade-cards">
          {rows.map((row, index) => (
            <div className="trade-card" key={`${row.trade_id}-${index}`}>
              <div>
                <strong>{row.instrument_id}</strong>
                <span className={row.side === "buy" ? "positive" : "negative"}>
                  {sideLabel(row.side)} · {offsetLabel(row.offset)}
                </span>
              </div>
              <div>
                <span>
                  {price(row.price)} × {quantity(row.volume)} 手
                </span>
                <small>{time(row.as_of_ms)}</small>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
export function OrderStatus({ value }: { value: string }) {
  return (
    <span
      className={`order-status ${value === "rejected" ? "order-rejected" : value === "filled" ? "order-filled" : value === "canceled" ? "order-canceled" : ""}`}
    >
      {statusLabel(value)}
    </span>
  );
}
