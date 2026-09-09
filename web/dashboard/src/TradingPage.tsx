import { useEffect, useMemo, useState } from "react";
import { getArchive, SessionExpired } from "./api";
import {
  BlockNote,
  Empty,
  Icon,
  OrderStatus,
  Panel,
  TradeTable,
} from "./components";
import {
  dayLabel,
  offsetLabel,
  price,
  quantity,
  sideLabel,
  time,
} from "./format";
import type { Archive, DayEntry, Order, Snapshot, Trade } from "./types";

export default function TradingPage({
  snapshot,
  days,
  now,
  onExpired,
}: {
  snapshot: Snapshot | null;
  days: DayEntry[];
  now: number;
  onExpired: () => void;
}) {
  const [kind, setKind] = useState<"trades" | "orders">("trades");
  const [selected, setSelected] = useState("");
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState("");
  const [page, setPage] = useState(0);
  const [archive, setArchive] = useState<Archive<Order | Trade> | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const day = selected || snapshot?.trading_day || days[0]?.trading_day || "";
  const choices = [
    ...new Set(
      [snapshot?.trading_day, ...days.map((d) => d.trading_day)].filter(
        (d): d is string => !!d && /^\d{8}$/.test(d),
      ),
    ),
  ]
    .sort()
    .reverse();
  useEffect(() => {
    setSelected("");
    setPage(0);
  }, [snapshot?.instance_id]);
  useEffect(() => {
    const controller = new AbortController();
    let disposed = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    setArchive(null);
    setError("");
    setPage(0);
    if (!day) return;
    const refresh = async () => {
      setLoading(true);
      try {
        const result = await getArchive<Order | Trade>(
          day,
          kind,
          controller.signal,
          snapshot?.data_scope_id ?? "",
        );
        if (!disposed) {
          setArchive(result);
          setError("");
        }
      } catch (failure) {
        if (failure instanceof SessionExpired) onExpired();
        else if (!disposed) setError("完整当日记录暂不可用");
      } finally {
        if (!disposed) {
          setLoading(false);
          if (day === snapshot?.trading_day)
            timer = setTimeout(refresh, 10_000);
        }
      }
    };
    void refresh();
    return () => {
      disposed = true;
      controller.abort();
      clearTimeout(timer);
    };
  }, [
    day,
    kind,
    snapshot?.trading_day,
    snapshot?.instance_id,
    snapshot?.data_scope_id,
    onExpired,
  ]);
  const fallback = day === snapshot?.trading_day ? snapshot?.[kind]?.data : [];
  const rows = archive?.data ?? fallback ?? [];
  const filtered = useMemo(
    () =>
      rows
        .filter(
          (row) =>
            row.instrument_id
              .toLowerCase()
              .includes(query.trim().toLowerCase()) &&
            (!status || ("status" in row && row.status === status)),
        )
        .sort((a, b) => b.as_of_ms - a.as_of_ms),
    [rows, query, status],
  );
  const visible = filtered.slice(page * 50, page * 50 + 50);
  return (
    <Panel
      title="订单与成交"
      subtitle="按交易日归档 · 保留真实成交与委托状态"
      tools={
        <div className="segments">
          <button
            className={kind === "trades" ? "active" : ""}
            onClick={() => {
              setKind("trades");
              setStatus("");
            }}
          >
            成交记录
          </button>
          <button
            className={kind === "orders" ? "active" : ""}
            onClick={() => setKind("orders")}
          >
            委托记录
          </button>
        </div>
      }
    >
      <div className="filters">
        <label>
          交易日
          <select
            aria-label="交易日"
            value={day}
            onChange={(e) => {
              setSelected(e.target.value);
              setPage(0);
            }}
          >
            <option value="" disabled>
              等待交易日
            </option>
            {choices.map((d) => (
              <option value={d} key={d}>
                {dayLabel(d)}
              </option>
            ))}
          </select>
        </label>
        <label className="search-field">
          <Icon name="search" size={17} />
          <input
            aria-label="筛选合约"
            placeholder="搜索合约，例如 hc2610"
            value={query}
            onChange={(e) => {
              setQuery(e.target.value);
              setPage(0);
            }}
          />
        </label>
        {kind === "orders" && (
          <label>
            状态
            <select
              aria-label="委托状态"
              value={status}
              onChange={(e) => {
                setStatus(e.target.value);
                setPage(0);
              }}
            >
              <option value="">全部状态</option>
              <option value="submitted">已报</option>
              <option value="partially_filled">部分成交</option>
              <option value="filled">全部成交</option>
              <option value="canceled">已撤单</option>
              <option value="rejected">已拒单</option>
            </select>
          </label>
        )}
        <span className="filter-count">{filtered.length} 条记录</span>
      </div>
      {error && (
        <div className="inline-notice">
          {error}
          {fallback?.length ? "，当前仅展示最近记录。" : "。"}
        </div>
      )}
      {loading && !rows.length ? (
        <Empty>正在读取交易记录</Empty>
      ) : kind === "trades" ? (
        <TradeTable
          rows={visible as Trade[]}
          empty={
            query ? "没有符合筛选条件的成交" : "当前交易日暂无可用成交记录"
          }
        />
      ) : visible.length ? (
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>合约</th>
                <th>方向 / 开平</th>
                <th>状态</th>
                <th className="numeric">委托 / 已成交</th>
                <th className="numeric">成交均价</th>
                <th>时间 · 北京</th>
                <th>策略 / 原因</th>
              </tr>
            </thead>
            <tbody>
              {(visible as Order[]).map((row) => (
                <tr key={row.order_id}>
                  <td>
                    <strong>{row.instrument_id}</strong>
                    <small>{row.exchange_id}</small>
                  </td>
                  <td>
                    {sideLabel(row.side)}
                    <small>{offsetLabel(row.offset)}</small>
                  </td>
                  <td>
                    <OrderStatus value={row.status} />
                  </td>
                  <td className="numeric">
                    {quantity(row.total_volume)} / {quantity(row.filled_volume)}
                  </td>
                  <td className="numeric">
                    {row.filled_volume > 0 ? price(row.avg_fill_price) : "—"}
                  </td>
                  <td className="tabular">{time(row.as_of_ms)}</td>
                  <td className="order-attribution">
                    {row.attribution === "strategy"
                      ? row.strategy_id
                      : "未归因"}
                    {(row.reject_message ||
                      row.reject_code ||
                      row.reason_code) && (
                      <small>
                        {row.reject_message ||
                          row.reject_code ||
                          row.reason_code}
                      </small>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <Empty>没有符合条件的委托记录</Empty>
      )}
      <div className="table-footer">
        <BlockNote block={archive ?? snapshot?.[kind]} now={now} />
        <div className="pagination">
          <button disabled={page === 0} onClick={() => setPage((p) => p - 1)}>
            上一页
          </button>
          <span>
            {page + 1} / {Math.max(1, Math.ceil(filtered.length / 50))}
          </span>
          <button
            disabled={(page + 1) * 50 >= filtered.length}
            onClick={() => setPage((p) => p + 1)}
          >
            下一页
          </button>
        </div>
      </div>
    </Panel>
  );
}
