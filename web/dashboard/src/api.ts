import type { Archive, Days, EquityPoint, Snapshot } from "./types";

export class SessionExpired extends Error {}
const record = (value: unknown): value is Record<string, unknown> =>
  !!value && typeof value === "object" && !Array.isArray(value);
const qualities = new Set([
  "fresh",
  "stale",
  "missing",
  "invalid",
  "incomplete",
  "catching_up",
  "historical",
]);
const qualityValid = (value: unknown) =>
  typeof value === "string" && qualities.has(value);
const nullableFinite = (value: unknown) =>
  value === null || (typeof value === "number" && Number.isFinite(value));
function rowsValid(
  value: unknown,
  fields: string[],
  numeric: string[],
  nullable = false,
) {
  return (
    Array.isArray(value) &&
    value.every(
      (row) =>
        record(row) &&
        fields.every((field) => typeof row[field] === "string") &&
        numeric.every(
          (field) =>
            (nullable && row[field] === null) ||
            (typeof row[field] === "number" && Number.isFinite(row[field])),
        ),
    )
  );
}
function transactionsValid(value: unknown, kind: "orders" | "trades") {
  return rowsValid(
    value,
    [
      "instrument_id",
      "exchange_id",
      "strategy_id",
      "side",
      "offset",
      "attribution",
      "order_id",
      kind === "orders" ? "status" : "trade_id",
    ],
    kind === "orders"
      ? ["total_volume", "filled_volume", "avg_fill_price", "as_of_ms"]
      : ["volume", "price", "as_of_ms"],
    true,
  );
}
function strategyRowsValid(value: unknown) {
  return (
    Array.isArray(value) &&
    value.every((candidate) => {
      if (!record(candidate)) return false;
      if (
        typeof candidate.strategy_id !== "string" ||
        typeof candidate.instrument_id !== "string" ||
        typeof candidate.net !== "number" ||
        !Number.isFinite(candidate.net) ||
        (candidate.owner_strategy_id !== undefined &&
          typeof candidate.owner_strategy_id !== "string")
      )
        return false;
      if (
        !["avg_open", "initial_stop", "trailing_stop", "take_profit"].every(
          (field) => field in candidate && nullableFinite(candidate[field]),
        ) ||
        (candidate.effective_stop !== undefined &&
          !nullableFinite(candidate.effective_stop)) ||
        (candidate.as_of_ms !== undefined &&
          !nullableFinite(candidate.as_of_ms)) ||
        (candidate.stop_kind !== undefined &&
          candidate.stop_kind !== null &&
          candidate.stop_kind !== "initial" &&
          candidate.stop_kind !== "trailing")
      )
        return false;
      return true;
    })
  );
}
function marketRowsValid(value: unknown) {
  return (
    Array.isArray(value) &&
    value.every((candidate) => {
      if (!record(candidate)) return false;
      if (
        typeof candidate.instrument_id !== "string" ||
        !candidate.instrument_id ||
        typeof candidate.exchange_id !== "string" ||
        (candidate.product_id !== undefined &&
          typeof candidate.product_id !== "string") ||
        !qualityValid(candidate.quality)
      )
        return false;
      return [
        "last_price",
        "bid_price_1",
        "ask_price_1",
        "volume",
        "open_interest",
        "as_of_ms",
      ].every(
        (field) => field in candidate && nullableFinite(candidate[field]),
      );
    })
  );
}
export async function getJson<T>(
  path: string,
  signal: AbortSignal,
): Promise<T> {
  const response = await fetch(path, {
    credentials: "same-origin",
    cache: "no-store",
    signal,
    redirect: "manual",
  });
  if (
    [401, 403].includes(response.status) ||
    response.type === "opaqueredirect" ||
    response.status === 302
  ) {
    throw new SessionExpired("登录已过期");
  }
  if (!response.ok)
    throw new Error(
      response.status === 404 ? "数据尚未发布" : "数据服务暂不可用",
    );
  if (!response.headers.get("content-type")?.includes("application/json"))
    throw new Error("数据格式异常");
  const result = await response.json();
  if (!result || result.schema_version !== 1)
    throw new Error("数据版本不受支持");
  return result as T;
}
export async function getCurrent(signal: AbortSignal) {
  const result = await getJson<Snapshot>("/data/v1/current.json", signal);
  if (
    typeof result.data_scope_id !== "string" ||
    !result.data_scope_id ||
    typeof result.generated_at_ms !== "number" ||
    typeof result.environment !== "string" ||
    typeof result.instance_id !== "string" ||
    typeof result.account_alias !== "string" ||
    typeof result.trading_day !== "string" ||
    ![
      "account",
      "positions",
      "strategy_positions",
      "health",
      "orders",
      "trades",
    ].every((key) => {
      const block = result[key as keyof Snapshot];
      return (
        block &&
        typeof block === "object" &&
        "data" in block &&
        "quality" in block
      );
    })
  )
    throw new Error("账户数据结构异常");
  if (
    !rowsValid(
      result.positions.data,
      [
        "instrument_id",
        "exchange_id",
        "posi_direction",
        "hedge_flag",
        "position_date",
      ],
      ["position", "today_position", "yd_position"],
    ) ||
    !strategyRowsValid(result.strategy_positions.data) ||
    !transactionsValid(result.orders.data, "orders") ||
    !transactionsValid(result.trades.data, "trades")
  )
    throw new Error("交易数据结构异常");
  result.strategy_positions.data = result.strategy_positions.data.map(
    (row) => ({
      ...row,
      effective_stop: row.effective_stop ?? null,
      stop_kind: row.stop_kind ?? null,
      as_of_ms: row.as_of_ms ?? null,
    }),
  );
  const marketBlock = (
    result as Snapshot & { markets?: Snapshot["markets"] }
  ).markets;
  if (marketBlock === undefined) {
    result.markets = {
      data: [],
      as_of_ms: null,
      quality: "missing",
      source: "not_published",
      trading_day: result.trading_day,
      stale_after_ms: 5_000,
    };
  } else if (
    !record(marketBlock) ||
    !qualityValid(marketBlock.quality) ||
    !nullableFinite(marketBlock.as_of_ms) ||
    typeof marketBlock.source !== "string" ||
    typeof marketBlock.trading_day !== "string" ||
    !marketRowsValid(marketBlock.data)
  ) {
    throw new Error("行情数据结构异常");
  }
  if (
    result.account.data !== null &&
    (!record(result.account.data) ||
      ![
        "balance",
        "available",
        "curr_margin",
        "commission",
        "close_profit",
        "position_profit",
      ].every((key) => {
        const value = (
          result.account.data as unknown as Record<string, unknown>
        )[key];
        return (
          value === null ||
          (typeof value === "number" && Number.isFinite(value))
        );
      }))
  )
    throw new Error("资金数据结构异常");
  if (result.health.data === null) result.health.data = {};
  const health = result.health.data;
  if (!record(health)) throw new Error("运行数据结构异常");
  const pipeline = record(health.pipeline) ? health.pipeline : undefined;
  const readiness = record(health.readiness) ? health.readiness : undefined;
  if (
    pipeline?.products &&
    !rowsValid(pipeline.products, ["product_id", "instrument_id", "status"], [])
  )
    throw new Error("行情数据结构异常");
  if (pipeline?.stages && !rowsValid(pipeline.stages, ["name", "status"], []))
    throw new Error("链路数据结构异常");
  if (
    readiness?.reasons &&
    (!Array.isArray(readiness.reasons) ||
      !readiness.reasons.every((reason) => typeof reason === "string"))
  )
    throw new Error("风控数据结构异常");
  return result;
}
export async function getDays(signal: AbortSignal, scope: string) {
  const result = await getJson<Days>("/data/v1/days.json", signal);
  if (!scope || result.data_scope_id !== scope)
    throw new Error("历史账户来源不匹配");
  if (
    !rowsValid(
      result.days,
      ["trading_day"],
      ["equity_points", "order_count", "trade_count"],
    )
  )
    throw new Error("历史索引异常");
  return result.days
    .filter((day) => /^\d{8}$/.test(day.trading_day))
    .sort((a, b) => b.trading_day.localeCompare(a.trading_day));
}
export async function getArchive<T>(
  day: string,
  kind: "equity" | "orders" | "trades",
  signal: AbortSignal,
  scope: string,
) {
  if (!/^\d{8}$/.test(day)) throw new Error("交易日无效");
  const result = await getJson<Archive<T>>(
    `/data/v1/days/${day}/${kind}.json`,
    signal,
  );
  if (!scope || result.data_scope_id !== scope)
    throw new Error("历史账户来源不匹配");
  if (
    kind === "equity"
      ? !rowsValid(result.data, [], ["as_of_ms", "balance"])
      : !transactionsValid(result.data, kind)
  )
    throw new Error("历史数据结构异常");
  return result;
}
// Insert real gaps before charting; never interpolate missing observations as equity.
export function equitySeries(points: EquityPoint[]): [number, number | null][] {
  const unique = new Map<number, number>();
  for (const point of points) {
    if (
      Number.isFinite(point.as_of_ms) &&
      point.as_of_ms > 0 &&
      Number.isFinite(point.balance)
    ) {
      unique.set(point.as_of_ms, point.balance);
    }
  }
  const sorted = [...unique.entries()].sort((a, b) => a[0] - b[0]);
  const result: [number, number | null][] = [];
  for (const [ts, balance] of sorted) {
    const previous = result.at(-1);
    if (previous && ts - previous[0] > 90_000)
      result.push([previous[0] + 60_000, null]);
    result.push([ts, balance]);
  }
  return result;
}
