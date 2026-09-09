import type { Block, Quality } from "./types";

const cashFormat = new Intl.NumberFormat("zh-CN", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});
const timeFormat = new Intl.DateTimeFormat("zh-CN", {
  timeZone: "Asia/Shanghai",
  hour12: false,
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});
export const finite = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value);
export const cash = (value: unknown, signed = false) =>
  finite(value)
    ? `${signed && value > 0 ? "+" : value < 0 ? "−" : ""}¥${cashFormat.format(Math.abs(value))}`
    : "—";
export const quantity = (value: unknown) =>
  finite(value) ? value.toLocaleString("zh-CN") : "—";
export const price = (value: unknown) =>
  finite(value)
    ? value.toLocaleString("zh-CN", { maximumFractionDigits: 8 })
    : "—";
export const time = (value: unknown) =>
  finite(value) && value > 0 ? timeFormat.format(value) : "—";
export const dayLabel = (day: string | undefined) =>
  day && /^\d{8}$/.test(day)
    ? `${day.slice(0, 4)}-${day.slice(4, 6)}-${day.slice(6, 8)}`
    : "—";
export const sideLabel = (side: string) =>
  ({ buy: "买入", sell: "卖出", unknown: "未知" })[side] ?? "未知";
export const offsetLabel = (offset: string) =>
  ({
    open: "开仓",
    close: "平仓",
    close_today: "平今",
    close_yesterday: "平昨",
    unknown: "未知",
  })[offset] ?? "未知";
export const statusLabel = (status: string) =>
  ({
    new: "新建",
    submitted: "已报",
    partially_filled: "部分成交",
    filled: "全部成交",
    canceled: "已撤单",
    rejected: "已拒单",
    unknown: "未知",
  })[status] ?? status;
export const qualityLabel: Record<Quality, string> = {
  fresh: "已更新",
  stale: "数据已过期",
  missing: "等待数据",
  invalid: "数据异常",
  incomplete: "查询未完整",
  catching_up: "同步中",
  historical: "历史记录",
};
export function effectiveQuality(
  block:
    | Pick<Block<unknown>, "quality" | "as_of_ms" | "stale_after_ms">
    | undefined,
  now: number,
  defaultTtl = 15_000,
): Quality {
  if (!block) return "missing";
  if (
    ![
      "fresh",
      "stale",
      "missing",
      "invalid",
      "incomplete",
      "catching_up",
      "historical",
    ].includes(block.quality)
  )
    return "invalid";
  if (block.quality !== "fresh") return block.quality;
  if (
    !finite(block.as_of_ms) ||
    block.as_of_ms <= 0 ||
    block.as_of_ms > now + 5_000
  )
    return "invalid";
  const ttl =
    finite(block.stale_after_ms) && block.stale_after_ms > 0
      ? block.stale_after_ms
      : defaultTtl;
  return now - block.as_of_ms > ttl ? "stale" : "fresh";
}
export function hasRows<T>(value: unknown): value is T[] {
  return Array.isArray(value);
}
export const pnlClass = (value: unknown) =>
  finite(value) ? (value > 0 ? "positive" : value < 0 ? "negative" : "") : "";
