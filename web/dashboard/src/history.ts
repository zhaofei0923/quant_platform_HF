import { useEffect, useState } from "react";
import { getArchive, getDays, SessionExpired } from "./api";
import type { DayEntry, EquityPoint } from "./types";

export function useHistory(
  range: number,
  instance: string,
  tradingDay: string,
  onExpired: () => void,
) {
  const [days, setDays] = useState<DayEntry[]>([]);
  const [points, setPoints] = useState<EquityPoint[]>([]);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [loadedScope, setLoadedScope] = useState("");
  useEffect(() => {
    const controller = new AbortController();
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    setDays([]);
    setPoints([]);
    setError("");
    setLoading(true);
    const refresh = async () => {
      if (document.hidden) {
        timer = setTimeout(refresh, 10_000);
        return;
      }
      try {
        if (!instance) return;
        const list = await getDays(controller.signal, instance);
        if (stopped) return;
        setDays(list);
        setLoadedScope(instance);
        // Calendar window relative to newest published trading day, not number of nonempty sessions.
        const latest = /^\d{8}$/.test(tradingDay) ? tradingDay : "";
        const latestDate = latest
          ? Date.UTC(
              +latest.slice(0, 4),
              +latest.slice(4, 6) - 1,
              +latest.slice(6, 8),
            )
          : 0;
        const selected = list.filter((item) => {
          const day = item.trading_day;
          const ts = Date.UTC(
            +day.slice(0, 4),
            +day.slice(4, 6) - 1,
            +day.slice(6, 8),
          );
          return (
            !!latest &&
            ts <= latestDate &&
            ts >= latestDate - (range - 1) * 86400_000
          );
        });
        const samples: EquityPoint[] = [];
        let failed = false;
        // Bound concurrency on the small shared trading server.
        for (let i = 0; i < selected.length; i += 4) {
          const group = await Promise.allSettled(
            selected
              .slice(i, i + 4)
              .map((day) =>
                getArchive<EquityPoint>(
                  day.trading_day,
                  "equity",
                  controller.signal,
                  instance,
                ),
              ),
          );
          for (const result of group) {
            if (result.status === "fulfilled")
              samples.push(...result.value.data);
            else if (result.reason instanceof SessionExpired)
              throw result.reason;
            else failed = true;
          }
        }
        if (!stopped) {
          setPoints(samples);
          setError(failed ? "部分历史暂不可用，缺失区间未补造" : "");
        }
      } catch (failure) {
        if (failure instanceof SessionExpired) onExpired();
        else if (!stopped) setError("权益历史尚未就绪或暂不可用");
      } finally {
        if (!stopped) {
          setLoading(false);
          timer = setTimeout(refresh, 60_000);
        }
      }
    };
    void refresh();
    return () => {
      stopped = true;
      controller.abort();
      clearTimeout(timer);
    };
  }, [range, instance, tradingDay, onExpired]);
  return {
    days: loadedScope === instance ? days : [],
    points: loadedScope === instance ? points : [],
    error,
    loading,
  };
}
