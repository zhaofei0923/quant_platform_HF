import { useCallback, useEffect, useRef, useState } from "react";
import { getCurrent, SessionExpired } from "./api";
import type { Snapshot } from "./types";

export function useDashboard() {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [now, setNow] = useState(Date.now());
  const stopped = useRef(false);
  const currentRequest = useRef<AbortController | null>(null);
  const channel = useRef<BroadcastChannel | null>(null);
  const endSession = useCallback((explicit = false, broadcast = true) => {
    if (stopped.current) return;
    stopped.current = true;
    currentRequest.current?.abort();
    setSnapshot(null);
    setError("");
    if (broadcast) channel.current?.postMessage("logout");
    // Remove sensitive rendered values synchronously before leaving, including bfcache previews.
    document.getElementById("root")?.replaceChildren();
    window.location.replace(
      explicit
        ? "/auth/logout"
        : `/auth/?rd=${encodeURIComponent(window.location.origin + "/")}`,
    );
  }, []);
  useEffect(() => {
    stopped.current = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let running = false;
    let disposed = false;
    const refresh = async () => {
      if (disposed || stopped.current || document.hidden || running) return;
      running = true;
      const controller = new AbortController();
      currentRequest.current = controller;
      const timeout = setTimeout(() => controller.abort(), 8_000);
      try {
        const value = await getCurrent(controller.signal);
        if (!disposed && !stopped.current) {
          setSnapshot(value);
          setError("");
        }
      } catch (failure) {
        if (failure instanceof SessionExpired) endSession();
        else if (!disposed && !stopped.current)
          setError(
            failure instanceof Error && failure.name !== "AbortError"
              ? failure.message
              : "连接超时，等待重试",
          );
      } finally {
        clearTimeout(timeout);
        running = false;
        if (!disposed && !stopped.current) {
          setLoading(false);
          timer = setTimeout(refresh, 2_000);
        }
      }
    };
    const visibility = () => {
      clearTimeout(timer);
      if (!document.hidden) void refresh();
    };
    const pageShow = (event: PageTransitionEvent) => {
      if (event.persisted) {
        setSnapshot(null);
        window.location.reload();
      }
    };
    if ("BroadcastChannel" in window) {
      channel.current = new BroadcastChannel("quant-dashboard-session");
      channel.current.onmessage = (event) => {
        if (event.data === "logout") endSession(false, false);
      };
    }
    document.addEventListener("visibilitychange", visibility);
    window.addEventListener("pageshow", pageShow);
    const clock = setInterval(() => setNow(Date.now()), 1_000);
    void refresh();
    return () => {
      disposed = true;
      clearTimeout(timer);
      clearInterval(clock);
      currentRequest.current?.abort();
      channel.current?.close();
      document.removeEventListener("visibilitychange", visibility);
      window.removeEventListener("pageshow", pageShow);
    };
  }, [endSession]);
  return { snapshot, error, loading, now, endSession };
}
