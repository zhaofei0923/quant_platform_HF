import { useEffect, useMemo, useRef } from "react";
import { init, use } from "echarts/core";
import { LineChart } from "echarts/charts";
import { GridComponent, MarkLineComponent, TooltipComponent } from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import { buildEquityTimeline } from "./equityTimeline";
import { cash } from "./format";
import type { EquityPoint } from "./types";

use([LineChart, GridComponent, MarkLineComponent, TooltipComponent, CanvasRenderer]);
const tickTime = new Intl.DateTimeFormat("zh-CN", {
  timeZone: "Asia/Shanghai",
  hour12: false,
  hour: "2-digit",
  minute: "2-digit",
});
const fullTime = new Intl.DateTimeFormat("zh-CN", {
  timeZone: "Asia/Shanghai",
  month: "2-digit",
  day: "2-digit",
  hour12: false,
  hour: "2-digit",
  minute: "2-digit",
});
function axisAmount(value: number): string {
  const absolute = Math.abs(value);
  const divisor = absolute >= 100_000_000 ? 100_000_000 : absolute >= 10_000 ? 10_000 : 1;
  const unit = divisor === 100_000_000 ? "亿" : divisor === 10_000 ? "万" : "";
  return `${(value / divisor).toLocaleString("zh-CN", { maximumFractionDigits: 2 })}${unit}`;
}
export default function EquityChart({
  points,
  range,
}: {
  points: EquityPoint[];
  range: number;
}) {
  const element = useRef<HTMLDivElement>(null);
  const chartRef = useRef<ReturnType<typeof init> | null>(null);
  const timeline = useMemo(() => buildEquityTimeline(points), [points]);
  useEffect(() => {
    if (!element.current) return;
    const chart = init(element.current, undefined, { renderer: "canvas" });
    chartRef.current = chart;
    chart.setOption({
      animation: false,
      backgroundColor: "transparent",
      textStyle: {
        fontFamily: '"Segoe UI", "Microsoft YaHei", sans-serif',
        color: "#9dafc5",
      },
      grid: { left: 16, right: 18, top: 24, bottom: 18, containLabel: true },
      tooltip: {
        trigger: "axis",
        renderMode: "richText",
        confine: true,
        backgroundColor: "#101C2D",
        borderColor: "#2b5870",
        borderWidth: 1,
        padding: [10, 12],
        textStyle: { color: "#e8f3ff" },
        axisPointer: { lineStyle: { color: "#35D9F3", opacity: 0.45, type: "dashed" } },
      },
      xAxis: {
        type: "category",
        boundaryGap: false,
        axisLine: { lineStyle: { color: "#263f56" } },
        axisTick: { show: false },
        splitLine: { show: false },
        axisLabel: {
          color: "#9dafc5",
          hideOverlap: true,
          margin: 14,
        },
      },
      yAxis: {
        type: "value",
        scale: true,
        splitNumber: 5,
        axisLabel: {
          color: "#9dafc5",
          formatter: axisAmount,
        },
        splitLine: { lineStyle: { color: "#22364c", type: "dashed" } },
      },
      series: [
        {
          id: "account-equity",
          type: "line",
          name: "账户权益",
          data: [],
          connectNulls: false,
          symbolSize: 6,
          smooth: false,
          // Sampling buckets can discard nulls and draw a false solid line over a gap.
          // Keep the original observations and the explicit compressed gap slots.
          markLine: {
            symbol: ["none", "none"],
            silent: true,
            animation: false,
            label: { show: false },
            lineStyle: { color: "#35D9F3", width: 1.5, type: "dashed", opacity: 0.6 },
            tooltip: { show: false },
          },
          lineStyle: { color: "#35D9F3", width: 2 },
          itemStyle: { color: "#35D9F3" },
          areaStyle: {
            color: {
              type: "linear",
              x: 0,
              y: 0,
              x2: 0,
              y2: 1,
              colorStops: [
                { offset: 0, color: "rgba(53, 217, 243, 0.19)" },
                { offset: 1, color: "rgba(53, 217, 243, 0.01)" },
              ],
            },
          },
        },
      ],
    });
    const observer = new ResizeObserver(() => chart.resize());
    observer.observe(element.current);
    return () => {
      observer.disconnect();
      chartRef.current = null;
      chart.dispose();
    };
  }, []);
  useEffect(() => {
    chartRef.current?.setOption({
      xAxis: {
        data: timeline.samples.map((sample) => sample.key),
        axisLabel: {
          formatter: (key: string) => key.startsWith("gap:") ? "" :
            range === 1 ? tickTime.format(Number(key)) : fullTime.format(Number(key)),
        },
      },
      tooltip: {
        formatter: (params: { dataIndex?: number } | { dataIndex?: number }[]) => {
          const index = (Array.isArray(params) ? params[0] : params)?.dataIndex;
          const sample = index === undefined ? undefined : timeline.samples[index];
          if (!sample || sample.asOfMs === null) return "无采样间隔（已压缩）";
          const lines = [fullTime.format(sample.asOfMs) + " 北京时间", `账户权益  ${cash(sample.balance)}`];
          if (sample.gapBefore) {
            lines.push("前段无采样，时间间隔已压缩", `${fullTime.format(sample.gapBefore.fromMs)} → ${fullTime.format(sample.gapBefore.toMs)}`);
          }
          return lines.join("\n");
        },
      },
      series: [
        {
          id: "account-equity",
          data: timeline.samples.map((sample) => sample.balance),
          showSymbol: timeline.samples.length === 1,
          markLine: {
            data: timeline.gaps.map((gap) => [
              { coord: [gap.fromKey, gap.fromBalance] },
              { coord: [gap.toKey, gap.toBalance] },
            ]),
          },
        },
      ],
    });
  }, [timeline, range]);
  return (
    <div
      ref={element}
      className="equity-chart"
      role="img"
      aria-label={`账户权益曲线，${timeline.samples.length - timeline.gaps.length} 个真实采样点；时间间隔已压缩，${timeline.gaps.length} 段无采样间隔用虚线标识`}
    />
  );
}
