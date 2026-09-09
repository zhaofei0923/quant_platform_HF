import { equitySeries } from "./api";
import type { EquityPoint } from "./types";

export interface EquityTimelineSample {
  key: string;
  asOfMs: number | null;
  balance: number | null;
  gapBefore?: { fromMs: number; toMs: number };
}

export interface EquityTimelineGap {
  fromKey: string;
  toKey: string;
  fromBalance: number;
  toBalance: number;
}

export interface EquityTimeline {
  samples: EquityTimelineSample[];
  gaps: EquityTimelineGap[];
}

// Category slots compress elapsed time without classifying why observations are absent.
export function buildEquityTimeline(points: EquityPoint[]): EquityTimeline {
  const series = equitySeries(points);
  const samples: EquityTimelineSample[] = [];
  const gaps: EquityTimelineGap[] = [];
  let gapBefore: EquityTimelineSample["gapBefore"];

  for (let index = 0; index < series.length; index += 1) {
    const [asOfMs, balance] = series[index];
    if (balance === null) {
      const previous = series[index - 1];
      const next = series[index + 1];
      if (!previous || !next || previous[1] === null || next[1] === null) continue;
      gapBefore = { fromMs: previous[0], toMs: next[0] };
      samples.push({
        key: `gap:${previous[0]}:${next[0]}`,
        asOfMs: null,
        balance: null,
      });
      gaps.push({
        fromKey: String(previous[0]),
        toKey: String(next[0]),
        fromBalance: previous[1],
        toBalance: next[1],
      });
      continue;
    }

    samples.push({
      key: String(asOfMs),
      asOfMs,
      balance,
      ...(gapBefore ? { gapBefore } : {}),
    });
    gapBefore = undefined;
  }

  return { samples, gaps };
}
