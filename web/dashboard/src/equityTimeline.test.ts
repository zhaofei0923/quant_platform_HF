import { describe, expect, it } from "vitest";
import { buildEquityTimeline } from "./equityTimeline";
import type { EquityPoint } from "./types";

const at = (asOfMs: number, balance = 200_000): EquityPoint => ({
  as_of_ms: asOfMs,
  balance,
  available: balance,
  curr_margin: 0,
  commission: 0,
  close_profit: 0,
  position_profit: 0,
});
const start = Date.parse("2026-09-09T09:00:00+08:00");

describe("compressed equity sampling timeline", () => {
  it.each([
    ["overnight", "2026-09-08T15:00:00+08:00", "2026-09-09T09:00:00+08:00"],
    ["midday", "2026-09-09T11:30:00+08:00", "2026-09-09T13:30:00+08:00"],
    ["missing observations", "2026-09-09T09:01:00+08:00", "2026-09-09T09:04:00+08:00"],
  ])("compresses %s into one unclassified empty slot with real endpoints", (_, from, to) => {
    const fromMs = Date.parse(from);
    const toMs = Date.parse(to);
    const timeline = buildEquityTimeline([at(fromMs, 200_000), at(toMs, 201_250)]);

    expect(timeline).toEqual({
      samples: [
        { key: String(fromMs), asOfMs: fromMs, balance: 200_000 },
        { key: `gap:${fromMs}:${toMs}`, asOfMs: null, balance: null },
        {
          key: String(toMs),
          asOfMs: toMs,
          balance: 201_250,
          gapBefore: { fromMs, toMs },
        },
      ],
      gaps: [{
        fromKey: String(fromMs),
        toKey: String(toMs),
        fromBalance: 200_000,
        toBalance: 201_250,
      }],
    });
  });

  it("retains the existing strictly greater than 90 second gap threshold", () => {
    const timeline = buildEquityTimeline([
      at(start),
      at(start + 60_000),
      at(start + 150_000),
      at(start + 240_001),
    ]);
    expect(timeline.samples).toHaveLength(5);
    expect(timeline.samples.filter((sample) => sample.asOfMs === null)).toHaveLength(1);
    expect(timeline.samples.at(-1)?.gapBefore).toEqual({
      fromMs: start + 150_000,
      toMs: start + 240_001,
    });
  });

  it("sorts timestamps and keeps the latest duplicate value, including zero", () => {
    const input = [at(start + 60_000, 1), at(start, 100), at(start, 0)];
    const original = input.map((point) => ({ ...point }));
    expect(buildEquityTimeline(input)).toEqual({
      samples: [
        { key: String(start), asOfMs: start, balance: 0 },
        { key: String(start + 60_000), asOfMs: start + 60_000, balance: 1 },
      ],
      gaps: [],
    });
    expect(input).toEqual(original);
  });

  it("keeps each gap independent and annotates only its real right endpoint", () => {
    const timeline = buildEquityTimeline([
      at(start, 0),
      at(start + 300_000, 20),
      at(start + 360_000, 30),
      at(start + 600_000, 10),
    ]);
    expect(timeline.gaps).toEqual([
      { fromKey: String(start), toKey: String(start + 300_000), fromBalance: 0, toBalance: 20 },
      { fromKey: String(start + 360_000), toKey: String(start + 600_000), fromBalance: 30, toBalance: 10 },
    ]);
    expect(timeline.samples).toHaveLength(6);
    expect(new Set(timeline.samples.map((sample) => sample.key)).size).toBe(6);
    expect(timeline.samples[2].gapBefore).toEqual({ fromMs: start, toMs: start + 300_000 });
    expect(timeline.samples[3].gapBefore).toBeUndefined();
    expect(timeline.samples[5].gapBefore).toEqual({ fromMs: start + 360_000, toMs: start + 600_000 });
  });

  it("filters invalid observations using the existing equity validation", () => {
    const timeline = buildEquityTimeline([
      at(Number.NaN), at(0), at(-1), at(Number.POSITIVE_INFINITY),
      at(start, Number.NaN), at(start, Number.NEGATIVE_INFINITY),
      at(start + 60_000, 0),
    ]);
    expect(timeline).toEqual({
      samples: [{ key: String(start + 60_000), asOfMs: start + 60_000, balance: 0 }],
      gaps: [],
    });
  });

  it("handles an empty history and a single valid observation", () => {
    expect(buildEquityTimeline([])).toEqual({ samples: [], gaps: [] });
    expect(buildEquityTimeline([at(start, -10.25)])).toEqual({
      samples: [{ key: String(start), asOfMs: start, balance: -10.25 }],
      gaps: [],
    });
  });
});
