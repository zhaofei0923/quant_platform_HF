import { describe, expect, it } from "vitest";
import { cash, dayLabel, effectiveQuality, price } from "./format";
import { equitySeries } from "./api";
import type { EquityPoint } from "./types";

describe("Financial display preserves meaning", () => {
  it("does not coerce unknown or invalid amounts into zero", () => {
    expect(cash(null)).toBe("—");
    expect(cash(undefined)).toBe("—");
    expect(cash(NaN)).toBe("—");
    expect(cash(0)).toBe("¥0.00");
    expect(cash(-12.5, true)).toBe("−¥12.50");
    expect(cash(12.5, true)).toBe("+¥12.50");
    expect(price(123.456)).toBe("123.456");
  });
  it("expires each source independently and rejects future timestamps", () => {
    expect(effectiveQuality({ quality: "fresh", as_of_ms: 1000 }, 17000)).toBe(
      "stale",
    );
    expect(
      effectiveQuality(
        { quality: "fresh", as_of_ms: 1000, stale_after_ms: 90000 },
        17000,
      ),
    ).toBe("fresh");
    expect(effectiveQuality({ quality: "fresh", as_of_ms: 50000 }, 17000)).toBe(
      "invalid",
    );
    expect(
      effectiveQuality({ quality: "incomplete", as_of_ms: 1000 }, 17000),
    ).toBe("incomplete");
    expect(
      effectiveQuality({ quality: "historical", as_of_ms: 1000 }, 17000),
    ).toBe("historical");
    expect(effectiveQuality({ quality: "missing", as_of_ms: null }, 17000)).toBe(
      "missing",
    );
    expect(effectiveQuality({ quality: "fresh", as_of_ms: null }, 17000)).toBe(
      "invalid",
    );
  });
  it("keeps calendar trading days separate from timestamps", () => {
    expect(dayLabel("20260908")).toBe("2026-09-08");
    expect(dayLabel("../a")).toBe("—");
  });
  it("deduplicates equity points and breaks missing minute intervals without zeros", () => {
    const point = (as_of_ms: number, balance: number) =>
      ({ as_of_ms, balance }) as EquityPoint;
    expect(
      equitySeries([point(180000, 102), point(60000, 100), point(60000, 101)]),
    ).toEqual([
      [60000, 101],
      [120000, null],
      [180000, 102],
    ]);
  });
});
