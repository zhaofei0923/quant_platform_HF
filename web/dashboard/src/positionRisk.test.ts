import { describe, expect, it } from "vitest";
import {
  buildContractPositionRows,
  buildRiskMonitorRows,
  directionalDistance,
  isExplicitClosedSession,
  marketQuality,
  riskQuality,
} from "./positionRisk";
import { block, fixture, NOW } from "../tests/fixtures";
import type { Quality } from "./types";

describe("contract position list", () => {
  it("uses one sorted row per exact held contract and excludes unheld quotes and zero-only contracts", () => {
    const value = fixture();
    value.positions.data.push(
      { ...value.positions.data[0], instrument_id: "rb2701" },
      { ...value.positions.data[0], instrument_id: "hc2705" },
      { ...value.positions.data[0], instrument_id: "ag2701", position: 0 },
    );
    value.markets.data.push({ ...value.markets.data[0], instrument_id: "cu2701" });
    const rows = buildContractPositionRows(value, NOW);
    expect(rows.map((row) => row.instrumentId)).toEqual([
      "hc2610", "hc2705", "rb2701",
    ]);
    expect(rows.map((row) => row.id)).toEqual(rows.map((row) => row.instrumentId));
    expect(rows[0].quote).toBe(value.markets.data[0]);
    expect(rows[1].quote).toBeUndefined();
  });

  it("sums raw broker records once across dates and strategies without changing their stops", () => {
    const value = fixture();
    value.positions.data[0].use_margin = 100;
    value.positions.data[0].position_profit = 10;
    value.positions.data.push(
      {
        ...value.positions.data[0],
        position_date: "2",
        position: 2,
        today_position: 0,
        yd_position: 2,
        use_margin: 200,
        position_profit: -5,
      },
      {
        ...value.positions.data[0],
        position: 0,
        use_margin: 5,
        position_profit: 2,
      },
    );
    value.strategy_positions.data.push({
      ...value.strategy_positions.data[0],
      strategy_id: "Trend",
      net: 2,
      effective_stop: 3260,
      take_profit: 3340,
    });
    const rows = buildContractPositionRows(value, NOW);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      brokerLong: 3,
      brokerShort: 0,
      brokerUnknown: 0,
      brokerMargin: 305,
      brokerPositionProfit: 7,
      brokerQuality: "fresh",
    });
    expect(rows[0].brokerPositions).toEqual(value.positions.data);
    expect(rows[0].strategies).toEqual(value.strategy_positions.data);
    expect(rows[0].riskRows.map((row) => row.strategy?.effective_stop)).toEqual([
      3270, 3260,
    ]);
    expect(rows[0].riskRows.map((row) => row.strategy?.take_profit)).toEqual([
      3310, 3340,
    ]);
    expect(rows[0].riskRows.every((row) => row.reconciliation === "matched")).toBe(true);
    // The existing risk helper repeats the broker total for each strategy.
    expect(rows[0].riskRows.reduce((sum, row) => sum + row.brokerQuantity, 0)).toBe(6);
    expect(rows[0].brokerLong).toBe(3);
  });

  it("retains simultaneous long and short positions without netting them", () => {
    const value = fixture();
    value.positions.data[0].position = 3;
    value.strategy_positions.data[0].net = 3;
    value.positions.data.push({
      ...value.positions.data[0],
      posi_direction: "3",
      position: 2,
    });
    value.strategy_positions.data.push({
      ...value.strategy_positions.data[0],
      strategy_id: "ShortTrend",
      net: -2,
      effective_stop: 3300,
      take_profit: 3200,
    });
    const [row] = buildContractPositionRows(value, NOW);
    expect(row).toMatchObject({ brokerLong: 3, brokerShort: 2, brokerUnknown: 0 });
    expect(row.riskRows.map((risk) => risk.direction)).toEqual(["long", "short"]);
    expect(row.riskRows.every((risk) => risk.reconciliation === "matched")).toBe(true);
  });

  it("keeps unbound speculative remainders and hedge, arbitrage, and unknown-direction positions visible", () => {
    const value = fixture();
    value.positions.data[0].position = 2;
    value.positions.data.push(
      { ...value.positions.data[0], hedge_flag: "2", position: 3 },
      { ...value.positions.data[0], hedge_flag: "3", position: 4 },
      { ...value.positions.data[0], posi_direction: "unknown", position: 5 },
    );
    const [row] = buildContractPositionRows(value, NOW);
    expect(row).toMatchObject({ brokerLong: 9, brokerShort: 0, brokerUnknown: 5 });
    const unbound = row.riskRows.filter((risk) => risk.reconciliation === "unbound");
    expect(unbound.map((risk) => risk.brokerQuantity)).toEqual([8, 5]);
    expect(unbound.every((risk) => risk.strategy === null)).toBe(true);
    expect(row.riskRows.find((risk) => risk.strategy)?.reconciliation).toBe("mismatch");
  });

  it("shows confirmed zero broker amounts for strategy-only positions after a complete fresh query", () => {
    const value = fixture();
    value.positions.data = [];
    const [row] = buildContractPositionRows(value, NOW);
    expect(row).toMatchObject({
      brokerLong: 0,
      brokerShort: 0,
      brokerUnknown: 0,
      brokerMargin: 0,
      brokerPositionProfit: 0,
      brokerQuality: "fresh",
    });
    expect(row.strategies).toHaveLength(1);
    expect(row.riskRows[0].reconciliation).toBe("mismatch");
  });

  it.each<Quality>(["missing", "invalid", "incomplete", "stale", "catching_up", "historical"])(
    "does not invent zero broker amounts for strategy-only positions with %s broker data",
    (quality) => {
      const value = fixture();
      value.positions.data = [];
      value.positions.quality = quality;
      const [row] = buildContractPositionRows(value, NOW);
      expect(row).toMatchObject({
        brokerLong: null,
        brokerShort: null,
        brokerUnknown: null,
        brokerMargin: null,
        brokerPositionProfit: null,
        brokerQuality: quality,
      });
      expect(row.riskRows[0].reconciliation).toBe("mismatch");
    },
  );

  it("retains stale broker values while downgrading a former quantity match", () => {
    const value = fixture();
    value.positions.as_of_ms = NOW - 15_001;
    const [row] = buildContractPositionRows(value, NOW);
    expect(row).toMatchObject({
      brokerLong: 1,
      brokerShort: 0,
      brokerMargin: 3921.6,
      brokerPositionProfit: 1280,
      brokerQuality: "stale",
    });
    expect(row.riskRows[0].reconciliation).toBe("mismatch");
    value.positions.data = [];
    expect(buildContractPositionRows(value, NOW)[0].brokerLong).toBeNull();
  });

  it("keeps malformed broker quantities visible as unknown instead of displaying flat or matched", () => {
    for (const quantity of [Number.NaN, Number.POSITIVE_INFINITY, -1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
      const value = fixture();
      value.positions.data[0].position = quantity;
      value.strategy_positions.data = [];
      const rows = buildContractPositionRows(value, NOW);
      expect(rows).toHaveLength(1);
      expect(rows[0].brokerLong).toBeNull();
      expect(rows[0].brokerQuality).toBe("invalid");
      expect(rows[0].riskRows.some((row) => row.reconciliation === "matched")).toBe(false);
    }
  });

  it("does not turn invalid broker money into zero or a reconciled position", () => {
    const value = fixture();
    value.positions.data[0].use_margin = Number.NaN;
    value.positions.data[0].position_profit = Number.POSITIVE_INFINITY;
    const [row] = buildContractPositionRows(value, NOW);
    expect(row).toMatchObject({ brokerLong: 1, brokerMargin: null, brokerPositionProfit: null, brokerQuality: "invalid" });
    expect(row.riskRows[0].reconciliation).toBe("mismatch");
  });

  it("rejects overflowing summed quantities and retains malformed strategy-only contracts", () => {
    const value = fixture();
    value.positions.data[0].position = Number.MAX_SAFE_INTEGER;
    value.positions.data.push({ ...value.positions.data[0], position: 1 });
    expect(buildContractPositionRows(value, NOW)[0]).toMatchObject({
      brokerLong: null,
      brokerQuality: "invalid",
    });
    value.positions.data = [];
    value.strategy_positions.data[0].net = Number.NaN;
    const [row] = buildContractPositionRows(value, NOW);
    expect(row.instrumentId).toBe("hc2610");
    expect(row.strategies).toHaveLength(1);
    expect(row.riskRows).toEqual([]);
  });

  it("does not mutate snapshot records when aggregating and reconciling", () => {
    const value = fixture();
    const before = structuredClone(value);
    buildContractPositionRows(value, NOW + 20_000);
    expect(value).toEqual(before);
  });

  it("returns no rows for confirmed empty holdings or an absent snapshot", () => {
    const value = fixture();
    value.positions.data[0].position = 0;
    value.strategy_positions.data[0].net = 0;
    expect(buildContractPositionRows(value, NOW)).toEqual([]);
    expect(buildContractPositionRows(null, NOW)).toEqual([]);
  });
});

describe("position risk monitor", () => {
  it("keeps multiple strategies separate while reconciling their combined speculative position", () => {
    const value = fixture();
    value.positions.data[0].position = 3;
    value.positions.data[0].today_position = 3;
    value.strategy_positions.data.push({
      ...value.strategy_positions.data[0],
      strategy_id: "Trend",
      owner_strategy_id: "Trend",
      net: 2,
      initial_stop: 3240,
      trailing_stop: 3260,
      effective_stop: 3260,
      take_profit: 3340,
    });
    const rows = buildRiskMonitorRows(
      value.positions.data,
      value.strategy_positions.data,
      value.markets.data,
    );
    expect(rows).toHaveLength(2);
    expect(rows.map((row) => row.strategy?.strategy_id)).toEqual([
      "KAMA",
      "Trend",
    ]);
    expect(rows.map((row) => row.strategy?.effective_stop)).toEqual([
      3270, 3260,
    ]);
    expect(rows.every((row) => row.multipleStrategies)).toBe(true);
    expect(rows.every((row) => row.reconciliation === "matched")).toBe(true);
    expect(rows.every((row) => row.brokerQuantity === 3)).toBe(true);
  });

  it("does not match strategy risk to arbitrage, hedge, or unknown-direction broker rows", () => {
    const value = fixture();
    value.positions.data[0].hedge_flag = "2";
    value.positions.data.push({
      ...value.positions.data[0],
      hedge_flag: "1",
      posi_direction: "unknown",
      position: 2,
      today_position: 2,
    });
    const rows = buildRiskMonitorRows(
      value.positions.data,
      value.strategy_positions.data,
      value.markets.data,
    );
    expect(rows.filter((row) => row.strategy)).toHaveLength(1);
    expect(rows.find((row) => row.strategy)?.reconciliation).toBe("mismatch");
    expect(rows.filter((row) => row.reconciliation === "unbound")).toHaveLength(
      2,
    );
    expect(
      rows
        .filter((row) => row.reconciliation === "unbound")
        .map((row) => row.brokerQuantity)
        .sort(),
    ).toEqual([1, 2]);
  });

  it("adds only the unmatched speculative remainder as an unbound row", () => {
    const value = fixture();
    value.positions.data[0].position = 2;
    value.positions.data[0].today_position = 2;
    const rows = buildRiskMonitorRows(
      value.positions.data,
      value.strategy_positions.data,
      value.markets.data,
    );
    expect(rows.find((row) => row.strategy)?.reconciliation).toBe("mismatch");
    const unbound = rows.find((row) => !row.strategy);
    expect(unbound?.brokerQuantity).toBe(1);
  });

  it("computes stop and take-profit distance in the position direction", () => {
    expect(directionalDistance(100, 95, "long", "stop")).toMatchObject({
      points: 5,
      percent: 5,
      crossed: false,
    });
    expect(directionalDistance(100, 105, "long", "take_profit")).toMatchObject({
      points: 5,
      crossed: false,
    });
    expect(directionalDistance(100, 105, "short", "stop")).toMatchObject({
      points: 5,
      crossed: false,
    });
    expect(directionalDistance(100, 95, "short", "take_profit")).toMatchObject({
      points: 5,
      crossed: false,
    });
    expect(directionalDistance(94, 95, "long", "stop")?.crossed).toBe(true);
    expect(directionalDistance(100, null, "long", "stop")).toBeNull();
  });

  it("uses a five-second live TTL and only labels an explicitly closed session as historical", () => {
    const value = fixture();
    const quote = { ...value.markets.data[0], as_of_ms: NOW - 6_000 };
    const marketBlock = { ...value.markets, data: [quote] };
    expect(marketQuality(quote, marketBlock, NOW, false)).toBe("stale");
    expect(marketQuality(quote, marketBlock, NOW, true)).toBe("historical");
    const strategy = {
      ...value.strategy_positions.data[0],
      as_of_ms: NOW - 6_000,
    };
    expect(
      riskQuality(strategy, { ...value.strategy_positions, data: [strategy] }, NOW),
    ).toBe("stale");
    expect(
      riskQuality(
        strategy,
        { ...value.strategy_positions, data: [strategy] },
        NOW,
        true,
      ),
    ).toBe("historical");
    expect(
      marketQuality(
        { ...quote, quality: "invalid" },
        marketBlock,
        NOW,
        true,
      ),
    ).toBe("invalid");
    expect(
      riskQuality(
        strategy,
        { ...value.strategy_positions, quality: "invalid", data: [strategy] },
        NOW,
        true,
      ),
    ).toBe("invalid");
    expect(isExplicitClosedSession(undefined, NOW)).toBe(false);
    expect(
      isExplicitClosedSession(
        {
          ...value.health,
          data: {
            pipeline: {
              ...value.health.data.pipeline,
              session: "unknown",
            },
          },
        },
        NOW,
      ),
    ).toBe(false);
    const closedHealth = {
      ...value.health,
      data: {
        ...value.health.data,
        pipeline: {
          ...value.health.data.pipeline,
          session: "closed",
          overall_status: "inactive",
        },
      },
    };
    expect(isExplicitClosedSession(closedHealth, NOW)).toBe(true);
    expect(
      isExplicitClosedSession(
        { ...closedHealth, as_of_ms: NOW - 11_000 },
        NOW,
      ),
    ).toBe(false);
    expect(
      isExplicitClosedSession(
        {
          ...closedHealth,
          data: {
            ...closedHealth.data,
            pipeline: {
              ...closedHealth.data.pipeline,
              as_of_ms: NOW - 11_000,
            },
          },
        },
        NOW,
      ),
    ).toBe(false);
    expect(
      isExplicitClosedSession(
        { ...closedHealth, quality: "invalid" },
        NOW,
      ),
    ).toBe(false);
  });

  it("does not invent a risk timestamp for a legacy strategy row", () => {
    const strategy = { ...fixture().strategy_positions.data[0], as_of_ms: null };
    expect(riskQuality(strategy, block([strategy]), NOW)).toBe("missing");
  });

  it("marks an unquoted held contract as missing despite a fresh market block", () => {
    const value = fixture();
    value.positions.data.push({ ...value.positions.data[0], instrument_id: "rb2701" });
    const row = buildContractPositionRows(value, NOW).find(
      (position) => position.instrumentId === "rb2701",
    );
    expect(row?.quote).toBeUndefined();
    expect(marketQuality(row?.quote, value.markets, NOW, false)).toBe("missing");
    expect(marketQuality(row?.quote, value.markets, NOW, true)).toBe("missing");
    for (const quality of ["invalid", "incomplete", "catching_up"] as const) {
      expect(marketQuality(row?.quote, { ...value.markets, quality }, NOW, false)).toBe(quality);
    }
  });
});
