import { afterEach, describe, expect, it, vi } from "vitest";
import { getArchive, getCurrent, getJson, SessionExpired } from "./api";
import { fixture } from "../tests/fixtures";

afterEach(() => vi.unstubAllGlobals());
const serve = (data: unknown, status = 200, type = "application/json") =>
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response(JSON.stringify(data), {
          status,
          headers: { "content-type": type },
        }),
    ),
  );
describe("Authenticated data boundary", () => {
  it("never treats an expired session as an empty account", async () => {
    serve({}, 401);
    await expect(
      getCurrent(new AbortController().signal),
    ).rejects.toBeInstanceOf(SessionExpired);
  });
  it("rejects login HTML returned in place of JSON", async () => {
    serve({}, 200, "text/html");
    await expect(
      getJson("/data/v1/current.json", new AbortController().signal),
    ).rejects.toThrow("数据格式异常");
  });
  it("rejects malformed order rows before rendering, allowing polling recovery", async () => {
    const value = fixture();
    (value.orders.data[0] as unknown as Record<string, unknown>).instrument_id =
      null;
    serve(value);
    await expect(getCurrent(new AbortController().signal)).rejects.toThrow(
      "交易数据结构异常",
    );
  });
  it("accepts a real zero balance", async () => {
    const value = fixture();
    value.account.data!.balance = 0;
    serve(value);
    expect(
      (await getCurrent(new AbortController().signal)).account.data!.balance,
    ).toBe(0);
  });
  it("accepts nullable quote values and preserves a true zero volume", async () => {
    const value = fixture();
    value.markets.data[0].last_price = null;
    value.markets.data[0].bid_price_1 = null;
    value.markets.data[0].volume = 0;
    serve(value);
    const result = await getCurrent(new AbortController().signal);
    expect(result.markets.data[0].last_price).toBeNull();
    expect(result.markets.data[0].volume).toBe(0);
  });
  it("rejects malformed nullable market values instead of coercing them", async () => {
    const value = fixture();
    (value.markets.data[0] as unknown as Record<string, unknown>).last_price =
      "3282";
    serve(value);
    await expect(getCurrent(new AbortController().signal)).rejects.toThrow(
      "行情数据结构异常",
    );
  });
  it("normalizes a legacy v1 snapshot without markets or new risk fields", async () => {
    const value = fixture() as unknown as Record<string, unknown>;
    delete value.markets;
    const strategyBlock = value.strategy_positions as {
      data: Record<string, unknown>[];
    };
    delete strategyBlock.data[0].effective_stop;
    delete strategyBlock.data[0].stop_kind;
    delete strategyBlock.data[0].as_of_ms;
    serve(value);
    const result = await getCurrent(new AbortController().signal);
    expect(result.markets).toMatchObject({
      quality: "missing",
      as_of_ms: null,
      data: [],
    });
    expect(result.strategy_positions.data[0]).toMatchObject({
      effective_stop: null,
      stop_kind: null,
      as_of_ms: null,
    });
  });
  it("accepts a publisher missing-market block with a null source time", async () => {
    const value = fixture();
    value.markets = {
      data: [],
      as_of_ms: null,
      quality: "missing",
      source: "dashboard_private_v1",
      trading_day: value.trading_day,
      stale_after_ms: 5_000,
    };
    serve(value);
    const result = await getCurrent(new AbortController().signal);
    expect(result.markets.quality).toBe("missing");
    expect(result.markets.as_of_ms).toBeNull();
    expect(result.markets.data).toEqual([]);
  });
  it("rejects an unknown active stop type", async () => {
    const value = fixture();
    (
      value.strategy_positions.data[0] as unknown as Record<string, unknown>
    ).stop_kind = "calculated";
    serve(value);
    await expect(getCurrent(new AbortController().signal)).rejects.toThrow(
      "交易数据结构异常",
    );
  });
  it("rejects a different account scope and path traversal in archive requests", async () => {
    serve({ schema_version: 1, data_scope_id: "other", data: [] });
    await expect(
      getArchive(
        "20260908",
        "trades",
        new AbortController().signal,
        "expected",
      ),
    ).rejects.toThrow("来源不匹配");
    await expect(
      getArchive(
        "../private",
        "trades",
        new AbortController().signal,
        "expected",
      ),
    ).rejects.toThrow("交易日无效");
  });
});
