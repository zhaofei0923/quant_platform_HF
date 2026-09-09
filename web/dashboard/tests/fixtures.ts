import type { Archive, EquityPoint, Snapshot } from "../src/types";

// Browser-test-only data. Never imported into the application or copied to the production public directory.
export const NOW = Date.parse("2026-09-08T06:32:08Z");
export const SCOPE = "test-only-42f36438ea103a8f";
export function block<T>(data: T) {
  return {
    data,
    as_of_ms: NOW,
    quality: "fresh" as const,
    trading_day: "20260908",
    source: "test_fixture",
  };
}
export function fixture(): Snapshot {
  const instruments = ["hc2610", "rb2610", "cu2610", "ni2610", "ag2610"];
  const prices = [3268, 3124, 78650, 121980, 9221];
  const trades = instruments.map((instrument_id, i) => ({
    trade_id: `trade-${i}`,
    order_id: `order-${i}`,
    instrument_id,
    exchange_id: "SHFE",
    strategy_id: i === 4 ? "" : "KAMA",
    side: i % 2 ? "sell" : "buy",
    offset: i % 2 ? "close" : "open",
    volume: (i % 2) + 1,
    price: prices[i],
    as_of_ms: NOW - (i + 1) * 60000,
    attribution: i === 4 ? "unattributed" : "strategy",
  }));
  return {
    schema_version: 1,
    data_scope_id: SCOPE,
    generated_at_ms: NOW,
    environment: "demo",
    account_alias: "个人账户",
    instance_id: "personal",
    trading_day: "20260908",
    account: block({
      balance: 203680.5,
      available: 182460.2,
      curr_margin: 21220.3,
      frozen_margin: 0,
      frozen_cash: 0,
      frozen_commission: 0,
      commission: 239.5,
      close_profit: 2640,
      position_profit: 1280,
    }),
    positions: block([
      {
        instrument_id: "hc2610",
        exchange_id: "SHFE",
        posi_direction: "2",
        hedge_flag: "1",
        position_date: "1",
        position: 1,
        today_position: 1,
        yd_position: 0,
        long_frozen: 0,
        short_frozen: 0,
        open_cost: 32680,
        position_cost: 32680,
        use_margin: 3921.6,
        position_profit: 1280,
        close_profit: 0,
      },
    ]),
    strategy_positions: block([
      {
        strategy_id: "KAMA",
        owner_strategy_id: "KAMA",
        instrument_id: "hc2610",
        net: 1,
        avg_open: 3268,
        initial_stop: 3250,
        take_profit: 3310,
        trailing_stop: 3270,
        effective_stop: 3270,
        stop_kind: "trailing",
        as_of_ms: NOW,
      },
    ]),
    markets: {
      ...block([
        {
          product_id: "hc",
          instrument_id: "hc2610",
          exchange_id: "SHFE",
          last_price: 3282,
          bid_price_1: 3281,
          ask_price_1: 3282,
          volume: 18420,
          open_interest: 96112,
          as_of_ms: NOW,
          quality: "fresh" as const,
        },
      ]),
      stale_after_ms: 5_000,
    },
    health: block({
      readiness: {
        as_of_ms: NOW,
        quality: "fresh",
        mode: "open_allowed",
        recovery_complete: true,
        trader_ready: true,
        gateway_healthy: true,
        settlement_confirmed: true,
        pending_exit_count: 0,
        unresolved_mapping_count: 0,
        reasons: [],
      },
      pipeline: {
        as_of_ms: NOW,
        quality: "fresh",
        overall_status: "healthy",
        session: "day",
        warning_count: 0,
        critical_count: 0,
        stages: [{ name: "strategy", status: "healthy" }],
        products: [
          {
            product_id: "hc",
            instrument_id: "hc2610",
            status: "healthy",
            strategy_evaluations: 86,
            candidates: 3,
            allowed: 2,
            pending_traces: 0,
            tick_age_seconds: 2,
          },
        ],
      },
    }),
    orders: block(
      trades.map((trade, i) => ({
        ...trade,
        total_volume: trade.volume,
        filled_volume: i === 2 ? 0 : trade.volume,
        avg_fill_price: trade.price,
        status: i === 2 ? "rejected" : "filled",
        reason_code: i === 2 ? "risk_rejected" : "",
      })),
    ),
    trades: block(trades),
  };
}
export function equity(): Archive<EquityPoint> {
  return {
    schema_version: 1,
    data_scope_id: SCOPE,
    ...block(
      Array.from({ length: 181 }, (_, index) => ({
        as_of_ms: NOW - (180 - index) * 60000,
        balance: 200000 + index * 20 + Math.sin(index / 5) * 73,
        available: 180000 + index * 20,
        curr_margin: 20000,
        commission: index * 1.3,
        close_profit: index * 12,
        position_profit: index * 4,
      })),
    ),
    quality: "historical",
  };
}
