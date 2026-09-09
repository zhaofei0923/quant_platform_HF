import { expect, test, type Page } from "@playwright/test";
import { mkdir } from "node:fs/promises";
import { equity, fixture, NOW, SCOPE } from "./fixtures";
import type { EquityPoint, Snapshot } from "../src/types";

async function setup(
  page: Page,
  value = fixture(),
  options: { priorDay?: boolean; foreignHistory?: boolean; equityPoints?: EquityPoint[] } = {},
) {
  await page.clock.install({ time: NOW });
  await page.route("**/data/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    let body: unknown;
    if (path.endsWith("/current.json")) body = value;
    else if (path.endsWith("/days.json"))
      body = {
        schema_version: 1,
        data_scope_id: SCOPE,
        generated_at_ms: NOW,
        days: [
          {
            trading_day: options.priorDay ? "20260907" : "20260908",
            equity_points: 181,
            order_count: 5,
            trade_count: 5,
          },
        ],
      };
    else if (path.endsWith("/equity.json"))
      body = {
        ...equity(),
        ...(options.equityPoints ? { data: options.equityPoints } : {}),
        data_scope_id: options.foreignHistory ? "foreign" : SCOPE,
      };
    else if (path.endsWith("/orders.json"))
      body = {
        schema_version: 1,
        data_scope_id: SCOPE,
        ...value.orders,
        quality: "historical",
      };
    else
      body = {
        schema_version: 1,
        data_scope_id: SCOPE,
        ...value.trades,
        quality: "historical",
      };
    await route.fulfill({ json: body });
  });
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "账户总览", exact: true }),
  ).toBeVisible();
}
test("desktop overview and chart use labeled examples without console errors", async ({
  page,
}) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await setup(page);
  await expect(page.getByText("¥203,680.50", { exact: true })).toBeVisible();
  await expect(page.getByRole("img", { name: /账户权益曲线/ })).toBeVisible();
  await expect(page.getByText("界面预览 · 示例数据")).toBeVisible();
  await page.screenshot({
    path: "/tmp/quant-dashboard-browser-tests/desktop.png",
    fullPage: true,
  });
  expect(errors).toEqual([]);
});
test("mobile is responsive and can actually log out", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await setup(page);
  await expect(page.getByRole("img", { name: /账户权益曲线/ })).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  await page.screenshot({
    path: "/tmp/quant-dashboard-browser-tests/mobile.png",
    fullPage: true,
  });
  await page.route("**/auth/logout", (route) =>
    route.fulfill({
      contentType: "text/html; charset=utf-8",
      body: "<p>已退出</p>",
    }),
  );
  await page.getByRole("button", { name: "退出登录", exact: true }).click();
  await expect(page).toHaveURL(/\/auth\/logout/);
  await expect(page.getByText("¥203,680.50", { exact: true })).toHaveCount(0);
});
test("contract filtering and rejected orders work", async ({ page }) => {
  await setup(page);
  await page.getByRole("button", { name: "订单与成交", exact: true }).click();
  await page.getByRole("textbox", { name: "筛选合约" }).fill("hc");
  await expect(page.locator("tbody tr")).toHaveCount(1);
  await page.getByRole("textbox", { name: "筛选合约" }).fill("");
  await page.getByRole("button", { name: "委托记录" }).click();
  await page.getByLabel("委托状态").selectOption("rejected");
  await expect(page.locator("tbody tr")).toHaveCount(1);
  await expect(page.locator("tbody")).toContainText("cu2610");
});

function contractPanel(page: Page) {
  return page.locator("section").filter({
    has: page.getByRole("heading", { name: "持仓列表", exact: true }),
  });
}
function contractRows(page: Page) {
  return contractPanel(page).locator(".contract-position-table [data-contract-row]");
}
async function openPositions(page: Page) {
  await page.getByRole("button", { name: "持仓", exact: true }).click();
  await expect(contractPanel(page)).toBeVisible();
}
function multiContractFixture(): Snapshot {
  const value = fixture();
  const broker = value.positions.data[0];
  value.positions.data.push(
    { ...broker, position_date: "2", position: 2, today_position: 0, yd_position: 2,
      long_frozen: 1, use_margin: 8000, position_profit: -200 },
    { ...broker, posi_direction: "3", position: 2, today_position: 2,
      use_margin: 7000, position_profit: 400 },
    { ...broker, instrument_id: "hc2705", use_margin: 5000, position_profit: 50 },
    { ...broker, instrument_id: "rb2701", use_margin: 4500, position_profit: -30 },
  );
  value.strategy_positions.data.push(
    { ...value.strategy_positions.data[0], strategy_id: "Trend", owner_strategy_id: "Trend",
      net: 2, effective_stop: 3260, take_profit: 3340 },
    { ...value.strategy_positions.data[0], strategy_id: "Short", owner_strategy_id: "Short",
      net: -2, effective_stop: 3300, take_profit: 3200 },
  );
  return value;
}
test("unknown broker positions retain the strategy contract without confirming flat", async ({ page }) => {
  const value = fixture();
  value.positions.data = [];
  value.positions.quality = "incomplete";
  await setup(page, value);
  await openPositions(page);
  await expect(contractRows(page)).toHaveCount(1);
  await expect(contractRows(page)).toContainText("hc2610");
  await expect(contractRows(page)).toContainText("待确认");
  await expect(page.getByText("当前空仓", { exact: true })).toHaveCount(0);
});
test("complete empty broker and strategy snapshots are rendered as flat", async ({ page }) => {
  const value = fixture();
  value.positions.data = [];
  value.strategy_positions.data = [];
  await setup(page, value);
  await openPositions(page);
  await expect(contractRows(page)).toHaveCount(0);
  await expect(page.getByText("当前空仓", { exact: true })).toBeVisible();
});
test("strategy-only contract shows broker zero and reconciliation warning", async ({ page }) => {
  const value = fixture();
  value.positions.data = [];
  await setup(page, value);
  await openPositions(page);
  const row = contractRows(page);
  await expect(row).toHaveCount(1);
  await expect(row).toContainText("待核对");
  await expect(row.locator(".contract-quantity strong")).toHaveText("0.00 手");
  await expect(row).not.toContainText("待确认");
});
test("one position panel shows prices, source times and collapsed details", async ({ page }) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  const value = fixture();
  value.markets.data[0].last_price = 3385;
  value.strategy_positions.data[0].effective_stop = 3378.75588567;
  value.strategy_positions.data[0].trailing_stop = 3378.75588567;
  value.strategy_positions.data[0].take_profit = 3470.64779096;
  await setup(page, value);
  await openPositions(page);
  const row = contractRows(page);
  await expect(row).toHaveCount(1);
  for (const text of ["hc2610", "行情", "风控"]) {
    await expect(row).toContainText(text);
  }
  await expect(row.locator(".contract-last-price")).toHaveText("3,385.00");
  await expect(row.locator(".contract-target-value")).toHaveText(["3,378.76", "3,470.65"]);
  await expect(row.locator(".contract-distance")).toHaveText(["距离 6.24 · 0.18%", "距离 85.65 · 2.53%"]);
  await expect(row.locator(".contract-quantity strong")).toHaveText("多 1.00");
  await expect(contractPanel(page)).toContainText("1 个持仓合约 · 每 2 秒更新");
  await expect(page.getByRole("heading", { name: "券商实际持仓", exact: true })).toHaveCount(0);
  await expect(page.getByRole("heading", { name: "策略风控明细", exact: true })).toHaveCount(0);
  const button = row.getByRole("button", { name: "hc2610 详情", exact: true });
  await expect(button).toHaveAttribute("aria-expanded", "false");
  await button.click();
  await expect(button).toHaveAttribute("aria-expanded", "true");
  const details = contractPanel(page).locator('.contract-position-table [data-contract-details="hc2610"]');
  await expect(details).toBeVisible();
  for (const text of ["3,250.00", "3,378.76", "3,470.65", "跟踪止损", "KAMA", "今仓", "多冻／空冻"]) {
    await expect(details).toContainText(text);
  }
  await expect(details.getByText("1.00 手", { exact: true })).toHaveCount(2);
  await expect(details.getByText("1.00／0.00", { exact: true })).toBeVisible();
  await expect(details.getByText("0.00／0.00", { exact: true })).toBeVisible();
  await page.screenshot({ path: "/tmp/quant-dashboard-browser-tests/positions-desktop.png", fullPage: true });
  await button.click();
  await expect(button).toHaveAttribute("aria-expanded", "false");
  expect(errors).toEqual([]);
});
test("multiple strategies and unbound exposure share one contract row", async ({ page }) => {
  const value = fixture();
  value.positions.data[0].position = 4;
  value.positions.data[0].today_position = 4;
  value.strategy_positions.data.push({
    ...value.strategy_positions.data[0], strategy_id: "Trend", owner_strategy_id: "Trend",
    net: 2, initial_stop: 3240, trailing_stop: 3260, effective_stop: 3260, take_profit: 3340,
  });
  await setup(page, value);
  await openPositions(page);
  const row = contractRows(page);
  await expect(row).toHaveCount(1);
  for (const text of ["KAMA", "Trend", "3,270.00", "3,260.00", "3,310.00", "3,340.00", "未绑定策略", "待核对"]) {
    await expect(row).toContainText(text);
  }
  await expect(row.locator(".contract-quantity strong")).toHaveText("多 4.00");
  await expect(row.locator('[data-risk-binding="unbound"]').first()).toContainText("1.00 手");
  await expect(row).toContainText("¥3,921.60");
  await expect(row).toContainText("1,280.00");
});
test("exact contracts, long-short and today-yesterday records have no duplicate main rows", async ({ page }) => {
  await setup(page, multiContractFixture());
  await openPositions(page);
  const rows = contractRows(page);
  await expect(rows).toHaveCount(3);
  await expect(rows.nth(0)).toContainText("hc2610");
  await expect(rows.nth(1)).toContainText("hc2705");
  await expect(rows.nth(2)).toContainText("rb2701");
  await expect(rows.nth(0).locator(".contract-quantity strong")).toHaveText("多 3.00／空 2.00");
  await expect(rows.nth(0)).toContainText("¥18,921.60");
  await expect(rows.nth(0)).toContainText("1,480.00");
  for (const target of ["3,270.00", "3,260.00", "3,300.00", "3,310.00", "3,340.00", "3,200.00"]) {
    await expect(rows.nth(0)).toContainText(target);
  }
  await expect(rows.nth(1)).toContainText("未绑定策略");
  await page.screenshot({ path: "/tmp/quant-dashboard-browser-tests/positions-multiple-desktop.png", fullPage: true });
});
test("non-speculative broker exposure does not inherit a stop", async ({ page }) => {
  const value = fixture();
  value.positions.data[0].hedge_flag = "2";
  await setup(page, value);
  await openPositions(page);
  const row = contractRows(page);
  await expect(row).toHaveCount(1);
  await expect(row).toContainText("KAMA");
  await expect(row).toContainText("待核对");
  const unbound = row.locator('[data-risk-binding="unbound"]');
  await expect(unbound.first()).toBeVisible();
  await expect(unbound.first()).toContainText("未绑定策略");
  await expect(unbound.first()).toContainText("—");
  await expect(unbound.first()).not.toContainText("3,270.00");
});
test("nullable market and risk prices stay unknown", async ({ page }) => {
  const value = fixture();
  value.markets.data[0].last_price = null;
  value.strategy_positions.data[0].effective_stop = null;
  value.strategy_positions.data[0].stop_kind = null;
  value.strategy_positions.data[0].take_profit = null;
  await setup(page, value);
  await openPositions(page);
  const row = contractRows(page);
  await expect(row.locator(".contract-last-price")).toHaveText("—");
  await expect(row.locator(".contract-target-value")).toHaveText(["—", "—"]);
  await expect(row).not.toContainText("NaN");
});
test("closed sessions preserve independent quote and risk quality", async ({ page }) => {
  const value = fixture();
  value.markets.data[0].as_of_ms = NOW - 6_000;
  value.health.data.pipeline!.session = "unknown";
  await setup(page, value);
  await openPositions(page);
  const sources = contractRows(page).locator(".contract-sources");
  await expect(sources).toContainText("数据已过期");
  value.health.data.pipeline!.session = "closed";
  value.health.data.pipeline!.overall_status = "inactive";
  await expect(sources.getByText("休市最后值", { exact: true })).toHaveCount(2, { timeout: 7_000 });
  value.health.data.pipeline!.as_of_ms = NOW - 11_000;
  await expect(sources).toContainText("数据已过期", { timeout: 7_000 });
  await expect(sources.getByText("休市最后值", { exact: true })).toHaveCount(0);
});
test("polling updates targets and keeps details open until instance changes", async ({ page }) => {
  const value = fixture();
  await setup(page, value);
  await openPositions(page);
  const row = contractRows(page);
  const button = row.getByRole("button", { name: "hc2610 详情", exact: true });
  await button.click();
  value.markets.data[0].last_price = 3288;
  value.strategy_positions.data[0].effective_stop = 3275;
  await expect(row.locator(".contract-last-price")).toHaveText("3,288.00", { timeout: 7_000 });
  await expect(row).toContainText("3,275.00", { timeout: 7_000 });
  await expect(row).toHaveCount(1);
  await expect(button).toHaveAttribute("aria-expanded", "true");
  value.instance_id = "replacement-instance";
  await expect(button).toHaveAttribute("aria-expanded", "false", { timeout: 7_000 });
});
test("390px multi-contract cards and details do not overflow", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await setup(page, multiContractFixture());
  await openPositions(page);
  const panel = contractPanel(page);
  await expect(panel.locator(".contract-position-table")).toBeHidden();
  const cards = panel.locator(".contract-position-card");
  await expect(cards).toHaveCount(3);
  await expect(cards.first()).toBeVisible();
  for (const text of ["当前止损", "行情", "风控", "KAMA", "Trend", "Short"]) {
    await expect(cards.first()).toContainText(text);
  }
  await expect(cards.first().locator(".contract-last-price")).toHaveText("3,282.00");
  await expect(cards.first().locator(".contract-quantity strong")).toHaveText("多 3.00／空 2.00");
  for (const target of await cards.first().locator(".contract-target-value").allTextContents()) {
    expect(target).toMatch(/^\d[\d,]*\.\d{2}$/);
  }
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page.screenshot({ path: "/tmp/quant-dashboard-browser-tests/positions-mobile.png", fullPage: true });
  await cards.first().getByRole("button", { name: "hc2610 详情", exact: true }).click();
  const details = cards.first().locator('[data-contract-details="hc2610"]');
  await expect(details).toBeVisible();
  await expect(details.getByText("2.00 手", { exact: true })).toHaveCount(4);
  await expect(details.getByText("0.00／2.00", { exact: true })).toBeVisible();
  await expect(details.getByText("1.00／0.00", { exact: true })).toHaveCount(2);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page.screenshot({ path: "/tmp/quant-dashboard-browser-tests/positions-mobile-details.png", fullPage: true });
});
test("old sources become stale even while HTTP requests succeed", async ({
  page,
}) => {
  const value = fixture();
  value.account.as_of_ms = NOW - 20_000;
  value.health.as_of_ms = NOW - 20_000;
  value.health.data.readiness!.as_of_ms = NOW - 20_000;
  await setup(page, value);
  await expect(page.getByRole("status")).toContainText("数据已过期");
  await page.getByRole("button", { name: "策略与运行", exact: true }).click();
  const recovery = page
    .locator("section")
    .filter({ has: page.getByRole("heading", { name: "交易许可与恢复" }) });
  await expect(recovery).toContainText("数据已过期");
  await expect(recovery.getByText("已完成", { exact: true })).toHaveCount(0);
});
test("today never shows a previous trading day curve", async ({ page }) => {
  await setup(page, fixture(), { priorDay: true });
  await expect(page.getByText("权益曲线等待首笔采样")).toBeVisible();
  await expect(page.getByRole("img", { name: /账户权益曲线/ })).toHaveCount(0);
});
test("a foreign account history is refused", async ({ page }) => {
  await setup(page, fixture(), { foreignHistory: true });
  await expect(
    page.getByText("部分历史暂不可用，缺失区间未补造"),
  ).toBeVisible();
  await expect(page.getByRole("img", { name: /账户权益曲线/ })).toHaveCount(0);
});
test("production environment is explicit", async ({ page }) => {
  const value = fixture();
  value.environment = "prod";
  await setup(page, value);
  await expect(page.getByText("实盘环境", { exact: true })).toBeVisible();
  await expect(page.locator(".environment")).toHaveClass(/environment-live/);
});
test("missing data has no fabricated balances", async ({ page }) => {
  await page.route("**/data/v1/**", (route) =>
    route.fulfill({ status: 404, body: "" }),
  );
  await page.goto("/");
  await expect(page.getByRole("status")).toContainText("数据尚未发布");
  await expect(page.locator(".metric strong")).toHaveText([
    "—",
    "—",
    "—",
    "—",
    "—",
    "—",
  ]);
  await mkdir("/tmp/quant-dashboard-browser-tests", { recursive: true });
  await page.screenshot({
    path: "/tmp/quant-dashboard-browser-tests/missing.png",
    fullPage: true,
  });
});
test("expired session removes sensitive data and navigates to login", async ({
  page,
}) => {
  await page.route("**/data/v1/**", (route) =>
    route.fulfill({ status: 401, contentType: "application/json", body: "{}" }),
  );
  await page.route("**/auth/**", (route) =>
    route.fulfill({
      contentType: "text/html; charset=utf-8",
      body: "<h1>登录</h1>",
    }),
  );
  await page.goto("/");
  await expect(page).toHaveURL(/\/auth\//);
  await expect(page.getByRole("heading", { name: "登录" })).toBeVisible();
});
test("corrupt rows are rejected and corrected data recovers without reload", async ({
  page,
}) => {
  const value = fixture();
  const original = value.orders.data[0].instrument_id;
  (value.orders.data[0] as unknown as Record<string, unknown>).instrument_id =
    null;
  await setup(page, value);
  await expect(page.getByRole("status")).toContainText("交易数据结构异常");
  value.orders.data[0].instrument_id = original;
  await expect(page.getByText("¥203,680.50", { exact: true })).toBeVisible({
    timeout: 7000,
  });
});

const technologyViews = [
  { name: "desktop", width: 1548, height: 1016 },
  { name: "wide-desktop", width: 1920, height: 1080 },
  { name: "mobile", width: 390, height: 844 },
] as const;
const longStrategy = "KAMA_跨周期趋势跟踪策略_持仓归属与移动止损_长期组合";
const longRejectReason = "风险检查拒绝委托：当前合约可用保证金不足，且本次申报超出单合约持仓限制。请核对账户资金、已有挂单与策略风控状态，拒单原因应完整保留供查询。";

for (const viewport of technologyViews) {
  test(`technology theme ${viewport.name}: four pages remain readable with large and long values`, async ({ page }) => {
    const errors: string[] = [];
    page.on("pageerror", (error) => errors.push(error.message));
    page.on("console", (message) => {
      if (message.type() === "error") errors.push(message.text());
    });
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    const value = multiContractFixture();
    value.account.data!.balance = 1_234_567_890.5;
    value.strategy_positions.data[0].strategy_id = longStrategy;
    value.strategy_positions.data[0].owner_strategy_id = longStrategy;
    value.orders.data[2].strategy_id = longStrategy;
    value.orders.data[2].reject_message = longRejectReason;
    value.health.data.readiness!.reasons = ["仅允许平仓：持仓对账尚未完成，等待风险状态确认。"];
    value.health.data.readiness!.mode = "close_only";
    await setup(page, value);
    await expect(page).toHaveTitle(/QUANT.*量化交易看板/);
    await expect(page).toHaveURL("http://127.0.0.1:5173/");
    await mkdir("/tmp/quant-dashboard-browser-tests", { recursive: true });
    const pages = [
      { id: "overview", nav: "总览", title: "账户总览" },
      { id: "positions", nav: "持仓", title: "账户持仓" },
      { id: "orders", nav: "订单与成交", title: "交易记录" },
      { id: "strategy", nav: "策略与运行", title: "策略与运行" },
    ];
    for (const destination of pages) {
      await page.getByRole("button", { name: destination.nav, exact: true }).click();
      await expect(page.getByRole("heading", { name: destination.title, level: 1, exact: true })).toBeVisible();
      const header = page.locator(".page-header");
      await expect(header).toContainText("示例数据");
      await expect(header).toContainText("2026-09-08");
      await expect(header).toContainText(/更新时间\s*14:32:/);
      if (destination.id === "overview") {
        await expect(page.getByText("¥1,234,567,890.50", { exact: true })).toBeVisible();
        expect(await page.locator(".metric:first-child strong").evaluate((node) =>
          node.getBoundingClientRect().height <= parseFloat(getComputedStyle(node).lineHeight) + 1,
        )).toBe(true);
        await expect(page.getByRole("img", { name: /账户权益曲线/ })).toBeVisible();
      } else if (destination.id === "positions") {
        const rows = viewport.name === "mobile"
          ? contractPanel(page).locator(".contract-position-card")
          : contractRows(page);
        await expect(rows).toHaveCount(3);
        await expect(rows.first()).toContainText(longStrategy);
        expect(await rows.first().locator(".contract-last-price").evaluate((node) =>
          node.getClientRects().length === 1,
        )).toBe(true);
      } else if (destination.id === "orders") {
        await page.getByRole("button", { name: "委托记录", exact: true }).click();
        await page.getByLabel("委托状态").selectOption("rejected");
        await expect(page.locator("tbody tr")).toHaveCount(1);
        await expect(page.getByText(longRejectReason, { exact: true })).toBeVisible();
      } else {
        await expect(page.getByText("只允许平仓", { exact: true })).toBeVisible();
        await expect(page.locator(".incident-list")).toContainText(value.health.data.readiness!.reasons![0]);
      }
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth), destination.id).toBe(true);
      await expect(page.locator("vite-error-overlay, nextjs-portal, #webpack-dev-server-client-overlay")).toHaveCount(0);
      await page.screenshot({
        path: `/tmp/quant-dashboard-browser-tests/tech-${viewport.name}-${destination.id}.png`,
        fullPage: true,
      });
    }
    expect(errors).toEqual([]);
  });
}

test("reduced motion keeps interactions usable and history refresh preserves the chart canvas", async ({ page }) => {
  await page.emulateMedia({ reducedMotion: "reduce" });
  await setup(page);
  const chart = page.getByRole("img", { name: /账户权益曲线/ });
  await expect(chart.locator("canvas")).toBeVisible();
  const originalCanvas = await chart.locator("canvas").elementHandle();
  expect(originalCanvas).not.toBeNull();
  await Promise.all([
    page.waitForResponse((response) => response.url().endsWith("/equity.json")),
    page.clock.fastForward(60_000),
  ]);
  await page.clock.runFor(100);
  await expect(chart.locator("canvas")).toBeVisible();
  expect(await originalCanvas!.evaluate((canvas) => canvas.isConnected && canvas === document.querySelector(".equity-chart canvas"))).toBe(true);
  await Promise.all([
    page.waitForResponse((response) => response.url().endsWith("/equity.json")),
    page.getByRole("button", { name: "7天", exact: true }).click(),
  ]);
  await expect(page.getByRole("button", { name: "7天", exact: true })).toHaveAttribute("aria-pressed", "true");
  await expect(chart.locator("canvas")).toBeVisible();
  await openPositions(page);
  await contractRows(page).getByRole("button", { name: "hc2610 详情", exact: true }).click();
  await expect(contractPanel(page).locator('.contract-position-table [data-contract-details="hc2610"]')).toBeVisible();
  const durations = await page.locator(".nav-item.selected, .panel, .contract-detail-toggle").evaluateAll((elements) =>
    elements.flatMap((element) => {
      const style = getComputedStyle(element);
      return [style.transitionDuration, style.animationDuration].flatMap((value) => value.split(",").map(parseFloat));
    }),
  );
  expect(durations.length).toBeGreaterThan(0);
  expect(durations.every((seconds) => seconds <= 0.00001)).toBe(true);
});

for (const viewport of [technologyViews[0], technologyViews[2]]) {
  test(`compressed equity timeline ${viewport.name}: real timestamps and dashed gaps survive range changes`, async ({ page }) => {
    const errors: string[] = [];
    let coreModuleUrl = "";
    page.on("pageerror", (error) => errors.push(error.message));
    page.on("console", (message) => {
      if (message.type() === "error") errors.push(message.text());
    });
    // Discover the module the actual page loaded; do not expose chart internals in production.
    page.on("response", (response) => {
      if (/\/echarts_core\.js(?:\?|$)/.test(response.url())) coreModuleUrl = response.url();
    });
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    const timestamps = [
      "2026-09-07T20:45:00+08:00", "2026-09-07T20:46:00+08:00",
      "2026-09-07T20:47:30+08:00", "2026-09-07T20:51:00+08:00",
      "2026-09-07T23:04:00+08:00", "2026-09-07T23:05:00+08:00",
      "2026-09-08T08:45:00+08:00", "2026-09-08T08:46:00+08:00",
    ].map((timestamp) => Date.parse(timestamp));
    const balances = [203000, 203050, 203075, 203040, 203200, 203180, 203400, 203450];
    const equityPoints = timestamps.map((as_of_ms, index) => ({
      ...equity().data[0], as_of_ms, balance: balances[index],
    }));
    await setup(page, fixture(), { equityPoints });
    const chart = page.getByRole("img", { name: /账户权益曲线/ });
    const panel = page.locator(".chart-panel");
    await expect(chart.locator("canvas")).toBeVisible();
    await expect(chart).toHaveAttribute("aria-label", /8 个真实采样点/);
    expect(coreModuleUrl).not.toBe("");
    const inspection = await page.evaluate(async (moduleUrl) => {
      const { getInstanceByDom } = await import(moduleUrl);
      const instance = getInstanceByDom(document.querySelector(".equity-chart"));
      const option = instance.getOption();
      const series = option.series[0];
      const keys: string[] = option.xAxis[0].data;
      const values: Array<number | null> = series.data;
      return {
        axisType: option.xAxis[0].type,
        keys,
        values,
        connectNulls: series.connectNulls,
        gapStyle: series.markLine.lineStyle.type,
        gapEndpoints: series.markLine.data.map((pair: Array<{ coord: [string, number] }>) => pair.map((point) => point.coord)),
        pixels: keys.map((key, index) => instance.convertToPixel({ seriesIndex: 0 }, [key, values[index] ?? 203000])[0] as number),
        overnightTooltip: option.tooltip[0].formatter([{ dataIndex: 9 }]) as string,
        missingTooltip: option.tooltip[0].formatter([{ dataIndex: 4 }]) as string,
        gapTooltip: option.tooltip[0].formatter([{ dataIndex: 8 }]) as string,
      };
    }, coreModuleUrl);
    expect(inspection.axisType).toBe("category");
    expect(inspection.values).toEqual([203000, 203050, 203075, null, 203040, null, 203200, 203180, null, 203400, 203450]);
    expect(inspection.keys.filter((key) => !key.startsWith("gap:"))).toEqual(timestamps.map(String));
    expect(inspection.connectNulls).toBe(false);
    expect(inspection.gapStyle).toBe("dashed");
    expect(inspection.gapEndpoints).toEqual([
      [[String(timestamps[2]), balances[2]], [String(timestamps[3]), balances[3]]],
      [[String(timestamps[3]), balances[3]], [String(timestamps[4]), balances[4]]],
      [[String(timestamps[5]), balances[5]], [String(timestamps[6]), balances[6]]],
    ]);
    const minuteWidth = inspection.pixels[1] - inspection.pixels[0];
    expect(minuteWidth).toBeGreaterThan(0);
    expect((inspection.pixels[2] - inspection.pixels[1]) / minuteWidth).toBeCloseTo(1, 5);
    expect((inspection.pixels[4] - inspection.pixels[2]) / minuteWidth).toBeCloseTo(2, 5);
    expect((inspection.pixels[9] - inspection.pixels[7]) / minuteWidth).toBeCloseTo(2, 5);
    expect(inspection.overnightTooltip).toContain("09/08 08:45 北京时间");
    expect(inspection.overnightTooltip).toContain("¥203,400.00");
    expect(inspection.overnightTooltip).toContain("09/07 23:05 → 09/08 08:45");
    expect(inspection.missingTooltip).toContain("09/07 20:47 → 09/07 20:51");
    expect(inspection.missingTooltip).toContain("前段无采样，时间间隔已压缩");
    expect(inspection.gapTooltip).toBe("无采样间隔（已压缩）");
    await mkdir("/tmp/quant-dashboard-browser-tests", { recursive: true });
    for (const point of [{ index: 9, name: "overnight" }, { index: 4, name: "missing" }]) {
      await panel.evaluate((node) => node.scrollIntoView({ block: "center" }));
      const pixel = await page.evaluate(async ({ moduleUrl, index }) => {
        const { getInstanceByDom } = await import(moduleUrl);
        const instance = getInstanceByDom(document.querySelector(".equity-chart"));
        const option = instance.getOption();
        return instance.convertToPixel({ seriesIndex: 0 }, [option.xAxis[0].data[index], option.series[0].data[index]]) as [number, number];
      }, { moduleUrl: coreModuleUrl, index: point.index });
      const bounds = await chart.boundingBox();
      expect(bounds).not.toBeNull();
      await page.mouse.move(bounds!.x + pixel[0], bounds!.y + pixel[1]);
      await page.clock.runFor(120);
      await panel.screenshot({ path: `/tmp/quant-dashboard-browser-tests/equity-gap-${viewport.name}-${point.name}.png` });
    }
    await page.getByRole("button", { name: "7天", exact: true }).click();
    await expect(page.getByRole("button", { name: "7天", exact: true })).toHaveAttribute("aria-pressed", "true");
    await expect(chart.locator("canvas")).toBeVisible();
    await expect(chart).toHaveAttribute("aria-label", /8 个真实采样点/);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
    await expect(page.locator("vite-error-overlay, nextjs-portal, #webpack-dev-server-client-overlay")).toHaveCount(0);
    expect(errors).toEqual([]);
  });
}
