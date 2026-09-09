import { readFile } from "node:fs/promises";
import { afterEach, describe, expect, it, vi } from "vitest";
import { getArchive, getCurrent, getDays } from "./api";

const root = process.env.DASHBOARD_CONTRACT_FIXTURE;
afterEach(() => vi.unstubAllGlobals());
describe.skipIf(!root)("Actual C++ publisher to browser contract", () => {
  for (const variant of ["", "/close-only", "/missing"]) {
    it(`reads the independently generated ${variant || "full"} publication`, async () => {
      vi.stubGlobal(
        "fetch",
        vi.fn(async (path: string) => {
          const bytes = await readFile(`${root}${variant}${path}`, "utf8");
          return new Response(bytes, {
            headers: { "content-type": "application/json" },
          });
        }),
      );
      const signal = new AbortController().signal;
      const current = await getCurrent(signal);
      expect(current.data_scope_id).toBeTruthy();
      const days = await getDays(signal, current.data_scope_id);
      for (const day of days) {
        for (const kind of ["equity", "orders", "trades"] as const) {
          const result = await getArchive(
            day.trading_day,
            kind,
            signal,
            current.data_scope_id,
          );
          expect(Array.isArray(result.data)).toBe(true);
        }
      }
    });
  }
});
