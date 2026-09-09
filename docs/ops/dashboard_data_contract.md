# Personal read-only dashboard data contract (public v1, private v1/v2)

`dashboard_publish_cli` is an independent C++17 observer. It does not connect to CTP,
place/cancel orders, change trading permission, or write to the selected trading runtime.
Nginx serves only the public files below, behind authentication.

## Invocation and identity binding

```text
dashboard_publish_cli --source-dir /absolute/selected/recovery/root \
  --identity-file /etc/quant-dashboard/identity.json \
  --output-dir /var/lib/quant-dashboard-public/data/v1 \
  --state-dir /var/lib/quant-dashboard-publisher \
  --watch-seconds 1 --retention-days 90 --strategy-stale-after-ms 5000
```

`--watch-seconds 0` performs one publication. Wall time cannot be overridden by a
CLI argument. Tests inject a clock into the `Publisher::PublishOnce` C++ interface.
Retention is 1–90 calendar days, default 90. The selected recovery root and all
inputs must be resolved from the actual trading instance, using the existing
`runtime_paths_cli --config PATH` procedure in its already configured environment.
The dashboard does not load `.env` or infer a default account.

The private descriptor is:

```json
{
  "schema_version": 1,
  "identity": {
    "environment": "simnow",
    "broker_id": "PRIVATE_BROKER_ID",
    "account_id": "PRIVATE_ACCOUNT_ID",
    "instance_id": "main"
  },
  "account_alias": "个人账户",
  "instance_alias": "云端主实例",
  "recovery_root": "/absolute/selected/recovery/root",
  "paths": {
    "private_snapshot": "/absolute/selected/recovery/root/monitor/dashboard_private.json",
    "readiness": "/absolute/selected/recovery/root/monitor/readiness.json",
    "pipeline_health": "/absolute/selected/recovery/root/monitor/pipeline_health.json",
    "wal": "/absolute/selected/recovery/root/wal/events.wal"
  }
}
```

`paths` entries may be omitted to use the shown suffixes. Overrides must remain
inside the selected root. All paths are absolute; symlink components and overlapping
source/public/private-state directories are rejected. The descriptor and private
snapshot must match all four identity fields. Checkpoints are separately bound to
the same identity. The public aliases must not contain the real account ID.

The publisher accepts private snapshot schema 1 and 2 while keeping every public
file at schema 1. Private schema 1 remains valid for account, position, health and
WAL publication, but its `markets` and `strategy_positions` public blocks are
explicitly `missing`. It does not recover live values by scanning market CSV or
strategy persistence files. Private schema 2 adds two identity-bound blocks:

```json
{
  "schema_version": 2,
  "market_quotes": {
    "quality": "ok",
    "source": "ctp_market_callback",
    "as_of_ms": 1788746400000,
    "trading_day": "20260907",
    "data": [
      {
        "instrument_id": "hc2610",
        "exchange_id": "SHFE",
        "trading_day": "20260907",
        "last_price": 3212.0,
        "bid_price_1": 3211.0,
        "ask_price_1": 3213.0,
        "volume": 20,
        "open_interest": 100,
        "as_of_ms": 1788746400000
      }
    ]
  },
  "strategy_risk": {
    "quality": "ok",
    "source": "strategy_engine",
    "as_of_ms": 1788746400000,
    "trading_day": "20260907",
    "data": [
      {
        "strategy_id": "outer_strategy",
        "owner_strategy_id": "kama_hc",
        "instrument_id": "hc2610",
        "net": -2,
        "avg_open": 3210.0,
        "initial_stop": 3235.0,
        "trailing_stop": 3225.0,
        "effective_stop": 3225.0,
        "stop_kind": "trailing",
        "take_profit": 3150.0,
        "as_of_ms": 1788746400000
      }
    ]
  }
}
```

Both additions use the same private block envelope as account and positions,
including `generation`, `last_attempt_ms`, and `error_code`. Price fields may be
JSON `null`. `strategy_id` is the outer StrategyEngine entry;
`owner_strategy_id` is the optional atomic strategy that owns the displayed risk
levels. `effective_stop` and `stop_kind` are supplied together by that owner and
are never inferred by the publisher or browser.

For non-null values, public prices must be finite, greater than zero and below
`1e100`. Strategy `net` must be a nonzero signed 32-bit integer. A non-null
`effective_stop` must exactly equal the `initial_stop` or `trailing_stop` selected
by `stop_kind`; an inconsistent risk row is rejected. Quote `volume` and
`open_interest` must be nonnegative integers within JavaScript's exact-integer
range. Non-null row timestamps must be positive exact integers, within the same
safe range, and no more than two seconds in the future. A null strategy timestamp
remains null and makes its block incomplete. Invalid newer quote rows cannot
replace an older valid quote for the same contract.

Each private state directory receives a random 128-bit `data_scope_id`, persisted
across restarts. All public responses carry it; it is not derived from the account
number. The output directory has a hidden ownership marker and writer lock; another
state directory cannot reuse it. Nginx must never expose these hidden files. When
changing account/instance, provision a separate private state and public directory.

## Public files

Only these paths are public:

| File | Contents |
| --- | --- |
| `current.json` | Latest observed account, full broker positions, strategy net positions, health, quotes, and recent orders/trades |
| `days.json` | Available trading days and equity/order/trade counts |
| `days/YYYYMMDD/equity.json` | Actual sampled account equity, oldest sample first |
| `days/YYYYMMDD/orders.json` | One merged row per observed order, newest first |
| `days/YYYYMMDD/trades.json` | Deduplicated observed exchange fills, newest first |

Every file has `schema_version: 1` and `data_scope_id`. The current response also
has `generated_at_ms`, `environment`, `account_alias`, `instance_id` (the public
instance alias), and `trading_day`. A fresh publication timestamp is not evidence
that its source data is fresh.

Each current block and each archive has this shape:

```json
{
  "as_of_ms": 1788746400000,
  "quality": "fresh",
  "source": "ctp",
  "trading_day": "20260907",
  "data": {}
}
```

Quality values are `fresh`, `stale`, `missing`, `invalid`, `incomplete`,
`catching_up`, or `historical`. `historical` means saved observations, not current
broker state. Unknown timestamps/numbers are `null`. List blocks always use arrays;
an empty array means flat/no records only when quality and query completeness
support that interpretation. Account/health data may be `null` when identity is
unverified. Never render missing, invalid, or failed data as a green zero.

The browser must reject a response with a different `data_scope_id` from the
current selection, clear prior data when the scope changes or authentication is
lost, and separately mark frozen publication timestamps when polling stops.

## Block fields

- `account.data`: `balance`, `available`, `curr_margin`, `frozen_margin`,
  `frozen_cash`, `frozen_commission`, `commission`, `close_profit`, `position_profit`.
  Freshness limit is 15 seconds from the broker callback timestamp. Failed queries
  retain prior values and source timestamp with `incomplete` quality.
- `positions.data[]`: `instrument_id`, `exchange_id`, `posi_direction`, `hedge_flag`,
  `position_date`, `position`, `today_position`, `yd_position`, `long_frozen`,
  `short_frozen`, `open_volume`, `close_volume`, `open_cost`, `position_cost`,
  `use_margin`, `position_profit`, `close_profit`. Preserve separate direction,
  hedge and date rows. A complete empty broker batch is a valid empty array;
  an unsuccessful/incomplete batch is not proof of being flat.
- `strategy_positions.data[]`: `strategy_id`, optional `owner_strategy_id`,
  `instrument_id`, `net`, `avg_open`, `initial_stop`, `take_profit`,
  `trailing_stop`, `effective_stop`, `stop_kind`, `as_of_ms`. Numeric risk fields
  and `stop_kind` may be `null`; allowed non-null kinds are `initial` and
  `trailing`. These are strategy beliefs, not broker account positions. Separate
  nonzero strategies on the same contract remain separate rows. The block exposes
  `stale_after_ms`, default 5000.
- `health.data.readiness`: `mode`, `recovery_complete`, `trader_ready`,
  `gateway_healthy`, `settlement_confirmed`, `pending_exit_count`,
  `unresolved_mapping_count`, `generation`, and fixed identifier `reasons`.
- `health.data.pipeline`: `overall_status`, `session`, `trading_day`,
  `warning_count`, `critical_count`, `generation`, `stages[]` (`name/status/reason`),
  and `products[]` (`product_id/instrument_id/exchange_id/status/reason`, strategy
  evaluations, candidates, allowed, pending traces, tick age/delay). Both nested
  health objects have their own `quality` and `as_of_ms`; freshness is 10 seconds.
  Only pipeline schema v3 is authoritative. No fallback to historical login logs.
  A heartbeat predating the current private writer's startup is stale, even if
  its absolute age is still within 10 seconds.
- `markets.data[]`: optional `product_id`, `instrument_id`, `exchange_id`,
  nullable `last_price`, `bid_price_1`, `ask_price_1`, `volume`, `open_interest`,
  plus row-level `quality` and nullable `as_of_ms`. The source is the private
  callback cache; the publisher does not establish subscriptions or scan recorded
  CSV. Only instruments appearing in a nonzero broker position or nonzero strategy
  row are emitted. A requested instrument without a quote receives a row of null
  values and `missing` quality. Quotes and strategy risk become stale after five
  seconds; both public blocks expose `stale_after_ms: 5000`. Quote age alone does
  not determine whether the system is healthy during
  a closed session; use pipeline session/status.
- `orders.data[]`: `order_id`, product instrument/exchange and strategy IDs,
  `side`, `offset`, `status`, `total_volume`, `filled_volume`, `avg_fill_price`,
  `as_of_ms`, `attribution`, `reject_code`, `reject_message`. `order_id` is opaque;
  raw client IDs and rejection text are not public. Rejection explanations use a
  fixed allowlist derived from explicit reason/status/error-code tokens:
  `risk_limit`, `opening_blocked`, `rate_limited`, `invalid_price_or_volume`,
  `ctp_rejected`, or `unknown_rejection`. The last category explicitly says the
  specific cause was not recognized and needs trading-side inspection. Original
  text is never copied. Terminal order states do not regress to active states on replay.
- `trades.data[]`: `trade_id`, `order_id`, instrument/exchange and strategy IDs,
  `side`, `offset`, `volume`, `price`, `as_of_ms`, `attribution`. Trade keys include
  account-bound scope, trading day, exchange, and trade ID. Conflicting duplicates
  are flagged rather than counted again. Unattributed fills stay unattributed.

Sides are `buy/sell/unknown`; offsets are
`open/close/close_today/close_yesterday/unknown`; order states are
`new/submitted/partially_filled/filled/canceled/rejected/unknown`. Current orders
and trades are capped at 200 newest rows per current trading day and expose
`recent_limit: 200`; archive files contain the retained day's observed rows.

`days.json` has `generated_at_ms` and `days[]`, each with `trading_day`,
`equity_points`, `order_count`, `trade_count`. The equity archive's `data[]` contains
`as_of_ms`, `balance`, `available`, `curr_margin`, `commission`, `close_profit`, and
`position_profit`. It samples a fresh source at most once per source minute, starts
with deployment, preserves gaps, and never fills missing history with zero.

## Accuracy and failure behavior

The account values are the CTP fields, not strategy PnL or audited net investment
returns. Do not derive returns/drawdown from balance changes: the current snapshot
does not expose deposits, withdrawals, or a verified opening balance. WAL-export
fee estimates are not used as actual commissions. Trading-day labels are required
for trade history; calendar timestamps are never substituted for a missing trading
day. Historical and current records share the same identity scope.

The observer processes at most 8 MiB of new WAL bytes per cycle, bounds individual
record memory, ignores unfinished tail records, checkpoints its inode/device and
byte cursor, and drains a rotated predecessor where still available. Schema v4
records require the repository CRC, exact account and broker identity, supported
record fields, stream identity and contiguous sequence. A submit mapping has the
production format's account binding without a broker field. Invalid records do not
enter orders/trades. Legacy records can be observed from the identity-bound root
but permanently mark affected coverage incomplete. The current selection uses the
latest source trading day; account sampling always retains the account block's own
day, so a lagging account query cannot move old equity into a new trading day.
A changed
cursor neighborhood or truncation resets the cursor while keeping deduplicated
history; possible source gaps remain visible as incomplete coverage. This is an
incremental monitoring view, not a replacement for the trading engine's full WAL
recovery validation.

Private daily state is atomically persisted before the cursor checkpoint; replay
after an interrupted publication is idempotent. Public JSON is replaced atomically
and uses mode 0640; private state uses mode 0600. The public parent directories
must retain the web reader group through setgid. Only known generated filenames
under the validated day directory are removed by retention. Publication failure
exits the observer for systemd restart and leaves the last complete public files;
the trading process and its permissions are unaffected.

The standalone CTest target `dashboard_publish_cli_test` exercises real temporary
files, rotation/truncation/restart, identity isolation, failed and empty snapshots,
private v1/v2 behavior, live-contract filtering, nullable prices, five-second
freshness, concurrent readers, bounded ingestion, and minute sampling.
