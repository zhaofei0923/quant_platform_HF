import { effectiveQuality, finite } from "./format";
import type {
  Block,
  Health,
  MarketQuote,
  Position,
  Quality,
  Snapshot,
  StrategyPosition,
} from "./types";

export type PositionDirection = "long" | "short" | "unknown";
export type Reconciliation = "matched" | "mismatch" | "unbound";

interface BrokerGroup {
  instrumentId: string;
  exchangeId: string;
  direction: PositionDirection;
  quantity: number;
}

export interface RiskMonitorRow {
  id: string;
  instrumentId: string;
  exchangeId: string;
  direction: PositionDirection;
  strategy: StrategyPosition | null;
  strategyQuantity: number | null;
  brokerQuantity: number;
  quote: MarketQuote | undefined;
  reconciliation: Reconciliation;
  multipleStrategies: boolean;
}

export interface PriceDistance {
  points: number;
  percent: number;
  crossed: boolean;
}

export interface ContractPositionRow {
  id: string;
  instrumentId: string;
  exchangeId: string;
  brokerPositions: Position[];
  strategies: StrategyPosition[];
  riskRows: RiskMonitorRow[];
  quote: MarketQuote | undefined;
  brokerLong: number | null;
  brokerShort: number | null;
  brokerUnknown: number | null;
  brokerPositionProfit: number | null;
  brokerMargin: number | null;
  brokerQuality: Quality;
}

export function brokerDirection(value: string): PositionDirection {
  if (["2", "long", "l", "L"].includes(value)) return "long";
  if (["3", "short", "s", "S"].includes(value)) return "short";
  return "unknown";
}

export function directionLabel(value: PositionDirection) {
  return value === "long" ? "多头" : value === "short" ? "空头" : "方向未知";
}

function strategyDirection(net: number): PositionDirection {
  return net > 0 ? "long" : net < 0 ? "short" : "unknown";
}

function groupKey(instrumentId: string, direction: PositionDirection) {
  return `${instrumentId}\u0000${direction}`;
}

export function latestQuotes(quotes: MarketQuote[]) {
  const result = new Map<string, MarketQuote>();
  for (const quote of quotes) {
    const prior = result.get(quote.instrument_id);
    if ((quote.as_of_ms ?? -1) >= (prior?.as_of_ms ?? -1))
      result.set(quote.instrument_id, quote);
  }
  return result;
}

export function buildRiskMonitorRows(
  positions: Position[],
  strategies: StrategyPosition[],
  quotes: MarketQuote[],
): RiskMonitorRow[] {
  const quoteByInstrument = latestQuotes(quotes);
  const brokerGroups = new Map<string, BrokerGroup>();
  const unboundBrokerGroups = new Map<string, BrokerGroup>();
  const addBrokerQuantity = (
    target: Map<string, BrokerGroup>,
    position: Position,
    direction: PositionDirection,
    amount = position.position,
  ) => {
    const key = groupKey(position.instrument_id, direction);
    const prior = target.get(key);
    if (prior) prior.quantity += amount;
    else
      target.set(key, {
        instrumentId: position.instrument_id,
        exchangeId: position.exchange_id,
        direction,
        quantity: amount,
      });
  };
  for (const position of positions) {
    if (!finite(position.position) || position.position <= 0) continue;
    const direction = brokerDirection(position.posi_direction);
    const matchable = position.hedge_flag === "1" && direction !== "unknown";
    addBrokerQuantity(
      matchable ? brokerGroups : unboundBrokerGroups,
      position,
      direction,
    );
  }

  const activeStrategies = strategies.filter(
    (strategy) => finite(strategy.net) && strategy.net !== 0,
  );
  const strategiesByGroup = new Map<string, StrategyPosition[]>();
  for (const strategy of activeStrategies) {
    const direction = strategyDirection(strategy.net);
    const key = groupKey(strategy.instrument_id, direction);
    const group = strategiesByGroup.get(key) ?? [];
    group.push(strategy);
    strategiesByGroup.set(key, group);
  }

  const result: RiskMonitorRow[] = [];
  for (const [key, group] of strategiesByGroup) {
    const direction = strategyDirection(group[0].net);
    const broker = brokerGroups.get(key);
    const strategyTotal = group.reduce(
      (total, strategy) => total + Math.abs(strategy.net),
      0,
    );
    const brokerTotal = broker?.quantity ?? 0;
    const mismatch = Math.abs(strategyTotal - brokerTotal) > 1e-9;
    group.forEach((strategy, index) => {
      result.push({
        id: `strategy:${key}:${strategy.strategy_id}:${index}`,
        instrumentId: strategy.instrument_id,
        exchangeId:
          broker?.exchangeId ??
          quoteByInstrument.get(strategy.instrument_id)?.exchange_id ??
          "",
        direction,
        strategy,
        strategyQuantity: Math.abs(strategy.net),
        brokerQuantity: brokerTotal,
        quote: quoteByInstrument.get(strategy.instrument_id),
        reconciliation: mismatch ? "mismatch" : "matched",
        multipleStrategies: group.length > 1,
      });
    });
  }

  for (const [key, broker] of brokerGroups) {
    const group = strategiesByGroup.get(key) ?? [];
    const strategyTotal = group.reduce(
      (total, strategy) => total + Math.abs(strategy.net),
      0,
    );
    const unboundQuantity = broker.quantity - strategyTotal;
    if (group.length === 0 || unboundQuantity > 1e-9) {
      const prior = unboundBrokerGroups.get(key);
      const amount = group.length === 0 ? broker.quantity : unboundQuantity;
      if (prior) prior.quantity += amount;
      else unboundBrokerGroups.set(key, { ...broker, quantity: amount });
    }
  }

  for (const [key, broker] of unboundBrokerGroups) {
    const relatedStrategies = strategiesByGroup.get(key) ?? [];
    result.push({
      id: `unbound:${key}`,
      instrumentId: broker.instrumentId,
      exchangeId: broker.exchangeId,
      direction: broker.direction,
      strategy: null,
      strategyQuantity: null,
      brokerQuantity: broker.quantity,
      quote: quoteByInstrument.get(broker.instrumentId),
      reconciliation: "unbound",
      multipleStrategies: relatedStrategies.length > 1,
    });
  }

  return result.sort((left, right) => {
    const instrument = left.instrumentId.localeCompare(right.instrumentId);
    if (instrument !== 0) return instrument;
    const direction = left.direction.localeCompare(right.direction);
    if (direction !== 0) return direction;
    if (left.strategy && !right.strategy) return -1;
    if (!left.strategy && right.strategy) return 1;
    return (left.strategy?.strategy_id ?? "").localeCompare(
      right.strategy?.strategy_id ?? "",
    );
  });
}

const validBrokerQuantity = (value: unknown): value is number =>
  finite(value) && Number.isSafeInteger(value) && value >= 0;

const validMoney = (value: unknown): value is number =>
  finite(value) && Math.abs(value) < 1e100;

function brokerSum(
  rows: Position[],
  field: "position" | "position_profit" | "use_margin",
  validate: (value: unknown) => boolean,
  emptyValue: number | null,
): number | null {
  if (rows.length === 0) return emptyValue;
  if (rows.some((row) => !validate(row[field]))) return null;
  const total = rows.reduce((sum, row) => sum + row[field], 0);
  return validate(total) ? total : null;
}

export function buildContractPositionRows(
  snapshot: Snapshot | null,
  now: number,
): ContractPositionRow[] {
  if (!snapshot) return [];
  const positions = snapshot.positions.data;
  const strategies = snapshot.strategy_positions.data.filter(
    (strategy) => strategy.net !== 0,
  );
  const instruments = new Set(
    positions
      .filter((position) => position.position !== 0)
      .map((position) => position.instrument_id),
  );
  for (const strategy of strategies) instruments.add(strategy.instrument_id);
  const quoteByInstrument = latestQuotes(snapshot.markets.data);
  const sourceQuality = effectiveQuality(snapshot.positions, now, 15_000);

  return [...instruments].sort((left, right) => left.localeCompare(right)).map(
    (instrumentId) => {
      const brokerPositions = positions.filter(
        (position) => position.instrument_id === instrumentId,
      );
      const contractStrategies = strategies.filter(
        (strategy) => strategy.instrument_id === instrumentId,
      );
      // Retained records keep their last values and carry their source quality.
      // An absent contract only establishes flatness after a complete fresh query.
      const emptyValue =
        brokerPositions.length > 0 || sourceQuality === "fresh" ? 0 : null;
      const directionQuantity = (direction: PositionDirection) => {
        const rows = brokerPositions.filter(
          (position) => brokerDirection(position.posi_direction) === direction,
        );
        return brokerSum(
          rows,
          "position",
          validBrokerQuantity,
          emptyValue,
        );
      };
      const brokerLong = directionQuantity("long");
      const brokerShort = directionQuantity("short");
      const brokerUnknown = directionQuantity("unknown");
      const brokerPositionProfit = brokerSum(
        brokerPositions,
        "position_profit",
        validMoney,
        emptyValue,
      );
      const brokerMargin = brokerSum(
        brokerPositions,
        "use_margin",
        (value) => validMoney(value) && value >= 0,
        emptyValue,
      );
      const invalidBroker =
        brokerPositions.length > 0 &&
        [brokerLong, brokerShort, brokerUnknown, brokerPositionProfit, brokerMargin]
          .some((value) => value === null);
      const brokerQuality = invalidBroker ? "invalid" : sourceQuality;
      const quote = quoteByInstrument.get(instrumentId);
      const validStrategies = contractStrategies.filter(
        (strategy) => finite(strategy.net) && Number.isSafeInteger(strategy.net),
      );
      const riskRows = buildRiskMonitorRows(
        brokerPositions.filter((position) =>
          validBrokerQuantity(position.position),
        ),
        validStrategies,
        quote ? [quote] : [],
      ).map((row) => ({
        ...row,
        reconciliation:
          row.reconciliation === "matched" &&
          (brokerQuality !== "fresh" ||
            validStrategies.length !== contractStrategies.length)
            ? ("mismatch" as const)
            : row.reconciliation,
      }));
      return {
        id: instrumentId,
        instrumentId,
        exchangeId:
          brokerPositions.find((position) => position.exchange_id)?.exchange_id ??
          quote?.exchange_id ??
          "",
        brokerPositions,
        strategies: contractStrategies,
        riskRows,
        quote,
        brokerLong,
        brokerShort,
        brokerUnknown,
        brokerPositionProfit,
        brokerMargin,
        brokerQuality,
      };
    },
  );
}

export function isExplicitClosedSession(
  health: Block<Health> | undefined,
  now: number,
) {
  if (effectiveQuality(health, now, 10_000) !== "fresh") return false;
  const pipeline = health?.data?.pipeline;
  if (
    !pipeline?.quality ||
    !finite(pipeline.as_of_ms) ||
    effectiveQuality(
      { quality: pipeline.quality, as_of_ms: pipeline.as_of_ms },
      now,
      10_000,
    ) !== "fresh"
  )
    return false;
  return ["closed", "inactive", "off_hours", "outside_session", "non_trading"].includes(
    pipeline.session?.toLowerCase() ?? "",
  );
}

const preventsHistorical = (quality: Quality) =>
  ["invalid", "missing", "incomplete", "catching_up"].includes(quality);

export function marketQuality(
  quote: MarketQuote | undefined,
  block: Block<MarketQuote[]> | undefined,
  now: number,
  explicitlyClosed: boolean,
): Quality {
  const blockState = effectiveQuality(
    block,
    now,
    block?.stale_after_ms ?? 5_000,
  );
  if (preventsHistorical(blockState)) return blockState;
  if (!quote) return "missing";
  const rowState = effectiveQuality(
    {
      quality: quote.quality,
      as_of_ms: quote.as_of_ms ?? 0,
      stale_after_ms: block?.stale_after_ms ?? 5_000,
    },
    now,
    5_000,
  );
  if (preventsHistorical(rowState)) return rowState;
  if (
    explicitlyClosed &&
    finite(quote.last_price) &&
    quote.last_price > 0 &&
    finite(quote.as_of_ms) &&
    quote.as_of_ms > 0
  )
    return "historical";
  return rowState;
}

export function riskQuality(
  strategy: StrategyPosition | null | undefined,
  block: Block<StrategyPosition[]> | undefined,
  now: number,
  explicitlyClosed = false,
): Quality {
  if (!strategy) return "missing";
  if (!finite(strategy.as_of_ms) || strategy.as_of_ms <= 0) return "missing";
  const blockState = effectiveQuality(
    block,
    now,
    block?.stale_after_ms ?? 5_000,
  );
  if (preventsHistorical(blockState)) return blockState;
  if (
    explicitlyClosed &&
    ["fresh", "stale", "historical"].includes(blockState)
  )
    return "historical";
  return effectiveQuality(
    {
      quality: block?.quality ?? "missing",
      as_of_ms: strategy.as_of_ms,
      stale_after_ms: block?.stale_after_ms ?? 5_000,
    },
    now,
    5_000,
  );
}

export function directionalDistance(
  lastPrice: number | null | undefined,
  targetPrice: number | null | undefined,
  direction: PositionDirection,
  target: "stop" | "take_profit",
): PriceDistance | null {
  if (
    !finite(lastPrice) ||
    lastPrice <= 0 ||
    !finite(targetPrice) ||
    targetPrice <= 0 ||
    direction === "unknown"
  )
    return null;
  const points =
    direction === "long"
      ? target === "stop"
        ? lastPrice - targetPrice
        : targetPrice - lastPrice
      : target === "stop"
        ? targetPrice - lastPrice
        : lastPrice - targetPrice;
  return {
    points,
    percent: (points / lastPrice) * 100,
    crossed: points <= 0,
  };
}
