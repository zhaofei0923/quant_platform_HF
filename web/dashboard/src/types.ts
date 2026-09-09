export type Quality =
  | "fresh"
  | "stale"
  | "missing"
  | "invalid"
  | "incomplete"
  | "catching_up"
  | "historical";
export interface Block<T> {
  data: T;
  as_of_ms: number | null;
  quality: Quality;
  source: string;
  trading_day: string;
  stale_after_ms?: number;
}
export interface Account {
  balance: number;
  available: number;
  curr_margin: number;
  frozen_margin: number;
  frozen_cash: number;
  frozen_commission: number;
  commission: number;
  close_profit: number;
  position_profit: number;
}
export interface Position {
  instrument_id: string;
  exchange_id: string;
  posi_direction: string;
  hedge_flag: string;
  position_date: string;
  position: number;
  today_position: number;
  yd_position: number;
  long_frozen: number;
  short_frozen: number;
  open_cost: number;
  position_cost: number;
  use_margin: number;
  position_profit: number;
  close_profit: number;
}
export interface StrategyPosition {
  strategy_id: string;
  owner_strategy_id?: string;
  instrument_id: string;
  net: number;
  avg_open: number | null;
  initial_stop: number | null;
  take_profit: number | null;
  trailing_stop: number | null;
  effective_stop: number | null;
  stop_kind: "initial" | "trailing" | null;
  as_of_ms: number | null;
}
export interface MarketQuote {
  product_id?: string;
  instrument_id: string;
  exchange_id: string;
  last_price: number | null;
  bid_price_1: number | null;
  ask_price_1: number | null;
  volume: number | null;
  open_interest: number | null;
  as_of_ms: number | null;
  quality: Quality;
}
export interface Order {
  order_id: string;
  instrument_id: string;
  exchange_id: string;
  strategy_id: string;
  side: string;
  offset: string;
  status: string;
  total_volume: number;
  filled_volume: number;
  avg_fill_price: number;
  as_of_ms: number;
  attribution: string;
  reason_code?: string;
  reject_code?: string;
  reject_message?: string;
}
export interface Trade {
  trade_id: string;
  order_id: string;
  instrument_id: string;
  exchange_id: string;
  strategy_id: string;
  side: string;
  offset: string;
  volume: number;
  price: number;
  as_of_ms: number;
  attribution: string;
}
export interface SourceStatus {
  as_of_ms?: number;
  quality?: Quality;
}
export interface Readiness extends SourceStatus {
  mode?: string;
  recovery_complete?: boolean;
  trader_ready?: boolean;
  gateway_healthy?: boolean;
  settlement_confirmed?: boolean;
  pending_exit_count?: number;
  unresolved_mapping_count?: number;
  reasons?: string[];
}
export interface Pipeline extends SourceStatus {
  overall_status?: string;
  session?: string;
  warning_count?: number;
  critical_count?: number;
  stages?: { name: string; status: string; reason?: string }[];
  products?: {
    product_id: string;
    instrument_id: string;
    status: string;
    reason?: string;
    strategy_evaluations?: number;
    candidates?: number;
    allowed?: number;
    pending_traces?: number;
    tick_age_seconds?: number;
  }[];
}
export interface Health {
  readiness?: Readiness;
  pipeline?: Pipeline;
}
export interface Snapshot {
  schema_version: 1;
  data_scope_id: string;
  generated_at_ms: number;
  environment: string;
  account_alias: string;
  instance_id: string;
  trading_day: string;
  account: Block<Account | null>;
  positions: Block<Position[]>;
  strategy_positions: Block<StrategyPosition[]>;
  markets: Block<MarketQuote[]>;
  health: Block<Health>;
  orders: Block<Order[]>;
  trades: Block<Trade[]>;
}
export interface DayEntry {
  trading_day: string;
  equity_points: number;
  order_count: number;
  trade_count: number;
}
export interface Days {
  schema_version: 1;
  data_scope_id: string;
  generated_at_ms: number;
  days: DayEntry[];
}
export interface EquityPoint {
  as_of_ms: number;
  balance: number;
  available: number;
  curr_margin: number;
  commission: number;
  close_profit: number;
  position_profit: number;
}
export interface Archive<T> extends Block<T[]> {
  schema_version: 1;
  data_scope_id: string;
}
