# Simulation Trade Candidate Parameters

## Effective Runtime Configs

- SimNow multi-product entry: `configs/sim/ctp_sim_trade_candidates.yaml`
- Portfolio backtest run config: `configs/backtest/backtest_run_sim_trade_candidates.yaml`
- Default validation wrapper config: `scripts/build/run_backtest_with_validation.sh` defaults to `configs/backtest/backtest_run_sim_trade_candidates.yaml`
- Per-product backtest run configs:
  - `configs/backtest/backtest_run_sim_trade_candidate_c.yaml`
  - `configs/backtest/backtest_run_sim_trade_candidate_rb.yaml`
  - `configs/backtest/backtest_run_sim_trade_candidate_m.yaml`
  - `configs/backtest/backtest_run_sim_trade_candidate_hc.yaml`
- Per-product Composite configs:
  - `configs/strategies/main_sim_trade_candidate_c.yaml`
  - `configs/strategies/main_sim_trade_candidate_rb.yaml`
  - `configs/strategies/main_sim_trade_candidate_m.yaml`
  - `configs/strategies/main_sim_trade_candidate_hc.yaml`
- Per-product KAMA parameter configs:
  - `configs/strategies/sub/kama_sim_trade_candidate_c.yaml`
  - `configs/strategies/sub/kama_sim_trade_candidate_rb.yaml`
  - `configs/strategies/sub/kama_sim_trade_candidate_m.yaml`
  - `configs/strategies/sub/kama_sim_trade_candidate_hc.yaml`

Backtest note: `configs/backtest/backtest_run_sim_trade_candidates.yaml` uses `product_ids` with `strategy_main_config_template` and `strategy_id_template` to choose the backtest products. Set `product_ids: "hc"` for a single product, or `product_ids: "c,rb,m,hc"` for the portfolio run; the script expands those products into independent Composite configs and runs them in one process with a timestamp-merged tick stream.

## rb KAMA / CompositeStrategy

- Recorded on: 2026-05-02
- Run id: `kama_rb_20230101_20241231_pf_grid_20260501_v1_refine1`
- Candidate: `c07_refine_pf_pnl_stop5`
- Scope: `rb`, 5min bars, 2023-2024 data only, initial equity 200000
- Final decision metric: highest mean Calmar on fixed rolling 60 trading-day, single-contract test windows
- Parameters:
  - `kama_filter: 0.2`
  - `risk_per_trade_pct: 0.005`
  - `stop_loss_atr_multiplier: 5.0`
- Validation note: c07 narrowly beat c06; keep c06 (`risk_per_trade_pct: 0.003`) as the conservative comparison candidate.
- Source file: `docs/results/opts/kama_rb_20230101_20241231_pf_grid_20260501_v1_refine1/final_selected_params_c07_fixed_rolling_calmar.yaml`

## m KAMA / CompositeStrategy

- Recorded on: 2026-05-02
- Run id: `kama_m_20230101_20241231_pf_grid_20260502_v1`
- Candidate: `c06_oos_midrisk_stop4125`
- Scope: `m`, 5min bars, Parquet v2, `strict_parquet: true`, 2023-2024 data, initial equity 200000
- Risk level: `risk_per_trade_pct: 0.005`
- Final decision metric within this risk level: highest aggregate fixed rolling test Calmar after excluding rollover-contaminated windows
- Parameters:
  - `kama_filter: 1.1`
  - `risk_per_trade_pct: 0.005`
  - `stop_loss_atr_multiplier: 4.125`
- Validation summary:
  - Fixed rolling hard Calmar: `5.7716`
  - Annualized return: `18.76%`
  - Max drawdown: `3.25%`
  - Total valid-window PnL: `26130`
  - Positive windows: `3/3`
  - PF > 1 windows: `3/3`
  - Weakest valid window Calmar: `0.3395`
- Validation note: c06 is the best validated candidate at `risk_per_trade_pct: 0.005`; it is not the all-risk final winner because c09 has slightly higher aggregate fixed rolling Calmar and lower drawdown at `risk_per_trade_pct: 0.003`.
- Rollover note: only 3 of 11 candidate 60-trading-day windows survived the single-contract rollover audit; use this as a simulation or pre-production validation candidate only.
- Source file: `docs/results/opts/kama_m_20230101_20241231_pf_grid_20260502_v1/optimal_candidate_params_risk_0005.md`
- Fixed rolling report: `docs/results/rolling/kama_m_20230101_20241231_pf_grid_20260502_v1_c06_oos_midrisk_stop4125_fixed_60d_single_contract_report.md`

## c KAMA / CompositeStrategy

- Recorded on: 2026-05-02
- Run id: `kama_c_20230103_20241231_pf_grid_v1`
- Candidate: `kf120_sl40_rp0005`
- Scope: `c`, 5min bars, Parquet v2, `strict_parquet: true`, 2023-2024 data, initial equity 200000
- Recommendation: `pre_live_evaluation`
- Final decision metric: highest OOS Calmar ratio on clean 2023H2 data (not used in training)
- Parameters:
  - `kama_filter: 1.2`
  - `risk_per_trade_pct: 0.005`
  - `stop_loss_atr_multiplier: 4.0`
- Search summary:
  - Coarse grid (12 trials): kf=[0.3,0.5,0.8,1.2], sl=[3.0,4.0,5.0], 12/12 completed, best PF=1.76 (kf=1.2, sl=4.0)
  - Refine1 (15 trials): kf=[0.8,1.2,1.5,1.8,2.1], confirmed kf=1.2 peak, PF unchanged
  - Refinement stopped: PF improvement < 3%
- Walk-Forward summary:
  - 2024年 3 training windows, 3/3 success, all test PF > 1.15
  - Test Calmars: 3.60, 2.75, 5.17
  - Test PnLs: +9,080, +4,700, +4,560
  - Window 0 params: kf=0.5, sl=4.0; Window 1: kf=0.3, sl=5.0; Window 2: kf=1.2, sl=5.0
  - Parameter stability: moderate (sl stable ~4-5, kf varies 0.3-1.2)
- OOS validation (2023-07-01 to 2023-12-31, clean data not used in any training window):
  - Observed candidates: 7
  - Best OOS Calmar: `6.42` (kf=1.2, sl=4.0, PnL=+19,480, PF=1.77)
  - All 7 candidates profitable in OOS (PF > 1.0, PnL > 0)
- Full-period sanity check (2023-01-03 to 2024-12-31):
  - Final equity: `252,722`
  - Total PnL: `+61,290` (+30.6%)
  - Max drawdown: `7,366` (3.68%)
  - Profit factor: `1.75`
  - Calmar ratio: `~8.3`
- Rollover note: c variety has dense contract rollovers; all 3 Walk-Forward 60-day test windows were contaminated (multi-contract). OOS validation was performed on clean 2023H2 single-contract data instead. Use as a simulation or pre-live evaluation candidate only. Consider running fixed-parameter rolling with shorter windows or on a per-contract basis before production.
- Historical optimization configs: run-specific search configs are no longer checked in; effective runtime configs are listed at the top of this document.
- Outputs:
  - Coarse results: `docs/results/opts/kama_c_20230103_20241231_pf_grid_v1/`
  - Refine1 results: `docs/results/opts/kama_c_20230103_20241231_pf_grid_v1_refine1/`
  - Rolling results: `runtime/rolling_optimize_kama_c_20230103_20241231_pf_grid_v1/`
  - Full backtest: `docs/results/backtest_runs/kama_c_v1_final_20260502T145450/`

## hc KAMA / CompositeStrategy

- Recorded on: 2026-05-02
- Run id: `kama_hc_20230101_20241231_pf_grid_20260502_v1`
- Candidate: `c02_oos_w1_f020_s600`
- Scope: `hc`, 5min bars, Parquet v2, `strict_parquet: true`, 2023-2024 data, initial equity 200000
- Recommendation: `pre_live_evaluation`
- Final decision metric: highest fixed-parameter rolling mean test Calmar across clean 60 trading-day, single-contract test windows
- Parameters:
  - `kama_filter: 0.2`
  - `risk_per_trade_pct: 0.005`
  - `stop_loss_atr_multiplier: 6.0`
- Validation summary:
  - Fixed rolling hard Calmar: `12.70859218293`
  - Window Calmars: `4.38034925629`, `19.60367244`, `14.1417548525`
  - Valid clean windows: `3/3`
  - Contaminated windows: `0`
  - Mean profit factor: `1.94273769827`
  - Total valid-window PnL: `22490`
  - Max window drawdown: `2.46234388549%`
  - Failed windows: `0`
- OOS screening summary:
  - Observed windows: `0`, `1`
  - Mean OOS Calmar: `11.992010848`
  - Min OOS Calmar: `4.380349256`
  - Mean OOS profit factor: `1.8690615025`
  - Total OOS PnL: `12720`
- Full-period sanity check:
  - Period: `20230101-20241231`
  - Final equity: `267270.522`
  - Total PnL after cost: `67270.522`
  - Total realized PnL: `72120`
  - Total commission: `4849.478`
  - Max drawdown abs: `7195.959`
  - Profit factor: `1.9824274622`
  - Recovery factor: `12.358569363`
  - Trades: `548`
  - Invariant violations: `0`
- Validation note: selected by the predeclared hard metric, not by in-sample PF or OOS cherry-picking; use as a simulation or pre-live evaluation candidate only.
- Rollover note: all 3 fixed rolling test windows were clean single-contract windows.
- Source file: `docs/results/opts/kama_hc_20230101_20241231_pf_grid_20260502_v1/final_recommended_params.yaml`
- Recommendation report: `docs/results/opts/kama_hc_20230101_20241231_pf_grid_20260502_v1/final_recommendation.md`
- Fixed validation summary: `docs/results/opts/kama_hc_20230101_20241231_pf_grid_20260502_v1/fixed_validation_summary.csv`
