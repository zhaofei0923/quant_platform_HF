#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/contracts/types.h"
#include "quant_hft/strategy/atomic_strategy.h"
#include "quant_hft/strategy/live_strategy.h"
#include "quant_hft/strategy/signal_merger.h"

namespace quant_hft {

class AtomicFactory;

struct SubStrategyOverrides {
    AtomicParams backtest_params;
    AtomicParams sim_params;
    AtomicParams live_params;
};

struct SubStrategyDefinition {
    std::string id;
    bool enabled{true};
    std::string type;
    std::int32_t timeframe_minutes{1};
    std::string config_path;
    AtomicParams params;
    SubStrategyOverrides overrides;
    std::vector<MarketRegime> entry_market_regimes;
};

struct CompositeStrategyDefinition {
    std::string run_type{"live"};
    bool enable_non_backtest{false};
    bool market_state_mode{true};
    SignalMergeRule merge_rule{SignalMergeRule::kPriority};
    std::vector<SubStrategyDefinition> sub_strategies;
};

struct CompositeAtomicTraceRow {
    std::string strategy_id;
    std::string strategy_type;
    std::optional<double> kama;
    std::optional<double> atr;
    std::optional<double> adx;
    std::optional<double> er;
    std::optional<double> stop_loss_price;
    std::optional<double> take_profit_price;
};

class CompositeStrategy : public ILiveStrategy {
   public:
    CompositeStrategy();
    explicit CompositeStrategy(CompositeStrategyDefinition definition,
                               AtomicFactory* factory = nullptr);
    ~CompositeStrategy() override = default;

    void Initialize(const StrategyContext& ctx) override;
    std::vector<SignalIntent> OnState(const StateSnapshot7D& state) override;
    void OnOrderEvent(const OrderEvent& event) override;
    void OnAccountSnapshot(const TradingAccountSnapshot& snapshot) override;
    std::vector<SignalIntent> OnTimer(EpochNanos now_ns) override;
    std::vector<StrategyMetric> CollectMetrics() const override;
    bool SaveState(StrategyState* out, std::string* error) const override;
    bool LoadState(const StrategyState& state, std::string* error) override;
    void Shutdown() override;
    std::vector<CompositeAtomicTraceRow> CollectAtomicIndicatorTrace() const;
    void SetBacktestAccountSnapshot(double equity, double pnl_after_cost);
    void SetBacktestContractMultiplier(const std::string& instrument_id, double multiplier);

   private:
    struct SubStrategySlot {
        ISubStrategy* strategy{nullptr};
        std::int32_t timeframe_minutes{1};
        std::vector<MarketRegime> entry_market_regimes;
    };

    struct TimeFilterSlot {
        ITimeFilterStrategy* strategy{nullptr};
        std::int32_t timeframe_minutes{1};
    };

    struct RiskControlSlot {
        IRiskControlStrategy* strategy{nullptr};
        std::int32_t timeframe_minutes{1};
    };

    struct AtomicTraceSlot {
        std::string strategy_id;
        std::string strategy_type;
        IAtomicIndicatorTraceProvider* provider{nullptr};
    };

    bool IsOpenSignalAllowedByRegime(const SubStrategySlot& slot, MarketRegime regime) const;
    bool AllowOpeningByTimeFilters(EpochNanos now_ns, std::int32_t timeframe_minutes);
    std::vector<SignalIntent> ApplyNonOpenSignalGate(const std::vector<SignalIntent>& signals);
    std::vector<SignalIntent> ApplyOpeningGate(const std::vector<SignalIntent>& opening_signals,
                                               const StateSnapshot7D& state);
    std::vector<SignalIntent> MergeSignals(const std::vector<SignalIntent>& signals) const;
    static AtomicParams MergeParamsForRunMode(const SubStrategyDefinition& definition,
                                              RunMode run_mode);
    static bool IsValidRunType(const std::string& run_type);
    void LoadDefinitionFromContext();
    void BuildAtomicStrategies();

    StrategyContext strategy_context_;
    CompositeStrategyDefinition definition_;
    bool has_embedded_definition_{false};
    AtomicFactory* factory_{nullptr};
    AtomicStrategyContext atomic_context_;
    std::unique_ptr<ISignalMerger> signal_merger_;

    std::vector<std::unique_ptr<IAtomicStrategy>> owned_atomic_strategies_;
    std::vector<SubStrategySlot> sub_strategies_;
    std::vector<TimeFilterSlot> time_filters_;
    std::vector<RiskControlSlot> risk_control_strategies_;
    std::vector<IAtomicOrderAware*> order_aware_strategies_;
    std::vector<AtomicTraceSlot> trace_providers_;
    std::unordered_map<std::string, std::int32_t> last_filled_volume_by_order_;
    std::unordered_map<std::string, std::string> position_owner_by_instrument_;
    std::unordered_map<std::string, SignalIntent> pending_reverse_open_by_instrument_;
};

bool RegisterCompositeStrategy(std::string* error = nullptr);

}  // namespace quant_hft
