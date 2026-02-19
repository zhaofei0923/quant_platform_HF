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

namespace quant_hft {

class AtomicFactory;

enum class SignalMergeRule : std::uint8_t {
    kPriority = 0,
};

struct AtomicStrategyDefinition {
    std::string id;
    std::string type;
    AtomicParams params;
    std::vector<MarketRegime> market_regimes;
};

struct CompositeStrategyDefinition {
    SignalMergeRule merge_rule{SignalMergeRule::kPriority};
    std::vector<AtomicStrategyDefinition> opening_strategies;
    std::vector<AtomicStrategyDefinition> stop_loss_strategies;
    std::vector<AtomicStrategyDefinition> take_profit_strategies;
    std::vector<AtomicStrategyDefinition> time_filters;
    std::vector<AtomicStrategyDefinition> risk_control_strategies;
};

struct CompositeAtomicTraceRow {
    std::string strategy_id;
    std::string strategy_type;
    std::optional<double> kama;
    std::optional<double> atr;
    std::optional<double> adx;
    std::optional<double> er;
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
    std::vector<SignalIntent> OnTimer(EpochNanos now_ns) override;
    void Shutdown() override;
    std::vector<CompositeAtomicTraceRow> CollectAtomicIndicatorTrace() const;

   private:
    struct OpeningSlot {
        IOpeningStrategy* strategy{nullptr};
        std::vector<MarketRegime> market_regimes;
    };

    struct AtomicTraceSlot {
        std::string strategy_id;
        std::string strategy_type;
        IAtomicIndicatorTraceProvider* provider{nullptr};
    };

    bool IsOpeningAllowedByRegime(const OpeningSlot& slot, MarketRegime regime) const;
    std::vector<SignalIntent> MergeSignals(const std::vector<SignalIntent>& signals) const;
    static int SignalPriority(SignalType signal_type);
    static bool IsPreferredSignal(const SignalIntent& lhs, const SignalIntent& rhs);
    void LoadDefinitionFromContext();
    void BuildAtomicStrategies();

    StrategyContext strategy_context_;
    CompositeStrategyDefinition definition_;
    bool has_embedded_definition_{false};
    AtomicFactory* factory_{nullptr};
    AtomicStrategyContext atomic_context_;

    std::vector<std::unique_ptr<IAtomicStrategy>> owned_atomic_strategies_;
    std::vector<OpeningSlot> opening_strategies_;
    std::vector<IStopLossStrategy*> stop_loss_strategies_;
    std::vector<ITakeProfitStrategy*> take_profit_strategies_;
    std::vector<ITimeFilterStrategy*> time_filters_;
    std::vector<IRiskControlStrategy*> risk_control_strategies_;
    std::vector<IAtomicOrderAware*> order_aware_strategies_;
    std::vector<AtomicTraceSlot> trace_providers_;
    std::unordered_map<std::string, std::int32_t> last_filled_volume_by_order_;
};

bool RegisterCompositeStrategy(std::string* error = nullptr);

}  // namespace quant_hft
