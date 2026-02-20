#pragma once

#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/indicators/atr.h"
#include "quant_hft/indicators/kama.h"
#include "quant_hft/strategy/atomic_strategy.h"

namespace quant_hft {

class KamaTrendStrategy final : public ISubStrategy, public IAtomicIndicatorTraceProvider {
   public:
    KamaTrendStrategy() = default;

    void Init(const AtomicParams& params) override;
    std::string GetId() const override;
    void Reset() override;
    std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                      const AtomicStrategyContext& ctx) override;
    std::optional<AtomicIndicatorSnapshot> IndicatorSnapshot() const override;

   private:
    int ClassifyDiff(double diff, double threshold) const;
    int ComputeOrderVolume(const AtomicStrategyContext& ctx, const std::string& instrument_id,
                           double atr_value) const;
    double ComputeStdKama() const;
    static std::string ExtractSymbolPrefixLower(const std::string& instrument_id);
    static std::string ToUpper(std::string text);
    static SignalIntent BuildCloseSignal(const std::string& strategy_id,
                                         const std::string& instrument_id,
                                         SignalType signal_type,
                                         std::int32_t position, double limit_price,
                                         EpochNanos ts_ns);

    std::string id_{"KamaTrendStrategy"};
    int er_period_{10};
    int fast_period_{2};
    int slow_period_{30};
    int std_period_{20};
    double kama_filter_{0.5};
    double risk_per_trade_pct_{0.01};
    int default_volume_{1};
    std::string stop_loss_mode_{"trailing_atr"};
    int stop_loss_atr_period_{14};
    double stop_loss_atr_multiplier_{2.0};
    std::string take_profit_mode_{"atr_target"};
    int take_profit_atr_period_{14};
    double take_profit_atr_multiplier_{3.0};

    std::unique_ptr<KAMA> kama_;
    std::unique_ptr<ATR> stop_loss_atr_;
    std::unique_ptr<ATR> take_profit_atr_;
    std::deque<double> kama_recent_;
    std::deque<double> kama_window_;
    double kama_window_sum_{0.0};
    double kama_window_sum_sq_{0.0};
    std::unordered_map<std::string, double> trailing_stop_by_instrument_;
    std::unordered_map<std::string, int> trailing_direction_by_instrument_;

    std::optional<double> last_kama_;
    std::optional<double> last_er_;
    std::optional<double> last_stop_atr_;
    std::optional<double> last_take_atr_;
    std::optional<double> last_stop_loss_price_;
    std::optional<double> last_take_profit_price_;
};

}  // namespace quant_hft
