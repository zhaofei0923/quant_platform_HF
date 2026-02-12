#pragma once

#include <mutex>
#include <string>
#include <unordered_map>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct SelfTradeRiskConfig {
    bool enabled{true};
    bool strict_mode{false};
    // <= 0 means always strict when crossing is detected.
    int strict_mode_trigger_hits{1};
};

class SelfTradeRiskEngine {
public:
    explicit SelfTradeRiskEngine(SelfTradeRiskConfig config);

    RiskDecision PreCheck(const OrderIntent& intent);
    void RecordAcceptedOrder(const OrderIntent& intent);
    void OnOrderEvent(const OrderEvent& event);

    bool strict_mode() const;
    int conflict_hits() const;

private:
    struct ActiveOrder {
        std::string account_id;
        std::string instrument_id;
        Side side{Side::kBuy};
        double price{0.0};
        std::int32_t remaining_volume{0};
        std::int32_t last_filled_volume{0};
    };

    static bool IsTerminalStatus(OrderStatus status);
    static bool IsCrossing(const OrderIntent& intent, const ActiveOrder& resting);
    static RiskDecision BuildDecision(RiskAction action,
                                      const std::string& reason,
                                      double observed_value,
                                      double threshold_value);

    mutable std::mutex mutex_;
    SelfTradeRiskConfig config_;
    bool strict_mode_{false};
    int conflict_hits_{0};
    std::unordered_map<std::string, ActiveOrder> active_orders_;
};

}  // namespace quant_hft
