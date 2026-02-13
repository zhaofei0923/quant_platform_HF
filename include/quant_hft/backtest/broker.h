#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/contracts/types.h"

namespace quant_hft::backtest {

struct BrokerConfig {
    double initial_capital{1'000'000.0};
    double commission_rate{0.0001};
    double slippage{0.0};
    bool partial_fill_enabled{true};
    double close_today_commission_rate{0.0002};
};

class SimulatedBroker {
public:
    explicit SimulatedBroker(BrokerConfig config = {});

    void OnTick(const Tick& tick);

    std::string PlaceOrder(const OrderIntent& intent);
    bool CancelOrder(const std::string& client_order_id);

    std::vector<Position> GetPositions(const std::string& symbol = "") const;
    double GetAccountBalance() const;

    void SetFillCallback(std::function<void(const Trade&)> callback);
    void SetOrderCallback(std::function<void(const Order&)> callback);

private:
    struct PendingOrder {
        Order order;
        OffsetFlag offset{OffsetFlag::kOpen};
        std::int32_t remaining_volume{0};
        bool is_market{false};
    };

    struct PositionLot {
        PositionDirection direction{PositionDirection::kLong};
        std::int32_t volume{0};
        double open_price{0.0};
    };

    void TryMatchOrder(PendingOrder* pending, const Tick& tick);
    double ComputeCommission(const PendingOrder& pending,
                             std::int32_t fill_qty,
                             double fill_price) const;
    double ApplySlippage(double raw_price, Side side) const;
    void ApplyTradeToPosition(const Trade& trade);

    BrokerConfig config_;
    std::vector<PendingOrder> buy_orders_;
    std::vector<PendingOrder> sell_orders_;
    std::unordered_map<std::string, std::vector<PositionLot>> lots_by_symbol_;
    std::unordered_map<std::string, Tick> last_tick_by_symbol_;

    double account_balance_{0.0};
    std::int64_t id_seed_{0};

    std::function<void(const Trade&)> fill_callback_;
    std::function<void(const Order&)> order_callback_;
};

}  // namespace quant_hft::backtest
