#pragma once

#include <mutex>
#include <string>
#include <unordered_map>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct CtpMarginPriceInputs {
    double pre_settlement_price{0.0};
    double settlement_price{0.0};
    double average_price{0.0};
    double open_price{0.0};
};

struct CtpOrderFundInputs {
    std::string client_order_id;
    double price{0.0};
    std::int32_t volume{0};
    std::int32_t volume_multiple{0};
    double margin_ratio_by_money{0.0};
    double margin_ratio_by_volume{0.0};
    double commission_ratio_by_money{0.0};
    double commission_ratio_by_volume{0.0};
};

class CtpAccountLedger {
public:
    static double ResolveMarginPrice(char margin_price_type,
                                     const CtpMarginPriceInputs& prices);
    static double ComputePositionMargin(char margin_price_type,
                                        const CtpMarginPriceInputs& prices,
                                        std::int32_t position_volume,
                                        std::int32_t volume_multiple,
                                        double margin_rate);
    static double ComputeOrderMargin(const CtpOrderFundInputs& inputs);
    static double ComputeOrderCommission(const CtpOrderFundInputs& inputs);

    void SetMarginPriceType(char margin_price_type);
    char margin_price_type() const;

    void ApplyTradingAccountSnapshot(const TradingAccountSnapshot& snapshot);
    bool ReserveOrderFunds(const CtpOrderFundInputs& inputs, std::string* error);
    bool ApplyOrderEvent(const OrderEvent& event, std::string* error);
    void ApplyDailySettlement(double previous_settlement_price,
                              double new_settlement_price,
                              std::int32_t net_position,
                              std::int32_t volume_multiple);
    void RollTradingDay(const std::string& trading_day);

    double balance() const;
    double available() const;
    double current_margin() const;
    double frozen_margin() const;
    double frozen_commission() const;
    double commission() const;
    double daily_settlement_pnl() const;
    std::string trading_day() const;

private:
    static double NormalizePrice(double price);
    static bool IsTerminalStatus(OrderStatus status);

    struct PendingOrderFunds {
        std::int32_t requested_volume{0};
        std::int32_t last_filled_volume{0};
        double margin_per_volume{0.0};
        double commission_per_volume{0.0};
        double frozen_margin_remaining{0.0};
        double frozen_commission_remaining{0.0};
    };

    mutable std::mutex mutex_;
    char margin_price_type_{'1'};
    double balance_{0.0};
    double available_{0.0};
    double current_margin_{0.0};
    double frozen_margin_{0.0};
    double frozen_commission_{0.0};
    double commission_{0.0};
    double daily_settlement_pnl_{0.0};
    std::string trading_day_;
    std::unordered_map<std::string, PendingOrderFunds> pending_order_funds_;
};

}  // namespace quant_hft
