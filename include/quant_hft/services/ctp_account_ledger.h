#pragma once

#include <mutex>
#include <string>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct CtpMarginPriceInputs {
    double pre_settlement_price{0.0};
    double settlement_price{0.0};
    double average_price{0.0};
    double open_price{0.0};
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

    void SetMarginPriceType(char margin_price_type);
    char margin_price_type() const;

    void ApplyTradingAccountSnapshot(const TradingAccountSnapshot& snapshot);
    void ApplyDailySettlement(double previous_settlement_price,
                              double new_settlement_price,
                              std::int32_t net_position,
                              std::int32_t volume_multiple);
    void RollTradingDay(const std::string& trading_day);

    double balance() const;
    double available() const;
    double daily_settlement_pnl() const;
    std::string trading_day() const;

private:
    static double NormalizePrice(double price);

    mutable std::mutex mutex_;
    char margin_price_type_{'1'};
    double balance_{0.0};
    double available_{0.0};
    double daily_settlement_pnl_{0.0};
    std::string trading_day_;
};

}  // namespace quant_hft
