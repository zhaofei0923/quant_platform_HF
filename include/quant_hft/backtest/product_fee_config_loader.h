#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

enum class ProductFeeMode : std::uint8_t {
    kRate = 0,
    kPerLot = 1,
};

struct ProductFeeEntry {
    std::string instrument_id;
    std::string symbol;
    double contract_multiplier{0.0};
    double long_margin_ratio{0.0};
    double short_margin_ratio{0.0};
    ProductFeeMode open_mode{ProductFeeMode::kRate};
    double open_value{0.0};
    ProductFeeMode close_mode{ProductFeeMode::kRate};
    double close_value{0.0};
    ProductFeeMode close_today_mode{ProductFeeMode::kRate};
    double close_today_value{0.0};
};

class ProductFeeBook {
   public:
    void Clear();

    bool Upsert(const ProductFeeEntry& entry, std::string* error = nullptr);

    const ProductFeeEntry* Find(const std::string& instrument_id) const;

    bool ExportContractMultipliers(
        std::unordered_map<std::string, double>* multipliers_by_instrument) const;

    static double ComputeCommission(const ProductFeeEntry& entry, OffsetFlag offset,
                                    std::int32_t volume, double fill_price);
    static double ComputePerLotMargin(const ProductFeeEntry& entry, Side side, double fill_price);
    static double ComputeRequiredMargin(const ProductFeeEntry& entry, Side side,
                                        std::int32_t volume, double fill_price);

   private:
    std::map<std::string, ProductFeeEntry> entries_by_instrument_;
    std::map<std::string, std::string> symbol_to_instrument_;
};

bool LoadProductFeeConfig(const std::string& path, ProductFeeBook* out, std::string* error);

}  // namespace quant_hft
