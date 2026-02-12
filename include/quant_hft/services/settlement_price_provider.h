#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace quant_hft {

struct SettlementPriceSource {
    enum class SourceType {
        kApi = 0,
        kExchangeFile = 1,
        kManual = 2,
        kCache = 3,
    };

    SourceType type{SourceType::kApi};
    std::string details;
};

class SettlementPriceProvider {
public:
    virtual ~SettlementPriceProvider() = default;

    virtual std::optional<std::pair<double, SettlementPriceSource>> GetSettlementPrice(
        const std::string& instrument_id,
        const std::string& trading_day) = 0;

    virtual std::unordered_map<std::string, std::pair<double, SettlementPriceSource>>
    BatchGetSettlementPrices(const std::vector<std::string>& instrument_ids,
                             const std::string& trading_day) = 0;

    virtual void SetManualOverride(const std::string& instrument_id,
                                   const std::string& trading_day,
                                   double price,
                                   const std::string& operator_id) = 0;
};

// Production-oriented implementation:
// - Source priority: manual override > API json > cache.
// - Cache is stored in a local file whose default name keeps sqlite extension for operational
//   compatibility with deployment conventions.
class ProdSettlementPriceProvider : public SettlementPriceProvider {
public:
    explicit ProdSettlementPriceProvider(
        std::string cache_db_path = "runtime/settlement_price_cache.sqlite",
        std::string api_price_json_path = "");
    ~ProdSettlementPriceProvider() override;

    std::optional<std::pair<double, SettlementPriceSource>> GetSettlementPrice(
        const std::string& instrument_id,
        const std::string& trading_day) override;

    std::unordered_map<std::string, std::pair<double, SettlementPriceSource>>
    BatchGetSettlementPrices(const std::vector<std::string>& instrument_ids,
                             const std::string& trading_day) override;

    void SetManualOverride(const std::string& instrument_id,
                           const std::string& trading_day,
                           double price,
                           const std::string& operator_id) override;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace quant_hft
