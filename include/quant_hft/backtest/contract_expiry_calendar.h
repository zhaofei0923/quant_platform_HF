#pragma once

#include <cstddef>
#include <string>
#include <unordered_map>

namespace quant_hft {

struct ContractExpiryEntry {
    std::string instrument_id;
    std::string last_trading_day;
};

class ContractExpiryCalendar {
   public:
    void Clear();

    bool Upsert(const ContractExpiryEntry& entry, std::string* error = nullptr);

    const ContractExpiryEntry* Find(const std::string& instrument_id) const;

    std::size_t Size() const noexcept;

   private:
    std::unordered_map<std::string, ContractExpiryEntry> entries_by_instrument_;
};

std::string CanonicalContractInstrumentId(const std::string& instrument_id);

bool LoadContractExpiryCalendar(const std::string& path, ContractExpiryCalendar* out,
                                std::string* error);

}  // namespace quant_hft
