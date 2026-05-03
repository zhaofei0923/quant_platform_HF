#pragma once

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>

namespace quant_hft {

inline std::string ExtractProductIdFromInstrumentId(std::string_view instrument_id) {
    const std::size_t dot = instrument_id.find_last_of('.');
    if (dot != std::string_view::npos) {
        instrument_id.remove_prefix(dot + 1);
    }

    std::size_t end = 0;
    while (end < instrument_id.size() &&
           std::isalpha(static_cast<unsigned char>(instrument_id[end])) != 0) {
        ++end;
    }
    if (end == 0) {
        end = instrument_id.size();
    }

    std::string product_id(instrument_id.substr(0, end));
    std::transform(product_id.begin(), product_id.end(), product_id.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return product_id;
}

}  // namespace quant_hft