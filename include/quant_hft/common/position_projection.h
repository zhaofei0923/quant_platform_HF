#pragma once

#include <cmath>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

inline std::string PositionProjectionKey(const Position& position) {
    const auto part = [](const std::string& value) {
        return std::to_string(value.size()) + ":" + value;
    };
    return part(position.account_id) + part(position.strategy_id) + part(position.symbol) +
           part(position.exchange) + std::to_string(static_cast<int>(position.hedge_flag));
}

inline std::string EncodePositionProjection(const Position& position) {
    std::ostringstream out;
    out << std::setprecision(std::numeric_limits<double>::max_digits10)
        << std::quoted(position.account_id) << ' ' << std::quoted(position.strategy_id) << ' '
        << std::quoted(position.symbol) << ' ' << std::quoted(position.exchange) << ' '
        << static_cast<int>(position.hedge_flag) << ' ' << position.version << ' '
        << position.long_qty << ' ' << position.short_qty << ' ' << position.avg_long_price << ' '
        << position.avg_short_price;
    return out.str();
}

inline bool DecodePositionProjection(const std::string& text, Position* position) {
    if (position == nullptr) return false;
    Position parsed;
    int hedge = 0;
    std::istringstream in(text);
    if (!(in >> std::quoted(parsed.account_id) >> std::quoted(parsed.strategy_id) >>
          std::quoted(parsed.symbol) >> std::quoted(parsed.exchange) >> hedge >> parsed.version >>
          parsed.long_qty >> parsed.short_qty >> parsed.avg_long_price >> parsed.avg_short_price)) {
        return false;
    }
    in >> std::ws;
    if (!in.eof() || parsed.account_id.empty() || parsed.strategy_id.empty() ||
        parsed.symbol.empty() || parsed.long_qty < 0 || parsed.short_qty < 0 ||
        !std::isfinite(parsed.avg_long_price) || !std::isfinite(parsed.avg_short_price) ||
        hedge < 0 || hedge > 2) return false;
    parsed.hedge_flag = static_cast<HedgeFlag>(hedge);
    *position = std::move(parsed);
    return true;
}

}  // namespace quant_hft
