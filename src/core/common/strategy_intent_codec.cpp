#include "quant_hft/core/strategy_intent_codec.h"

#include <vector>

namespace quant_hft {
namespace {

bool ParseInt(const std::string& text, std::int32_t* out) {
    if (out == nullptr) {
        return false;
    }
    try {
        *out = static_cast<std::int32_t>(std::stoi(text));
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseInt64(const std::string& text, std::int64_t* out) {
    if (out == nullptr) {
        return false;
    }
    try {
        *out = static_cast<std::int64_t>(std::stoll(text));
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseDouble(const std::string& text, double* out) {
    if (out == nullptr) {
        return false;
    }
    try {
        *out = std::stod(text);
        return true;
    } catch (...) {
        return false;
    }
}

std::vector<std::string> Split(const std::string& value, char delim) {
    std::vector<std::string> result;
    std::size_t start = 0;
    while (start <= value.size()) {
        const auto end = value.find(delim, start);
        if (end == std::string::npos) {
            result.push_back(value.substr(start));
            break;
        }
        result.push_back(value.substr(start, end - start));
        start = end + 1;
    }
    return result;
}

}  // namespace

bool StrategyIntentCodec::DecodeSignalIntent(const std::string& strategy_id,
                                             const std::string& encoded,
                                             SignalIntent* out,
                                             std::string* error) {
    if (out == nullptr || strategy_id.empty()) {
        if (error != nullptr) {
            *error = "output pointer or strategy_id is empty";
        }
        return false;
    }

    const auto segments = Split(encoded, '|');
    if (segments.size() != 7) {
        if (error != nullptr) {
            *error = "intent segment count must be 7";
        }
        return false;
    }

    SignalIntent intent;
    intent.strategy_id = strategy_id;
    intent.instrument_id = segments[0];
    if (intent.instrument_id.empty()) {
        if (error != nullptr) {
            *error = "instrument_id is empty";
        }
        return false;
    }
    if (!ParseSide(segments[1], &intent.side)) {
        if (error != nullptr) {
            *error = "invalid side: " + segments[1];
        }
        return false;
    }
    if (!ParseOffset(segments[2], &intent.offset)) {
        if (error != nullptr) {
            *error = "invalid offset: " + segments[2];
        }
        return false;
    }
    if (!ParseInt(segments[3], &intent.volume)) {
        if (error != nullptr) {
            *error = "invalid volume: " + segments[3];
        }
        return false;
    }
    if (!ParseDouble(segments[4], &intent.limit_price)) {
        if (error != nullptr) {
            *error = "invalid limit_price: " + segments[4];
        }
        return false;
    }
    if (!ParseInt64(segments[5], &intent.ts_ns)) {
        if (error != nullptr) {
            *error = "invalid signal_ts_ns: " + segments[5];
        }
        return false;
    }
    intent.trace_id = segments[6];
    if (intent.trace_id.empty()) {
        if (error != nullptr) {
            *error = "trace_id is empty";
        }
        return false;
    }

    *out = intent;
    return true;
}

bool StrategyIntentCodec::ParseSide(const std::string& text, Side* out) {
    if (out == nullptr) {
        return false;
    }
    if (text == "BUY") {
        *out = Side::kBuy;
        return true;
    }
    if (text == "SELL") {
        *out = Side::kSell;
        return true;
    }
    return false;
}

bool StrategyIntentCodec::ParseOffset(const std::string& text, OffsetFlag* out) {
    if (out == nullptr) {
        return false;
    }
    if (text == "OPEN") {
        *out = OffsetFlag::kOpen;
        return true;
    }
    if (text == "CLOSE") {
        *out = OffsetFlag::kClose;
        return true;
    }
    if (text == "CLOSE_TODAY") {
        *out = OffsetFlag::kCloseToday;
        return true;
    }
    if (text == "CLOSE_YESTERDAY") {
        *out = OffsetFlag::kCloseYesterday;
        return true;
    }
    return false;
}

std::string StrategyIntentCodec::ToSideString(Side side) {
    return side == Side::kSell ? "SELL" : "BUY";
}

std::string StrategyIntentCodec::ToOffsetString(OffsetFlag offset) {
    switch (offset) {
        case OffsetFlag::kOpen:
            return "OPEN";
        case OffsetFlag::kClose:
            return "CLOSE";
        case OffsetFlag::kCloseToday:
            return "CLOSE_TODAY";
        case OffsetFlag::kCloseYesterday:
            return "CLOSE_YESTERDAY";
    }
    return "OPEN";
}

}  // namespace quant_hft
