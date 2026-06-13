#include "quant_hft/services/ctp_close_offset_resolver.h"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <unordered_map>

namespace quant_hft {
namespace {

std::string UpperAscii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::toupper(ch)); });
    return value;
}

std::string ProductFromInstrumentId(const std::string& instrument_id) {
    std::string product;
    const auto dot = instrument_id.find('.');
    const std::size_t start = dot == std::string::npos ? 0 : dot + 1;
    for (std::size_t i = start; i < instrument_id.size(); ++i) {
        const char ch = instrument_id[i];
        if (std::isalpha(static_cast<unsigned char>(ch))) {
            product.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
        } else if (!product.empty()) {
            break;
        }
    }
    return product;
}

bool IsCloseOffset(OffsetFlag offset) {
    return offset == OffsetFlag::kClose || offset == OffsetFlag::kCloseToday ||
           offset == OffsetFlag::kCloseYesterday;
}

std::string AvailabilityKey(const OrderIntent& intent, PositionDirection direction,
                            const std::string& exchange_id, const std::string& hedge_flag,
                            const std::string& position_date) {
    std::ostringstream key;
    key << intent.account_id << '|' << intent.instrument_id << '|' << exchange_id << '|'
        << hedge_flag << '|' << static_cast<int>(direction) << '|' << position_date;
    return key.str();
}

PositionDirection ResolveCloseDirection(const OrderIntent& intent) {
    return intent.side == Side::kBuy ? PositionDirection::kShort : PositionDirection::kLong;
}

PlannedOrder MakeDatedClosePlan(const PlannedOrder& planned, OffsetFlag offset, std::int32_t volume,
                                const std::string& suffix) {
    PlannedOrder out = planned;
    out.intent.offset = offset;
    out.intent.volume = volume;
    if (!suffix.empty()) {
        out.intent.client_order_id += suffix;
        out.intent.trace_id += suffix;
    }
    return out;
}

std::string OffsetName(OffsetFlag offset) {
    switch (offset) {
        case OffsetFlag::kCloseToday:
            return "close_today";
        case OffsetFlag::kCloseYesterday:
            return "close_yesterday";
        case OffsetFlag::kOpen:
            return "open";
        case OffsetFlag::kClose:
        default:
            return "close";
    }
}

void RenumberSlices(std::vector<PlannedOrder>* plans) {
    if (plans == nullptr) {
        return;
    }
    const auto total = static_cast<std::int32_t>(plans->size());
    for (std::size_t i = 0; i < plans->size(); ++i) {
        (*plans)[i].slice_index = static_cast<std::int32_t>(i + 1);
        (*plans)[i].slice_total = total;
    }
}

bool ResolveSinglePlan(const PlannedOrder& planned, const CtpPositionLedger& position_ledger,
                       std::unordered_map<std::string, std::int32_t>* available_by_key,
                       std::vector<PlannedOrder>* output, std::string* error) {
    const OrderIntent& intent = planned.intent;
    if (intent.offset != OffsetFlag::kClose) {
        output->push_back(planned);
        return true;
    }

    const std::string exchange_id = InferCtpExchangeIdFromInstrumentId(intent.instrument_id);
    if (!RequiresExplicitCtpCloseOffset(exchange_id)) {
        output->push_back(planned);
        return true;
    }
    if (intent.account_id.empty() || intent.instrument_id.empty() || intent.volume <= 0) {
        if (error != nullptr) {
            *error = "invalid close intent for CTP offset resolution";
        }
        return false;
    }

    const PositionDirection direction = ResolveCloseDirection(intent);
    const std::string hedge_flag = CtpHedgeFlagToText(intent.hedge_flag);
    const auto load_available = [&](const std::string& position_date) {
        const std::string key =
            AvailabilityKey(intent, direction, exchange_id, hedge_flag, position_date);
        auto it = available_by_key->find(key);
        if (it != available_by_key->end()) {
            return it->second;
        }
        const std::int32_t closable =
            position_ledger.GetClosableVolume(intent.account_id, intent.instrument_id, direction,
                                              position_date, exchange_id, hedge_flag);
        available_by_key->emplace(key, closable);
        return closable;
    };
    const auto consume_available = [&](const std::string& position_date, std::int32_t volume) {
        const std::string key =
            AvailabilityKey(intent, direction, exchange_id, hedge_flag, position_date);
        auto it = available_by_key->find(key);
        if (it != available_by_key->end()) {
            it->second = std::max<std::int32_t>(0, it->second - volume);
        }
    };

    const std::int32_t today_closable = load_available("today");
    const std::int32_t yesterday_closable = load_available("yesterday");
    const std::int32_t total_closable = today_closable + yesterday_closable;
    if (total_closable < intent.volume) {
        if (error != nullptr) {
            std::ostringstream message;
            message << "insufficient dated closable volume"
                    << " account_id=" << intent.account_id
                    << " instrument_id=" << intent.instrument_id << " exchange_id=" << exchange_id
                    << " requested=" << intent.volume << " today_closable=" << today_closable
                    << " yesterday_closable=" << yesterday_closable
                    << " total_closable=" << total_closable;
            *error = message.str();
        }
        return false;
    }

    std::vector<PlannedOrder> resolved;
    std::int32_t remaining = intent.volume;
    if (today_closable > 0 && remaining > 0) {
        const std::int32_t volume = std::min(remaining, today_closable);
        resolved.push_back(MakeDatedClosePlan(planned, OffsetFlag::kCloseToday, volume, ""));
        consume_available("today", volume);
        remaining -= volume;
    }
    if (remaining > 0) {
        resolved.push_back(MakeDatedClosePlan(planned, OffsetFlag::kCloseYesterday, remaining, ""));
        consume_available("yesterday", remaining);
    }
    if (resolved.empty()) {
        if (error != nullptr) {
            *error = "no dated close plan produced";
        }
        return false;
    }

    if (resolved.size() > 1U) {
        for (auto& child : resolved) {
            const std::string suffix = child.intent.offset == OffsetFlag::kCloseToday
                                           ? "#close-today"
                                           : "#close-yesterday";
            child.intent.client_order_id += suffix;
            child.intent.trace_id += suffix;
        }
    }

    for (auto& child : resolved) {
        if (!IsCloseOffset(child.intent.offset) || child.intent.volume <= 0) {
            if (error != nullptr) {
                *error =
                    "invalid resolved close plan with offset=" + OffsetName(child.intent.offset);
            }
            return false;
        }
        output->push_back(std::move(child));
    }
    return true;
}

}  // namespace

std::string InferCtpExchangeIdFromInstrumentId(const std::string& instrument_id) {
    const auto dot = instrument_id.find('.');
    if (dot != std::string::npos && dot > 0) {
        return UpperAscii(instrument_id.substr(0, dot));
    }

    const std::string product = ProductFromInstrumentId(instrument_id);
    if (product.empty()) {
        return "";
    }
    if (product == "ag" || product == "al" || product == "ao" || product == "au" ||
        product == "br" || product == "bu" || product == "cu" || product == "fu" ||
        product == "hc" || product == "ni" || product == "pb" || product == "rb" ||
        product == "ru" || product == "sn" || product == "sp" || product == "ss" ||
        product == "wr" || product == "zn") {
        return "SHFE";
    }
    if (product == "a" || product == "b" || product == "bb" || product == "c" || product == "cs" ||
        product == "eb" || product == "eg" || product == "fb" || product == "i" || product == "j" ||
        product == "jd" || product == "jm" || product == "l" || product == "lh" || product == "m" ||
        product == "p" || product == "pg" || product == "pp" || product == "rr" || product == "v" ||
        product == "y") {
        return "DCE";
    }
    if (product == "ap" || product == "cf" || product == "cj" || product == "cy" ||
        product == "fg" || product == "jr" || product == "lr" || product == "ma" ||
        product == "oi" || product == "pf" || product == "pk" || product == "pm" ||
        product == "ri" || product == "rm" || product == "rs" || product == "sa" ||
        product == "sf" || product == "sh" || product == "sm" || product == "sr" ||
        product == "ta" || product == "ur" || product == "wh" || product == "zc") {
        return "CZCE";
    }
    if (product == "bc" || product == "ec" || product == "lu" || product == "nr" ||
        product == "sc") {
        return "INE";
    }
    if (product == "if" || product == "ih" || product == "im" || product == "ic" ||
        product == "t" || product == "tf" || product == "tl" || product == "ts") {
        return "CFFEX";
    }
    if (product == "lc" || product == "si") {
        return "GFEX";
    }
    return "";
}

std::string CtpHedgeFlagToText(HedgeFlag hedge_flag) {
    switch (hedge_flag) {
        case HedgeFlag::kArbitrage:
            return "2";
        case HedgeFlag::kHedge:
            return "3";
        case HedgeFlag::kSpeculation:
        default:
            return "1";
    }
}

bool RequiresExplicitCtpCloseOffset(const std::string& exchange_id) {
    const std::string normalized = UpperAscii(exchange_id);
    return normalized == "SHFE" || normalized == "INE";
}

bool ResolveCtpCloseOffsets(const std::vector<PlannedOrder>& input,
                            const CtpPositionLedger& position_ledger,
                            std::vector<PlannedOrder>* output, std::string* error) {
    if (output == nullptr) {
        if (error != nullptr) {
            *error = "output vector is required";
        }
        return false;
    }

    output->clear();
    output->reserve(input.size());
    std::unordered_map<std::string, std::int32_t> available_by_key;
    for (const PlannedOrder& planned : input) {
        if (!ResolveSinglePlan(planned, position_ledger, &available_by_key, output, error)) {
            output->clear();
            return false;
        }
    }
    RenumberSlices(output);
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

}  // namespace quant_hft
