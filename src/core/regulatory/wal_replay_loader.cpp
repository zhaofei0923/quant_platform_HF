#include "quant_hft/core/wal_replay_loader.h"

#include <cctype>
#include <fstream>
#include <string>

namespace quant_hft {

namespace {

std::string Trim(const std::string& text) {
    std::size_t begin = 0;
    while (begin < text.size() && std::isspace(static_cast<unsigned char>(text[begin])) != 0) {
        ++begin;
    }
    std::size_t end = text.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(text[end - 1])) != 0) {
        --end;
    }
    return text.substr(begin, end - begin);
}

bool ExtractRawValue(const std::string& line, const std::string& key, std::string* raw_value) {
    const std::string marker = "\"" + key + "\":";
    const auto marker_pos = line.find(marker);
    if (marker_pos == std::string::npos) {
        return false;
    }

    std::size_t pos = marker_pos + marker.size();
    while (pos < line.size() && std::isspace(static_cast<unsigned char>(line[pos])) != 0) {
        ++pos;
    }
    if (pos >= line.size()) {
        return false;
    }

    if (line[pos] == '"') {
        ++pos;
        std::size_t end = pos;
        bool escaped = false;
        while (end < line.size()) {
            const char ch = line[end];
            if (escaped) {
                escaped = false;
            } else if (ch == '\\') {
                escaped = true;
            } else if (ch == '"') {
                break;
            }
            ++end;
        }
        if (end >= line.size() || line[end] != '"') {
            return false;
        }
        *raw_value = line.substr(pos, end - pos);
        return true;
    }

    std::size_t end = pos;
    while (end < line.size()) {
        const char ch = line[end];
        if (ch == ',' || ch == '}') {
            break;
        }
        ++end;
    }
    *raw_value = Trim(line.substr(pos, end - pos));
    return !raw_value->empty();
}

std::string UnescapeJsonString(const std::string& raw) {
    std::string out;
    out.reserve(raw.size());
    for (std::size_t i = 0; i < raw.size(); ++i) {
        const char ch = raw[i];
        if (ch != '\\') {
            out.push_back(ch);
            continue;
        }
        if (i + 1 >= raw.size()) {
            out.push_back(ch);
            continue;
        }
        const char next = raw[++i];
        switch (next) {
            case '"':
                out.push_back('"');
                break;
            case '\\':
                out.push_back('\\');
                break;
            case 'n':
                out.push_back('\n');
                break;
            case 'r':
                out.push_back('\r');
                break;
            case 't':
                out.push_back('\t');
                break;
            default:
                out.push_back(next);
                break;
        }
    }
    return out;
}

bool ParseInt64Field(const std::string& line, const std::string& key, std::int64_t* value) {
    std::string raw;
    if (!ExtractRawValue(line, key, &raw)) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const auto parsed = std::stoll(raw, &consumed);
        if (consumed != raw.size()) {
            return false;
        }
        *value = parsed;
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseIntField(const std::string& line, const std::string& key, int* value) {
    std::int64_t parsed = 0;
    if (!ParseInt64Field(line, key, &parsed)) {
        return false;
    }
    *value = static_cast<int>(parsed);
    return true;
}

bool ParseDoubleField(const std::string& line, const std::string& key, double* value) {
    std::string raw;
    if (!ExtractRawValue(line, key, &raw)) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const auto parsed = std::stod(raw, &consumed);
        if (consumed != raw.size()) {
            return false;
        }
        *value = parsed;
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseStringField(const std::string& line, const std::string& key, std::string* value) {
    std::string raw;
    if (!ExtractRawValue(line, key, &raw)) {
        return false;
    }
    *value = UnescapeJsonString(raw);
    return true;
}

bool ParseSide(int raw_side, Side* side) {
    switch (raw_side) {
        case 0:
            *side = Side::kBuy;
            return true;
        case 1:
            *side = Side::kSell;
            return true;
        default:
            return false;
    }
}

bool ParseOffset(int raw_offset, OffsetFlag* offset) {
    switch (raw_offset) {
        case 0:
            *offset = OffsetFlag::kOpen;
            return true;
        case 1:
            *offset = OffsetFlag::kClose;
            return true;
        case 2:
            *offset = OffsetFlag::kCloseToday;
            return true;
        case 3:
            *offset = OffsetFlag::kCloseYesterday;
            return true;
        default:
            return false;
    }
}

bool ParseStatus(int raw_status, OrderStatus* status) {
    switch (raw_status) {
        case 0:
            *status = OrderStatus::kNew;
            return true;
        case 1:
            *status = OrderStatus::kAccepted;
            return true;
        case 2:
            *status = OrderStatus::kPartiallyFilled;
            return true;
        case 3:
            *status = OrderStatus::kFilled;
            return true;
        case 4:
            *status = OrderStatus::kCanceled;
            return true;
        case 5:
            *status = OrderStatus::kRejected;
            return true;
        default:
            return false;
    }
}

bool IsReplayableWalKind(const std::string& line, bool* replayable) {
    std::string kind;
    if (!ParseStringField(line, "kind", &kind)) {
        return false;
    }
    std::string event_type;
    if (ParseStringField(line, "event_type", &event_type) && event_type == "trade_fill") {
        *replayable = false;
        return true;
    }
    *replayable = (kind == "order" || kind == "trade");
    return true;
}

bool IsCtpOrderSubmitMapping(const std::string& line) {
    std::string kind;
    return ParseStringField(line, "kind", &kind) && kind == "ctp_order_submit_mapping";
}

bool ParseWalLine(const std::string& line, OrderEvent* event) {
    bool replayable = false;
    if (!IsReplayableWalKind(line, &replayable) || !replayable) {
        return false;
    }

    int raw_status = 0;
    if (!ParseIntField(line, "status", &raw_status) || !ParseStatus(raw_status, &event->status)) {
        return false;
    }

    if (!ParseStringField(line, "client_order_id", &event->client_order_id)) {
        return false;
    }

    (void)ParseStringField(line, "account_id", &event->account_id);
    (void)ParseStringField(line, "strategy_id", &event->strategy_id);
    (void)ParseStringField(line, "exchange_order_id", &event->exchange_order_id);
    (void)ParseStringField(line, "instrument_id", &event->instrument_id);
    (void)ParseStringField(line, "exchange_id", &event->exchange_id);
    (void)ParseStringField(line, "trade_id", &event->trade_id);
    (void)ParseStringField(line, "raw_trade_id", &event->raw_trade_id);
    (void)ParseStringField(line, "trading_day", &event->trading_day);
    (void)ParseStringField(line, "event_source", &event->event_source);
    (void)ParseStringField(line, "reason", &event->reason);
    (void)ParseStringField(line, "status_msg", &event->status_msg);
    (void)ParseStringField(line, "order_submit_status", &event->order_submit_status);
    (void)ParseStringField(line, "order_ref", &event->order_ref);
    (void)ParseStringField(line, "trace_id", &event->trace_id);

    std::int64_t ts_ns = 0;
    if (!ParseInt64Field(line, "ts_ns", &ts_ns)) {
        return false;
    }
    event->ts_ns = ts_ns;

    std::int64_t exchange_ts_ns = 0;
    if (ParseInt64Field(line, "exchange_ts_ns", &exchange_ts_ns)) {
        event->exchange_ts_ns = exchange_ts_ns;
    } else {
        event->exchange_ts_ns = ts_ns;
    }
    std::int64_t recv_ts_ns = 0;
    if (ParseInt64Field(line, "recv_ts_ns", &recv_ts_ns)) {
        event->recv_ts_ns = recv_ts_ns;
    } else {
        event->recv_ts_ns = ts_ns;
    }

    int filled_volume = 0;
    if (!ParseIntField(line, "filled_volume", &filled_volume)) {
        return false;
    }
    event->filled_volume = filled_volume;

    int parsed_int = 0;
    int raw_side = 0;
    if (ParseIntField(line, "side", &raw_side)) {
        (void)ParseSide(raw_side, &event->side);
    }
    int raw_offset = 0;
    if (ParseIntField(line, "offset", &raw_offset)) {
        (void)ParseOffset(raw_offset, &event->offset);
    }
    if (ParseIntField(line, "last_trade_volume", &parsed_int)) {
        event->last_trade_volume = parsed_int;
    }
    if (ParseIntField(line, "front_id", &parsed_int)) {
        event->front_id = parsed_int;
    }
    if (ParseIntField(line, "session_id", &parsed_int)) {
        event->session_id = parsed_int;
    }
    if (ParseIntField(line, "query_request_id", &parsed_int)) {
        event->query_request_id = parsed_int;
    }
    std::int64_t recovery_generation = 0;
    if (ParseInt64Field(line, "recovery_generation", &recovery_generation) &&
        recovery_generation >= 0) {
        event->recovery_generation = static_cast<std::uint64_t>(recovery_generation);
    }

    int total_volume = 0;
    if (ParseIntField(line, "total_volume", &total_volume)) {
        event->total_volume = total_volume;
    } else {
        event->total_volume = filled_volume;
    }

    double avg_fill_price = 0.0;
    if (ParseDoubleField(line, "avg_fill_price", &avg_fill_price)) {
        event->avg_fill_price = avg_fill_price;
    } else {
        event->avg_fill_price = 0.0;
    }

    return true;
}

bool ParseCtpOrderSubmitMappingLine(const std::string& line, CtpOrderSubmitMapping* mapping) {
    if (mapping == nullptr) {
        return false;
    }
    if (!ParseStringField(line, "client_order_id", &mapping->client_order_id) ||
        !ParseStringField(line, "order_ref", &mapping->order_ref)) {
        return false;
    }

    (void)ParseStringField(line, "run_id", &mapping->run_id);
    (void)ParseStringField(line, "account_id", &mapping->account_id);
    (void)ParseStringField(line, "strategy_id", &mapping->strategy_id);
    (void)ParseStringField(line, "trace_id", &mapping->trace_id);
    (void)ParseStringField(line, "instrument_id", &mapping->instrument_id);
    (void)ParseStringField(line, "exchange_id", &mapping->exchange_id);
    (void)ParseStringField(line, "trading_day", &mapping->trading_day);

    std::int64_t submit_ts_ns = 0;
    if (ParseInt64Field(line, "submit_ts_ns", &submit_ts_ns)) {
        mapping->submit_ts_ns = submit_ts_ns;
    }

    int raw_side = 0;
    if (ParseIntField(line, "side", &raw_side)) {
        (void)ParseSide(raw_side, &mapping->side);
    }
    int raw_offset = 0;
    if (ParseIntField(line, "offset", &raw_offset)) {
        (void)ParseOffset(raw_offset, &mapping->offset);
    }
    int parsed_int = 0;
    if (ParseIntField(line, "volume", &parsed_int)) {
        mapping->volume = parsed_int;
    }
    (void)ParseDoubleField(line, "price", &mapping->price);
    if (ParseIntField(line, "front_id", &parsed_int)) {
        mapping->front_id = parsed_int;
    }
    if (ParseIntField(line, "session_id", &parsed_int)) {
        mapping->session_id = parsed_int;
    }
    if (ParseIntField(line, "request_id", &parsed_int)) {
        mapping->request_id = parsed_int;
    }
    if (ParseIntField(line, "phase", &parsed_int) && parsed_int >= 0 && parsed_int <= 2) {
        mapping->phase = static_cast<OrderSubmitMappingPhase>(parsed_int);
    }
    return true;
}

}  // namespace

WalReplayStats WalReplayLoader::Replay(const std::string& wal_path,
                                       OrderStateMachine* order_state_machine,
                                       IPortfolioLedger* portfolio_ledger,
                                       CtpOrderMappingStore* order_mapping_store) const {
    WalReplayStats stats;

    std::ifstream stream(wal_path);
    if (!stream.is_open()) {
        return stats;
    }

    std::string line;
    while (std::getline(stream, line)) {
        if (Trim(line).empty()) {
            continue;
        }

        ++stats.lines_total;
        if (IsCtpOrderSubmitMapping(line)) {
            CtpOrderSubmitMapping mapping;
            if (!ParseCtpOrderSubmitMappingLine(line, &mapping)) {
                ++stats.parse_errors;
                continue;
            }
            if (order_mapping_store != nullptr) {
                order_mapping_store->Upsert(mapping);
            }
            ++stats.submit_mappings_loaded;
            ++stats.ignored_lines;
            continue;
        }

        bool replayable = false;
        if (!IsReplayableWalKind(line, &replayable)) {
            ++stats.parse_errors;
            continue;
        }
        if (!replayable) {
            ++stats.ignored_lines;
            continue;
        }

        OrderEvent event;
        if (!ParseWalLine(line, &event)) {
            ++stats.parse_errors;
            continue;
        }
        ++stats.events_loaded;

        bool apply_to_ledger = true;
        if (order_state_machine != nullptr && !order_state_machine->RecoverFromOrderEvent(event)) {
            ++stats.state_rejected;
            apply_to_ledger = false;
        }

        if (apply_to_ledger && portfolio_ledger != nullptr) {
            portfolio_ledger->OnOrderEvent(event);
            ++stats.ledger_applied;
        }
    }

    return stats;
}

}  // namespace quant_hft
