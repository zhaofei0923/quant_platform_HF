#include <cctype>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <regex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

std::string Trim(std::string text) {
    const auto is_space = [](unsigned char ch) { return std::isspace(ch) != 0; };
    while (!text.empty() && is_space(static_cast<unsigned char>(text.front()))) {
        text.erase(text.begin());
    }
    while (!text.empty() && is_space(static_cast<unsigned char>(text.back()))) {
        text.pop_back();
    }
    return text;
}

std::string ReadText(const std::string& path) {
    std::ifstream input(path);
    if (!input.is_open()) {
        throw std::runtime_error("unable to read file: " + path);
    }
    std::ostringstream buffer;
    buffer << input.rdbuf();
    return buffer.str();
}

std::string ExtractBlock(const std::string& text, const std::string& marker) {
    const std::size_t start = text.find(marker);
    if (start == std::string::npos) {
        throw std::runtime_error("missing block marker: " + marker);
    }
    const std::size_t open_pos = text.find('{', start);
    if (open_pos == std::string::npos) {
        throw std::runtime_error("missing opening brace for marker: " + marker);
    }

    int depth = 0;
    for (std::size_t index = open_pos; index < text.size(); ++index) {
        if (text[index] == '{') {
            ++depth;
        } else if (text[index] == '}') {
            --depth;
            if (depth == 0) {
                return text.substr(open_pos + 1, index - open_pos - 1);
            }
        }
    }
    throw std::runtime_error("missing closing brace for marker: " + marker);
}

std::vector<std::string> ParseCppStructFields(const std::string& path, const std::string& name) {
    const std::string text = ReadText(path);
    const std::string block = ExtractBlock(text, "struct " + name + " {");

    std::vector<std::string> fields;
    std::istringstream lines(block);
    std::string raw_line;
    const std::regex field_pattern(R"(([A-Za-z_][A-Za-z0-9_]*)\s*(?:\{[^;]*\})?\s*;$)");

    while (std::getline(lines, raw_line)) {
        const std::size_t comment_pos = raw_line.find("//");
        std::string line = Trim(raw_line.substr(0, comment_pos));
        if (line.empty()) {
            continue;
        }
        if (line.find('(') != std::string::npos || line.rfind("struct ", 0) == 0) {
            continue;
        }
        if (line.back() != ';') {
            continue;
        }

        std::smatch matched;
        if (std::regex_search(line, matched, field_pattern) && matched.size() > 1) {
            fields.push_back(matched[1]);
        }
    }
    return fields;
}

std::vector<std::string> ParseProtoMessageFields(const std::string& path, const std::string& name) {
    const std::string text = ReadText(path);
    const std::string block = ExtractBlock(text, "message " + name + " {");

    std::vector<std::string> fields;
    std::istringstream lines(block);
    std::string raw_line;
    const std::regex field_pattern(R"([A-Za-z0-9_.<>]+\s+([A-Za-z_][A-Za-z0-9_]*)\s*=)");

    while (std::getline(lines, raw_line)) {
        const std::size_t comment_pos = raw_line.find("//");
        std::string line = Trim(raw_line.substr(0, comment_pos));
        if (line.empty()) {
            continue;
        }
        std::smatch matched;
        if (std::regex_search(line, matched, field_pattern) && matched.size() > 1) {
            fields.push_back(matched[1]);
        }
    }
    return fields;
}

std::vector<std::string> NormalizeFields(const std::string& contract, const std::string& source,
                                         const std::vector<std::string>& fields) {
    std::vector<std::string> normalized;
    normalized.reserve(fields.size());
    for (const std::string& field : fields) {
        if (contract == "OrderIntent" && source == "proto" && field == "order_type") {
            normalized.push_back("type");
        } else {
            normalized.push_back(field);
        }
    }
    return normalized;
}

bool AssertFieldSetEqual(const std::vector<std::string>& actual,
                         const std::vector<std::string>& expected, const std::string& contract,
                         const std::string& source, std::string* error) {
    const std::set<std::string> actual_set(actual.begin(), actual.end());
    const std::set<std::string> expected_set(expected.begin(), expected.end());
    if (actual_set == expected_set) {
        return true;
    }

    std::ostringstream oss;
    oss << contract << " mismatch in " << source << ": expected={";
    bool first = true;
    for (const std::string& field : expected_set) {
        if (!first) {
            oss << ",";
        }
        first = false;
        oss << field;
    }
    oss << "} actual={";
    first = true;
    for (const std::string& field : actual_set) {
        if (!first) {
            oss << ",";
        }
        first = false;
        oss << field;
    }
    oss << "}";
    if (error != nullptr) {
        *error = oss.str();
    }
    return false;
}

}  // namespace

int main() {
    try {
        const std::map<std::string, std::vector<std::string>> expected_fields = {
            {"Exchange", {"id", "name"}},
            {"Instrument",
             {"symbol", "exchange_id", "product_id", "contract_multiplier", "price_tick",
              "margin_rate", "commission_rate", "commission_type", "close_today_commission_rate"}},
            {"Tick",
             {"symbol", "exchange", "ts_ns", "exchange_ts_ns", "last_price", "last_volume",
              "ask_price1", "ask_volume1", "bid_price1", "bid_volume1", "volume", "turnover",
              "open_interest"}},
            {"Bar",
             {"symbol", "exchange", "timeframe", "ts_ns", "open", "high", "low", "close", "volume",
              "turnover", "open_interest"}},
            {"Order",
             {"order_id", "account_id", "strategy_id", "symbol", "exchange", "side", "offset",
              "order_type", "price", "quantity", "filled_quantity", "avg_fill_price", "status",
              "created_at_ns", "updated_at_ns", "commission", "message"}},
            {"Trade",
             {"trade_id", "order_id", "account_id", "strategy_id", "symbol", "exchange", "side",
              "offset", "price", "quantity", "trade_ts_ns", "commission", "profit"}},
            {"Position",
             {"symbol", "exchange", "strategy_id", "account_id", "long_qty", "short_qty",
              "long_today_qty", "short_today_qty", "long_yd_qty", "short_yd_qty", "avg_long_price",
              "avg_short_price", "position_profit", "margin", "update_time_ns"}},
            {"Account",
             {"account_id", "balance", "available", "margin", "commission", "position_profit",
              "close_profit", "risk_degree", "update_time_ns"}},
            {"MarketSnapshot",
             {"instrument_id", "exchange_id", "trading_day", "action_day", "update_time",
              "update_millisec", "last_price", "bid_price_1", "ask_price_1", "bid_volume_1",
              "ask_volume_1", "volume", "settlement_price", "average_price_raw",
              "average_price_norm", "is_valid_settlement", "exchange_ts_ns", "recv_ts_ns"}},
            {"RiskDecision",
             {"action", "rule_id", "rule_group", "rule_version", "policy_id", "policy_scope",
              "observed_value", "threshold_value", "decision_tags", "reason", "decision_ts_ns"}},
            {"OrderEvent",
             {"account_id",
              "strategy_id",
              "client_order_id",
              "exchange_order_id",
              "instrument_id",
              "exchange_id",
              "side",
              "offset",
              "status",
              "total_volume",
              "filled_volume",
              "avg_fill_price",
              "reason",
              "status_msg",
              "order_submit_status",
              "order_ref",
              "front_id",
              "session_id",
              "trade_id",
              "event_source",
              "exchange_ts_ns",
              "recv_ts_ns",
              "ts_ns",
              "trace_id",
              "execution_algo_id",
              "slice_index",
              "slice_total",
              "throttle_applied",
              "venue",
              "route_id",
              "slippage_bps",
              "impact_cost"}},
            {"OrderIntent",
             {"account_id", "client_order_id", "strategy_id", "instrument_id", "side", "offset",
              "hedge_flag", "type", "time_condition", "volume_condition", "volume", "price",
              "ts_ns", "trace_id"}},
            {"TradingAccountSnapshot",
             {"account_id", "investor_id", "balance", "available", "curr_margin", "frozen_margin",
              "frozen_cash", "frozen_commission", "commission", "close_profit", "position_profit",
              "trading_day", "ts_ns", "source"}},
            {"InvestorPositionSnapshot", {"account_id",
                                          "investor_id",
                                          "instrument_id",
                                          "exchange_id",
                                          "posi_direction",
                                          "hedge_flag",
                                          "position_date",
                                          "position",
                                          "today_position",
                                          "yd_position",
                                          "long_frozen",
                                          "short_frozen",
                                          "open_volume",
                                          "close_volume",
                                          "position_cost",
                                          "open_cost",
                                          "position_profit",
                                          "close_profit",
                                          "margin_rate_by_money",
                                          "margin_rate_by_volume",
                                          "use_margin",
                                          "ts_ns",
                                          "source"}},
            {"BrokerTradingParamsSnapshot",
             {"account_id", "investor_id", "margin_price_type", "algorithm", "ts_ns", "source"}},
            {"InstrumentMetaSnapshot",
             {"instrument_id", "exchange_id", "product_id", "volume_multiple", "price_tick",
              "max_margin_side_algorithm", "ts_ns", "source"}},
        };

        const auto resolve_path = [](const std::string& relative_path) {
            const std::filesystem::path cwd = std::filesystem::current_path();
            const std::filesystem::path direct = cwd / relative_path;
            if (std::filesystem::exists(direct)) {
                return direct.string();
            }
            const std::filesystem::path parent = cwd.parent_path() / relative_path;
            if (std::filesystem::exists(parent)) {
                return parent.string();
            }
            return direct.string();
        };

        const std::string cpp_path = resolve_path("include/quant_hft/contracts/types.h");
        const std::string proto_path = resolve_path("proto/quant_hft/v1/contracts.proto");

        for (const auto& [contract, expected] : expected_fields) {
            const std::vector<std::string> cpp_fields =
                NormalizeFields(contract, "C++", ParseCppStructFields(cpp_path, contract));
            const std::vector<std::string> proto_fields =
                NormalizeFields(contract, "proto", ParseProtoMessageFields(proto_path, contract));

            std::string error;
            if (!AssertFieldSetEqual(cpp_fields, expected, contract, "C++", &error) ||
                !AssertFieldSetEqual(proto_fields, expected, contract, "proto", &error)) {
                std::cerr << "verify_contract_sync_cli: " << error << '\n';
                return 2;
            }
        }

        std::cout << "contract sync verification passed\n";
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "verify_contract_sync_cli: " << ex.what() << '\n';
        return 2;
    }
}
