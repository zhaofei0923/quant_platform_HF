#include "quant_hft/backtest/product_fee_config_loader.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "quant_hft/core/simple_json.h"

namespace quant_hft {
namespace {

struct EntryBuildFlags {
    bool has_contract_multiplier{false};
    bool has_long_margin_ratio{false};
    bool has_short_margin_ratio{false};
    bool has_open_mode{false};
    bool has_open_value{false};
    bool has_close_mode{false};
    bool has_close_value{false};
    bool has_close_today_mode{false};
    bool has_close_today_value{false};
};

struct CommissionPair {
    bool has_money{false};
    bool has_volume{false};
    double money{0.0};
    double volume{0.0};
};

std::string Trim(const std::string& input) {
    std::size_t begin = 0;
    while (begin < input.size() && std::isspace(static_cast<unsigned char>(input[begin])) != 0) {
        ++begin;
    }
    std::size_t end = input.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(input[end - 1])) != 0) {
        --end;
    }
    return input.substr(begin, end - begin);
}

std::string ToLower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

std::string StripInlineComment(const std::string& input) {
    bool in_single_quote = false;
    bool in_double_quote = false;
    for (std::size_t i = 0; i < input.size(); ++i) {
        const char ch = input[i];
        if (ch == '\'' && !in_double_quote) {
            in_single_quote = !in_single_quote;
            continue;
        }
        if (ch == '"' && !in_single_quote) {
            in_double_quote = !in_double_quote;
            continue;
        }
        if (ch == '#' && !in_single_quote && !in_double_quote) {
            return input.substr(0, i);
        }
    }
    return input;
}

std::string Unquote(std::string value) {
    value = Trim(value);
    if (value.size() >= 2 && ((value.front() == '"' && value.back() == '"') ||
                              (value.front() == '\'' && value.back() == '\''))) {
        return value.substr(1, value.size() - 2);
    }
    return value;
}

bool ParseKeyValue(const std::string& line, std::string* out_key, std::string* out_value) {
    if (out_key == nullptr || out_value == nullptr) {
        return false;
    }
    const std::size_t pos = line.find(':');
    if (pos == std::string::npos) {
        return false;
    }
    *out_key = Unquote(line.substr(0, pos));
    *out_value = Unquote(line.substr(pos + 1));
    return !out_key->empty();
}

bool ParseDoubleText(const std::string& raw, double* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string text = Trim(raw);
    if (text.empty()) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const double parsed = std::stod(text, &consumed);
        if (consumed != text.size()) {
            return false;
        }
        *out = parsed;
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseDoubleValue(const simple_json::Value& value, double* out) {
    if (out == nullptr) {
        return false;
    }
    if (value.IsNumber()) {
        *out = value.number_value;
        return true;
    }
    if (value.IsString()) {
        return ParseDoubleText(value.string_value, out);
    }
    return false;
}

std::string ExtractSymbolPrefix(const std::string& instrument_id) {
    if (instrument_id.empty()) {
        return "";
    }
    std::string core = instrument_id;
    const std::size_t dot = core.rfind('.');
    if (dot != std::string::npos && dot + 1 < core.size()) {
        core = core.substr(dot + 1);
    }

    std::string symbol;
    for (char ch : core) {
        if (std::isalpha(static_cast<unsigned char>(ch)) == 0) {
            break;
        }
        symbol.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
    }
    return symbol;
}

bool ParseFeeMode(const std::string& text, ProductFeeMode* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string mode = ToLower(Trim(text));
    if (mode == "rate") {
        *out = ProductFeeMode::kRate;
        return true;
    }
    if (mode == "per_lot" || mode == "perlot") {
        *out = ProductFeeMode::kPerLot;
        return true;
    }
    return false;
}

bool ParseFeeModeValue(const simple_json::Value& value, ProductFeeMode* out) {
    if (out == nullptr || !value.IsString()) {
        return false;
    }
    return ParseFeeMode(value.string_value, out);
}

std::string FormatLineError(int line_no, const std::string& reason) {
    std::ostringstream oss;
    oss << "line " << line_no << ": " << reason;
    return oss.str();
}

bool ParseCommissionPair(const CommissionPair& pair, ProductFeeMode* out_mode, double* out_value,
                         const std::string& where, const std::string& label, std::string* error) {
    if (out_mode == nullptr || out_value == nullptr) {
        if (error != nullptr) {
            *error = where + ": internal null commission output";
        }
        return false;
    }
    if (!pair.has_money && !pair.has_volume) {
        if (error != nullptr) {
            *error = where + ": missing " + label + " commission fields";
        }
        return false;
    }
    if ((pair.has_money && pair.money > 0.0) && (pair.has_volume && pair.volume > 0.0)) {
        if (error != nullptr) {
            *error = where + ": " + label + " commission money and volume cannot both be positive";
        }
        return false;
    }
    if (pair.has_money && pair.money > 0.0) {
        *out_mode = ProductFeeMode::kRate;
        *out_value = pair.money;
        return true;
    }
    *out_mode = ProductFeeMode::kPerLot;
    *out_value = pair.has_volume ? pair.volume : 0.0;
    return true;
}

bool ApplyCommissionFromPairs(ProductFeeEntry* entry, EntryBuildFlags* flags,
                              const CommissionPair& open, const CommissionPair& close,
                              const CommissionPair& close_today, const std::string& where,
                              std::string* error) {
    if (entry == nullptr || flags == nullptr) {
        if (error != nullptr) {
            *error = where + ": internal null commission state";
        }
        return false;
    }
    if (!ParseCommissionPair(open, &entry->open_mode, &entry->open_value, where, "open", error)) {
        return false;
    }
    flags->has_open_mode = true;
    flags->has_open_value = true;

    if (!ParseCommissionPair(close, &entry->close_mode, &entry->close_value, where, "close",
                             error)) {
        return false;
    }
    flags->has_close_mode = true;
    flags->has_close_value = true;

    if (!ParseCommissionPair(close_today, &entry->close_today_mode, &entry->close_today_value,
                             where, "close_today", error)) {
        return false;
    }
    flags->has_close_today_mode = true;
    flags->has_close_today_value = true;
    return true;
}

bool ValidateEntry(ProductFeeEntry* entry, const EntryBuildFlags& flags, const std::string& where,
                   std::string* error) {
    if (entry == nullptr) {
        if (error != nullptr) {
            *error = where + ": entry is null";
        }
        return false;
    }
    if (entry->instrument_id.empty()) {
        if (error != nullptr) {
            *error = where + ": instrument_id is required";
        }
        return false;
    }
    if (entry->symbol.empty()) {
        entry->symbol = ExtractSymbolPrefix(entry->instrument_id);
    } else {
        entry->symbol = ToLower(entry->symbol);
    }
    if (entry->symbol.empty()) {
        if (error != nullptr) {
            *error = where + ": symbol is required";
        }
        return false;
    }
    if (!flags.has_contract_multiplier || !(entry->contract_multiplier > 0.0)) {
        if (error != nullptr) {
            *error = where + ": contract_multiplier must be > 0";
        }
        return false;
    }
    if (!flags.has_long_margin_ratio || !std::isfinite(entry->long_margin_ratio) ||
        entry->long_margin_ratio <= 0.0) {
        if (error != nullptr) {
            *error = where + ": long_margin_ratio must be > 0";
        }
        return false;
    }
    if (!flags.has_short_margin_ratio || !std::isfinite(entry->short_margin_ratio) ||
        entry->short_margin_ratio <= 0.0) {
        if (error != nullptr) {
            *error = where + ": short_margin_ratio must be > 0";
        }
        return false;
    }
    if (!flags.has_open_mode || !flags.has_open_value || !flags.has_close_mode ||
        !flags.has_close_value || !flags.has_close_today_mode || !flags.has_close_today_value) {
        if (error != nullptr) {
            *error = where + ": missing commission fields";
        }
        return false;
    }
    if (entry->open_value < 0.0 || entry->close_value < 0.0 || entry->close_today_value < 0.0) {
        if (error != nullptr) {
            *error = where + ": commission value must be non-negative";
        }
        return false;
    }
    return true;
}

bool ParseEntryField(ProductFeeEntry* entry, EntryBuildFlags* flags, const std::string& key,
                     const std::string& value, const std::string& where, std::string* error) {
    if (entry == nullptr || flags == nullptr) {
        if (error != nullptr) {
            *error = where + ": internal null state";
        }
        return false;
    }
    if (key == "symbol") {
        entry->symbol = ToLower(value);
        return true;
    }
    if (key == "product") {
        entry->symbol = ToLower(value);
        return true;
    }
    if (key == "contract_multiplier") {
        double parsed = 0.0;
        if (!ParseDoubleText(value, &parsed)) {
            if (error != nullptr) {
                *error = where + ": invalid contract_multiplier";
            }
            return false;
        }
        entry->contract_multiplier = parsed;
        flags->has_contract_multiplier = true;
        return true;
    }
    if (key == "volume_multiple") {
        double parsed = 0.0;
        if (!ParseDoubleText(value, &parsed)) {
            if (error != nullptr) {
                *error = where + ": invalid volume_multiple";
            }
            return false;
        }
        entry->contract_multiplier = parsed;
        flags->has_contract_multiplier = true;
        return true;
    }
    if (key == "long_margin_ratio") {
        double parsed = 0.0;
        if (!ParseDoubleText(value, &parsed)) {
            if (error != nullptr) {
                *error = where + ": invalid long_margin_ratio";
            }
            return false;
        }
        entry->long_margin_ratio = parsed;
        flags->has_long_margin_ratio = true;
        return true;
    }
    if (key == "short_margin_ratio") {
        double parsed = 0.0;
        if (!ParseDoubleText(value, &parsed)) {
            if (error != nullptr) {
                *error = where + ": invalid short_margin_ratio";
            }
            return false;
        }
        entry->short_margin_ratio = parsed;
        flags->has_short_margin_ratio = true;
        return true;
    }
    if (key == "open_mode") {
        ProductFeeMode mode = ProductFeeMode::kRate;
        if (!ParseFeeMode(value, &mode)) {
            if (error != nullptr) {
                *error = where + ": invalid open_mode";
            }
            return false;
        }
        entry->open_mode = mode;
        flags->has_open_mode = true;
        return true;
    }
    if (key == "open_value") {
        double parsed = 0.0;
        if (!ParseDoubleText(value, &parsed)) {
            if (error != nullptr) {
                *error = where + ": invalid open_value";
            }
            return false;
        }
        entry->open_value = parsed;
        flags->has_open_value = true;
        return true;
    }
    if (key == "close_mode") {
        ProductFeeMode mode = ProductFeeMode::kRate;
        if (!ParseFeeMode(value, &mode)) {
            if (error != nullptr) {
                *error = where + ": invalid close_mode";
            }
            return false;
        }
        entry->close_mode = mode;
        flags->has_close_mode = true;
        return true;
    }
    if (key == "close_value") {
        double parsed = 0.0;
        if (!ParseDoubleText(value, &parsed)) {
            if (error != nullptr) {
                *error = where + ": invalid close_value";
            }
            return false;
        }
        entry->close_value = parsed;
        flags->has_close_value = true;
        return true;
    }
    if (key == "close_today_mode") {
        ProductFeeMode mode = ProductFeeMode::kRate;
        if (!ParseFeeMode(value, &mode)) {
            if (error != nullptr) {
                *error = where + ": invalid close_today_mode";
            }
            return false;
        }
        entry->close_today_mode = mode;
        flags->has_close_today_mode = true;
        return true;
    }
    if (key == "close_today_value") {
        double parsed = 0.0;
        if (!ParseDoubleText(value, &parsed)) {
            if (error != nullptr) {
                *error = where + ": invalid close_today_value";
            }
            return false;
        }
        entry->close_today_value = parsed;
        flags->has_close_today_value = true;
        return true;
    }
    if (error != nullptr) {
        *error = where + ": unsupported field: " + key;
    }
    return false;
}

bool LoadProductFeeYaml(const std::filesystem::path& path, ProductFeeBook* out,
                        std::string* error) {
    std::ifstream input(path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open product fee config: " + path.string();
        }
        return false;
    }

    ProductFeeBook book;
    bool in_products = false;
    ProductFeeEntry current;
    EntryBuildFlags current_flags;
    bool has_current = false;
    bool in_commission_section = false;
    bool in_trading_sessions_section = false;
    CommissionPair open_pair;
    CommissionPair close_pair;
    CommissionPair close_today_pair;
    std::string raw_line;
    int line_no = 0;

    const auto flush_current = [&](int line_for_error) -> bool {
        if (!has_current) {
            return true;
        }
        const std::string where =
            FormatLineError(line_for_error, "product `" + current.instrument_id + "`");
        if (in_commission_section || open_pair.has_money || open_pair.has_volume ||
            close_pair.has_money || close_pair.has_volume || close_today_pair.has_money ||
            close_today_pair.has_volume) {
            std::string local_error;
            if (!ApplyCommissionFromPairs(&current, &current_flags, open_pair, close_pair,
                                          close_today_pair, where, &local_error)) {
                if (error != nullptr) {
                    *error = local_error;
                }
                return false;
            }
        }
        std::string local_error;
        if (!ValidateEntry(&current, current_flags, where, &local_error)) {
            if (error != nullptr) {
                *error = local_error;
            }
            return false;
        }
        if (!book.Upsert(current, &local_error)) {
            if (error != nullptr) {
                *error = local_error;
            }
            return false;
        }
        current = ProductFeeEntry{};
        current_flags = EntryBuildFlags{};
        has_current = false;
        in_commission_section = false;
        in_trading_sessions_section = false;
        open_pair = CommissionPair{};
        close_pair = CommissionPair{};
        close_today_pair = CommissionPair{};
        return true;
    };

    while (std::getline(input, raw_line)) {
        ++line_no;
        const std::string cleaned = StripInlineComment(raw_line);
        const std::size_t first_non_space = cleaned.find_first_not_of(' ');
        if (first_non_space == std::string::npos) {
            continue;
        }
        const int indent = static_cast<int>(first_non_space);
        const std::string text = Trim(cleaned);
        if (text.empty()) {
            continue;
        }

        if (!text.empty() && text.front() == '-') {
            if (in_trading_sessions_section && indent >= 6) {
                continue;
            }
            if (error != nullptr) {
                *error = FormatLineError(line_no, "unexpected list item");
            }
            return false;
        }

        std::string key;
        std::string value;
        if (!ParseKeyValue(text, &key, &value)) {
            if (error != nullptr) {
                *error = FormatLineError(line_no, "invalid key/value entry");
            }
            return false;
        }

        if (indent == 0) {
            if (key != "products") {
                if (error != nullptr) {
                    *error = FormatLineError(line_no, "unsupported top-level field: " + key);
                }
                return false;
            }
            if (!value.empty()) {
                if (error != nullptr) {
                    *error = FormatLineError(line_no, "products must be a YAML section");
                }
                return false;
            }
            in_products = true;
            continue;
        }

        if (!in_products) {
            if (error != nullptr) {
                *error = FormatLineError(line_no, "products section is required");
            }
            return false;
        }

        if (indent == 2) {
            if (!value.empty()) {
                if (error != nullptr) {
                    *error = FormatLineError(line_no, "instrument entry must be a YAML section");
                }
                return false;
            }
            if (!flush_current(line_no)) {
                return false;
            }
            current = ProductFeeEntry{};
            current.instrument_id = key;
            current_flags = EntryBuildFlags{};
            has_current = true;
            in_commission_section = false;
            in_trading_sessions_section = false;
            open_pair = CommissionPair{};
            close_pair = CommissionPair{};
            close_today_pair = CommissionPair{};
            continue;
        }

        if (indent == 4) {
            if (!has_current) {
                if (error != nullptr) {
                    *error = FormatLineError(line_no, "field appears before instrument section");
                }
                return false;
            }
            if (key == "commission") {
                if (!value.empty()) {
                    if (error != nullptr) {
                        *error = FormatLineError(line_no, "commission must be a YAML section");
                    }
                    return false;
                }
                in_commission_section = true;
                in_trading_sessions_section = false;
                continue;
            }
            if (key == "trading_sessions") {
                if (!value.empty()) {
                    if (error != nullptr) {
                        *error =
                            FormatLineError(line_no, "trading_sessions must be a YAML section");
                    }
                    return false;
                }
                in_commission_section = false;
                in_trading_sessions_section = true;
                continue;
            }
            in_commission_section = false;
            in_trading_sessions_section = false;
            const std::string where =
                FormatLineError(line_no, "product `" + current.instrument_id + "`");
            if (!ParseEntryField(&current, &current_flags, key, value, where, error)) {
                return false;
            }
            continue;
        }

        if (indent == 6 && in_commission_section) {
            double parsed = 0.0;
            if (!ParseDoubleText(value, &parsed)) {
                if (error != nullptr) {
                    *error = FormatLineError(line_no, "invalid commission value for key: " + key);
                }
                return false;
            }
            if (key == "open_ratio_by_money") {
                open_pair.money = parsed;
                open_pair.has_money = true;
                continue;
            }
            if (key == "open_ratio_by_volume") {
                open_pair.volume = parsed;
                open_pair.has_volume = true;
                continue;
            }
            if (key == "close_ratio_by_money") {
                close_pair.money = parsed;
                close_pair.has_money = true;
                continue;
            }
            if (key == "close_ratio_by_volume") {
                close_pair.volume = parsed;
                close_pair.has_volume = true;
                continue;
            }
            if (key == "close_today_ratio_by_money") {
                close_today_pair.money = parsed;
                close_today_pair.has_money = true;
                continue;
            }
            if (key == "close_today_ratio_by_volume") {
                close_today_pair.volume = parsed;
                close_today_pair.has_volume = true;
                continue;
            }
            if (error != nullptr) {
                *error = FormatLineError(line_no, "unsupported commission field: " + key);
            }
            return false;
        }

        if (indent == 6 && in_trading_sessions_section) {
            if (error != nullptr) {
                *error = FormatLineError(line_no, "trading_sessions only supports list items");
            }
            return false;
        }

        if (error != nullptr) {
            *error = FormatLineError(line_no, "unsupported indentation level");
        }
        return false;
    }

    if (input.bad()) {
        if (error != nullptr) {
            *error = "failed reading product fee config: " + path.string();
        }
        return false;
    }
    if (!in_products) {
        if (error != nullptr) {
            *error = "line 1: products section is required";
        }
        return false;
    }
    if (!flush_current(line_no > 0 ? line_no : 1)) {
        return false;
    }

    *out = std::move(book);
    return true;
}

bool ParseEntryFieldFromJson(ProductFeeEntry* entry, EntryBuildFlags* flags, const std::string& key,
                             const simple_json::Value& value, const std::string& where,
                             std::string* error) {
    if (entry == nullptr || flags == nullptr) {
        if (error != nullptr) {
            *error = where + ": internal null state";
        }
        return false;
    }
    if (key == "symbol") {
        if (!value.IsString()) {
            if (error != nullptr) {
                *error = where + ": symbol must be string";
            }
            return false;
        }
        entry->symbol = ToLower(value.string_value);
        return true;
    }
    if (key == "product") {
        if (!value.IsString()) {
            if (error != nullptr) {
                *error = where + ": product must be string";
            }
            return false;
        }
        entry->symbol = ToLower(value.string_value);
        return true;
    }
    if (key == "trading_sessions") {
        if (!value.IsArray()) {
            if (error != nullptr) {
                *error = where + ": trading_sessions must be array";
            }
            return false;
        }
        return true;
    }
    if (key == "contract_multiplier") {
        double parsed = 0.0;
        if (!ParseDoubleValue(value, &parsed)) {
            if (error != nullptr) {
                *error = where + ": invalid contract_multiplier";
            }
            return false;
        }
        entry->contract_multiplier = parsed;
        flags->has_contract_multiplier = true;
        return true;
    }
    if (key == "volume_multiple") {
        double parsed = 0.0;
        if (!ParseDoubleValue(value, &parsed)) {
            if (error != nullptr) {
                *error = where + ": invalid volume_multiple";
            }
            return false;
        }
        entry->contract_multiplier = parsed;
        flags->has_contract_multiplier = true;
        return true;
    }
    if (key == "long_margin_ratio") {
        double parsed = 0.0;
        if (!ParseDoubleValue(value, &parsed)) {
            if (error != nullptr) {
                *error = where + ": invalid long_margin_ratio";
            }
            return false;
        }
        entry->long_margin_ratio = parsed;
        flags->has_long_margin_ratio = true;
        return true;
    }
    if (key == "short_margin_ratio") {
        double parsed = 0.0;
        if (!ParseDoubleValue(value, &parsed)) {
            if (error != nullptr) {
                *error = where + ": invalid short_margin_ratio";
            }
            return false;
        }
        entry->short_margin_ratio = parsed;
        flags->has_short_margin_ratio = true;
        return true;
    }
    if (key == "open_mode") {
        ProductFeeMode mode = ProductFeeMode::kRate;
        if (!ParseFeeModeValue(value, &mode)) {
            if (error != nullptr) {
                *error = where + ": invalid open_mode";
            }
            return false;
        }
        entry->open_mode = mode;
        flags->has_open_mode = true;
        return true;
    }
    if (key == "open_value") {
        double parsed = 0.0;
        if (!ParseDoubleValue(value, &parsed)) {
            if (error != nullptr) {
                *error = where + ": invalid open_value";
            }
            return false;
        }
        entry->open_value = parsed;
        flags->has_open_value = true;
        return true;
    }
    if (key == "close_mode") {
        ProductFeeMode mode = ProductFeeMode::kRate;
        if (!ParseFeeModeValue(value, &mode)) {
            if (error != nullptr) {
                *error = where + ": invalid close_mode";
            }
            return false;
        }
        entry->close_mode = mode;
        flags->has_close_mode = true;
        return true;
    }
    if (key == "close_value") {
        double parsed = 0.0;
        if (!ParseDoubleValue(value, &parsed)) {
            if (error != nullptr) {
                *error = where + ": invalid close_value";
            }
            return false;
        }
        entry->close_value = parsed;
        flags->has_close_value = true;
        return true;
    }
    if (key == "close_today_mode") {
        ProductFeeMode mode = ProductFeeMode::kRate;
        if (!ParseFeeModeValue(value, &mode)) {
            if (error != nullptr) {
                *error = where + ": invalid close_today_mode";
            }
            return false;
        }
        entry->close_today_mode = mode;
        flags->has_close_today_mode = true;
        return true;
    }
    if (key == "close_today_value") {
        double parsed = 0.0;
        if (!ParseDoubleValue(value, &parsed)) {
            if (error != nullptr) {
                *error = where + ": invalid close_today_value";
            }
            return false;
        }
        entry->close_today_value = parsed;
        flags->has_close_today_value = true;
        return true;
    }
    if (key == "commission") {
        if (!value.IsObject()) {
            if (error != nullptr) {
                *error = where + ": commission must be object";
            }
            return false;
        }
        CommissionPair open_pair;
        CommissionPair close_pair;
        CommissionPair close_today_pair;
        for (const auto& [commission_key, commission_value] : value.object_value) {
            double parsed = 0.0;
            if (!ParseDoubleValue(commission_value, &parsed)) {
                if (error != nullptr) {
                    *error = where + ": invalid commission field `" + commission_key + "`";
                }
                return false;
            }
            if (commission_key == "open_ratio_by_money") {
                open_pair.money = parsed;
                open_pair.has_money = true;
            } else if (commission_key == "open_ratio_by_volume") {
                open_pair.volume = parsed;
                open_pair.has_volume = true;
            } else if (commission_key == "close_ratio_by_money") {
                close_pair.money = parsed;
                close_pair.has_money = true;
            } else if (commission_key == "close_ratio_by_volume") {
                close_pair.volume = parsed;
                close_pair.has_volume = true;
            } else if (commission_key == "close_today_ratio_by_money") {
                close_today_pair.money = parsed;
                close_today_pair.has_money = true;
            } else if (commission_key == "close_today_ratio_by_volume") {
                close_today_pair.volume = parsed;
                close_today_pair.has_volume = true;
            } else {
                if (error != nullptr) {
                    *error = where + ": unsupported commission field: " + commission_key;
                }
                return false;
            }
        }
        return ApplyCommissionFromPairs(entry, flags, open_pair, close_pair, close_today_pair,
                                        where, error);
    }
    if (error != nullptr) {
        *error = where + ": unsupported field: " + key;
    }
    return false;
}

bool LoadProductFeeJson(const std::filesystem::path& path, ProductFeeBook* out,
                        std::string* error) {
    std::ifstream input(path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open product fee config: " + path.string();
        }
        return false;
    }
    std::ostringstream buffer;
    buffer << input.rdbuf();
    if (input.bad()) {
        if (error != nullptr) {
            *error = "failed reading product fee config: " + path.string();
        }
        return false;
    }

    simple_json::Value root;
    if (!simple_json::Parse(buffer.str(), &root, error)) {
        return false;
    }
    if (!root.IsObject()) {
        if (error != nullptr) {
            *error = "product fee json root must be object";
        }
        return false;
    }
    const simple_json::Value* products = root.Find("products");
    const std::map<std::string, simple_json::Value>* products_object = nullptr;
    if (products != nullptr) {
        if (!products->IsObject()) {
            if (error != nullptr) {
                *error = "product fee json field `products` must be object";
            }
            return false;
        }
        products_object = &products->object_value;
    } else {
        products_object = &root.object_value;
    }

    ProductFeeBook book;
    for (const auto& [instrument_id, product_node] : *products_object) {
        if (!product_node.IsObject()) {
            if (error != nullptr) {
                *error = "product `" + instrument_id + "` must be object";
            }
            return false;
        }

        ProductFeeEntry entry;
        entry.instrument_id = instrument_id;
        EntryBuildFlags flags;
        for (const auto& [field, value] : product_node.object_value) {
            if (!ParseEntryFieldFromJson(&entry, &flags, field, value,
                                         "product `" + instrument_id + "`", error)) {
                return false;
            }
        }
        std::string local_error;
        if (!ValidateEntry(&entry, flags, "product `" + instrument_id + "`", &local_error)) {
            if (error != nullptr) {
                *error = local_error;
            }
            return false;
        }
        if (!book.Upsert(entry, &local_error)) {
            if (error != nullptr) {
                *error = local_error;
            }
            return false;
        }
    }

    *out = std::move(book);
    return true;
}

}  // namespace

void ProductFeeBook::Clear() {
    entries_by_instrument_.clear();
    symbol_to_instrument_.clear();
}

bool ProductFeeBook::Upsert(const ProductFeeEntry& entry, std::string* error) {
    if (entry.instrument_id.empty()) {
        if (error != nullptr) {
            *error = "product fee entry instrument_id is required";
        }
        return false;
    }
    ProductFeeEntry normalized = entry;
    normalized.symbol = ToLower(normalized.symbol);
    entries_by_instrument_[normalized.instrument_id] = normalized;
    if (!normalized.symbol.empty() && symbol_to_instrument_.count(normalized.symbol) == 0) {
        symbol_to_instrument_[normalized.symbol] = normalized.instrument_id;
    }
    return true;
}

const ProductFeeEntry* ProductFeeBook::Find(const std::string& instrument_id) const {
    auto it = entries_by_instrument_.find(instrument_id);
    if (it != entries_by_instrument_.end()) {
        return &it->second;
    }

    const std::string symbol = ExtractSymbolPrefix(instrument_id);
    if (symbol.empty()) {
        return nullptr;
    }
    const auto symbol_it = symbol_to_instrument_.find(symbol);
    if (symbol_it == symbol_to_instrument_.end()) {
        return nullptr;
    }
    it = entries_by_instrument_.find(symbol_it->second);
    return it == entries_by_instrument_.end() ? nullptr : &it->second;
}

bool ProductFeeBook::ExportContractMultipliers(
    std::unordered_map<std::string, double>* multipliers_by_instrument) const {
    if (multipliers_by_instrument == nullptr) {
        return false;
    }
    multipliers_by_instrument->clear();
    multipliers_by_instrument->reserve(entries_by_instrument_.size());
    for (const auto& [instrument_id, entry] : entries_by_instrument_) {
        if (std::isfinite(entry.contract_multiplier) && entry.contract_multiplier > 0.0) {
            (*multipliers_by_instrument)[instrument_id] = entry.contract_multiplier;
        }
    }
    return true;
}

double ProductFeeBook::ComputeCommission(const ProductFeeEntry& entry, OffsetFlag offset,
                                         std::int32_t volume, double fill_price) {
    if (volume <= 0 || !std::isfinite(fill_price) || fill_price <= 0.0 ||
        !std::isfinite(entry.contract_multiplier) || entry.contract_multiplier <= 0.0) {
        return 0.0;
    }

    ProductFeeMode mode = entry.close_mode;
    double value = entry.close_value;
    if (offset == OffsetFlag::kOpen) {
        mode = entry.open_mode;
        value = entry.open_value;
    } else if (offset == OffsetFlag::kCloseToday) {
        mode = entry.close_today_mode;
        value = entry.close_today_value;
    }

    value = std::max(0.0, value);
    if (mode == ProductFeeMode::kPerLot) {
        return value * static_cast<double>(volume);
    }

    const double notional =
        fill_price * static_cast<double>(volume) * std::max(0.0, entry.contract_multiplier);
    return std::max(0.0, notional * value);
}

double ProductFeeBook::ComputePerLotMargin(const ProductFeeEntry& entry, Side side,
                                           double fill_price) {
    if (!std::isfinite(fill_price) || fill_price <= 0.0 ||
        !std::isfinite(entry.contract_multiplier) || entry.contract_multiplier <= 0.0) {
        return 0.0;
    }
    const double margin_ratio =
        side == Side::kBuy ? entry.long_margin_ratio : entry.short_margin_ratio;
    if (!std::isfinite(margin_ratio) || margin_ratio <= 0.0) {
        return 0.0;
    }
    return fill_price * entry.contract_multiplier * margin_ratio;
}

double ProductFeeBook::ComputeRequiredMargin(const ProductFeeEntry& entry, Side side,
                                             std::int32_t volume, double fill_price) {
    if (volume <= 0) {
        return 0.0;
    }
    const double per_lot = ComputePerLotMargin(entry, side, fill_price);
    if (!std::isfinite(per_lot) || per_lot <= 0.0) {
        return 0.0;
    }
    return per_lot * static_cast<double>(volume);
}

bool LoadProductFeeConfig(const std::string& path, ProductFeeBook* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "product fee config output is null";
        }
        return false;
    }
    if (path.empty()) {
        if (error != nullptr) {
            *error = "product fee config path is empty";
        }
        return false;
    }

    const std::filesystem::path config_path(path);
    const std::string ext = ToLower(config_path.extension().string());
    if (ext == ".json") {
        return LoadProductFeeJson(config_path, out, error);
    }
    if (ext == ".yaml" || ext == ".yml") {
        return LoadProductFeeYaml(config_path, out, error);
    }

    if (error != nullptr) {
        *error = "unsupported product fee config extension: " + ext;
    }
    return false;
}

}  // namespace quant_hft
