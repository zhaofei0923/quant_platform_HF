#include "quant_hft/backtest/contract_expiry_calendar.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <string>

namespace quant_hft {
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

std::string StripInlineComment(const std::string& text) {
    std::string out;
    bool in_single_quote = false;
    bool in_double_quote = false;
    for (char ch : text) {
        if (ch == '\'' && !in_double_quote) {
            in_single_quote = !in_single_quote;
        } else if (ch == '"' && !in_single_quote) {
            in_double_quote = !in_double_quote;
        } else if (ch == '#' && !in_single_quote && !in_double_quote) {
            break;
        }
        out.push_back(ch);
    }
    return out;
}

bool ParseKeyValue(const std::string& text, std::string* key, std::string* value) {
    if (key == nullptr || value == nullptr) {
        return false;
    }
    const std::size_t colon = text.find(':');
    if (colon == std::string::npos) {
        return false;
    }
    *key = Trim(text.substr(0, colon));
    *value = Trim(text.substr(colon + 1));
    return !key->empty();
}

bool IsNormalizedTradingDay(const std::string& text) {
    if (text.size() != 8) {
        return false;
    }
    return std::all_of(text.begin(), text.end(), [](unsigned char ch) { return std::isdigit(ch) != 0; });
}

}  // namespace

std::string CanonicalContractInstrumentId(const std::string& instrument_id) {
    std::string normalized = Trim(instrument_id);
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return normalized;
}

void ContractExpiryCalendar::Clear() { entries_by_instrument_.clear(); }

bool ContractExpiryCalendar::Upsert(const ContractExpiryEntry& entry, std::string* error) {
    const std::string instrument_id = CanonicalContractInstrumentId(entry.instrument_id);
    if (instrument_id.empty()) {
        if (error != nullptr) {
            *error = "contract expiry entry instrument_id is required";
        }
        return false;
    }
    if (!IsNormalizedTradingDay(entry.last_trading_day)) {
        if (error != nullptr) {
            *error = "contract expiry entry last_trading_day must be YYYYMMDD";
        }
        return false;
    }
    entries_by_instrument_[instrument_id] =
        ContractExpiryEntry{instrument_id, entry.last_trading_day};
    return true;
}

const ContractExpiryEntry* ContractExpiryCalendar::Find(const std::string& instrument_id) const {
    const std::string canonical = CanonicalContractInstrumentId(instrument_id);
    const auto it = entries_by_instrument_.find(canonical);
    return it == entries_by_instrument_.end() ? nullptr : &it->second;
}

std::size_t ContractExpiryCalendar::Size() const noexcept { return entries_by_instrument_.size(); }

bool LoadContractExpiryCalendar(const std::string& path, ContractExpiryCalendar* out,
                                std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "contract expiry calendar output is null";
        }
        return false;
    }
    std::ifstream input(path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open contract expiry calendar: " + path;
        }
        return false;
    }

    ContractExpiryCalendar calendar;
    bool in_contracts = false;
    std::string current_instrument_id;
    std::string raw_line;
    int line_no = 0;
    while (std::getline(input, raw_line)) {
        ++line_no;
        const std::string stripped = StripInlineComment(raw_line);
        const std::size_t first_non_space = stripped.find_first_not_of(' ');
        if (first_non_space == std::string::npos) {
            continue;
        }
        const int indent = static_cast<int>(first_non_space);
        const std::string text = Trim(stripped);
        if (text.empty()) {
            continue;
        }

        std::string key;
        std::string value;
        if (!ParseKeyValue(text, &key, &value)) {
            if (error != nullptr) {
                *error = "line " + std::to_string(line_no) + ": invalid key/value entry";
            }
            return false;
        }

        if (indent == 0) {
            if (key != "contracts" || !value.empty()) {
                if (error != nullptr) {
                    *error = "line " + std::to_string(line_no) +
                             ": contract expiry calendar must start with `contracts:`";
                }
                return false;
            }
            in_contracts = true;
            current_instrument_id.clear();
            continue;
        }

        if (!in_contracts) {
            if (error != nullptr) {
                *error = "line " + std::to_string(line_no) + ": unexpected content before contracts";
            }
            return false;
        }

        if (indent == 2) {
            if (!value.empty()) {
                if (error != nullptr) {
                    *error = "line " + std::to_string(line_no) +
                             ": contract entry must be a YAML section";
                }
                return false;
            }
            current_instrument_id = key;
            continue;
        }

        if (indent == 4 && key == "last_trading_day") {
            if (current_instrument_id.empty()) {
                if (error != nullptr) {
                    *error = "line " + std::to_string(line_no) +
                             ": last_trading_day must belong to a contract entry";
                }
                return false;
            }
            std::string upsert_error;
            if (!calendar.Upsert({current_instrument_id, value}, &upsert_error)) {
                if (error != nullptr) {
                    *error = "line " + std::to_string(line_no) + ": " + upsert_error;
                }
                return false;
            }
            continue;
        }

        if (error != nullptr) {
            *error = "line " + std::to_string(line_no) + ": unsupported contract expiry field: " +
                     key;
        }
        return false;
    }

    if (calendar.Size() == 0U) {
        if (error != nullptr) {
            *error = "contract expiry calendar is empty";
        }
        return false;
    }

    *out = std::move(calendar);
    return true;
}

}  // namespace quant_hft
