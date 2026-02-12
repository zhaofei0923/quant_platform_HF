#include "quant_hft/core/timescale_sql_client.h"

namespace quant_hft {

bool InMemoryTimescaleSqlClient::InsertRow(
    const std::string& table,
    const std::unordered_map<std::string, std::string>& row,
    std::string* error) {
    if (table.empty()) {
        if (error != nullptr) {
            *error = "empty table";
        }
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    tables_[table].push_back(row);
    return true;
}

bool InMemoryTimescaleSqlClient::UpsertRow(
    const std::string& table,
    const std::unordered_map<std::string, std::string>& row,
    const std::vector<std::string>& conflict_keys,
    const std::vector<std::string>& update_keys,
    std::string* error) {
    if (table.empty()) {
        if (error != nullptr) {
            *error = "empty table";
        }
        return false;
    }
    if (row.empty()) {
        if (error != nullptr) {
            *error = "empty row";
        }
        return false;
    }
    if (conflict_keys.empty()) {
        return InsertRow(table, row, error);
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto& rows = tables_[table];
    for (const auto& key : conflict_keys) {
        if (row.find(key) == row.end()) {
            if (error != nullptr) {
                *error = "missing conflict key: " + key;
            }
            return false;
        }
    }

    auto matches_conflict_keys =
        [&conflict_keys, &row](const std::unordered_map<std::string, std::string>& existing) {
            for (const auto& key : conflict_keys) {
                const auto existing_it = existing.find(key);
                const auto row_it = row.find(key);
                if (existing_it == existing.end() || row_it == row.end() ||
                    existing_it->second != row_it->second) {
                    return false;
                }
            }
            return true;
        };

    const auto update_columns = [&]() {
        if (!update_keys.empty()) {
            return update_keys;
        }
        std::vector<std::string> derived;
        derived.reserve(row.size());
        for (const auto& [key, value] : row) {
            (void)value;
            bool is_conflict_key = false;
            for (const auto& conflict_key : conflict_keys) {
                if (conflict_key == key) {
                    is_conflict_key = true;
                    break;
                }
            }
            if (!is_conflict_key) {
                derived.push_back(key);
            }
        }
        return derived;
    }();

    for (auto& existing : rows) {
        if (!matches_conflict_keys(existing)) {
            continue;
        }
        for (const auto& key : update_columns) {
            const auto row_it = row.find(key);
            if (row_it == row.end()) {
                if (error != nullptr) {
                    *error = "missing update key: " + key;
                }
                return false;
            }
            existing[key] = row_it->second;
        }
        return true;
    }

    rows.push_back(row);
    return true;
}

std::vector<std::unordered_map<std::string, std::string>>
InMemoryTimescaleSqlClient::QueryRows(const std::string& table,
                                      const std::string& key,
                                      const std::string& value,
                                      std::string* error) const {
    (void)error;
    std::lock_guard<std::mutex> lock(mutex_);
    const auto table_it = tables_.find(table);
    if (table_it == tables_.end()) {
        return {};
    }

    std::vector<std::unordered_map<std::string, std::string>> out;
    out.reserve(table_it->second.size());
    for (const auto& row : table_it->second) {
        const auto it = row.find(key);
        if (it != row.end() && it->second == value) {
            out.push_back(row);
        }
    }
    return out;
}

std::vector<std::unordered_map<std::string, std::string>>
InMemoryTimescaleSqlClient::QueryAllRows(const std::string& table,
                                         std::string* error) const {
    (void)error;
    std::lock_guard<std::mutex> lock(mutex_);
    const auto table_it = tables_.find(table);
    if (table_it == tables_.end()) {
        return {};
    }
    return table_it->second;
}

bool InMemoryTimescaleSqlClient::Ping(std::string* error) const {
    (void)error;
    return true;
}

}  // namespace quant_hft
