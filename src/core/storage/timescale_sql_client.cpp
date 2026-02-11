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
