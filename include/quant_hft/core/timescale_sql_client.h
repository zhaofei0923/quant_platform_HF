#pragma once

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace quant_hft {

class ITimescaleSqlClient {
public:
    virtual ~ITimescaleSqlClient() = default;

    virtual bool InsertRow(const std::string& table,
                           const std::unordered_map<std::string, std::string>& row,
                           std::string* error) = 0;

    virtual std::vector<std::unordered_map<std::string, std::string>> QueryRows(
        const std::string& table,
        const std::string& key,
        const std::string& value,
        std::string* error) const = 0;

    virtual std::vector<std::unordered_map<std::string, std::string>> QueryAllRows(
        const std::string& table,
        std::string* error) const = 0;
    virtual bool Ping(std::string* error) const = 0;
};

class InMemoryTimescaleSqlClient : public ITimescaleSqlClient {
public:
    bool InsertRow(const std::string& table,
                   const std::unordered_map<std::string, std::string>& row,
                   std::string* error) override;

    std::vector<std::unordered_map<std::string, std::string>> QueryRows(
        const std::string& table,
        const std::string& key,
        const std::string& value,
        std::string* error) const override;

    std::vector<std::unordered_map<std::string, std::string>> QueryAllRows(
        const std::string& table,
        std::string* error) const override;
    bool Ping(std::string* error) const override;

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string,
                       std::vector<std::unordered_map<std::string, std::string>>>
        tables_;
};

}  // namespace quant_hft
