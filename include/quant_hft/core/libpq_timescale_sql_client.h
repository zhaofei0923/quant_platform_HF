#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/core/storage_connection_config.h"
#include "quant_hft/core/timescale_sql_client.h"

namespace quant_hft {

class LibpqTimescaleSqlClient : public ITimescaleSqlClient {
public:
    struct LibpqApi;

    explicit LibpqTimescaleSqlClient(TimescaleConnectionConfig config);

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
    static const LibpqApi& Api();
    bool ValidateSimpleIdentifier(const std::string& identifier,
                                  const std::string& field_name,
                                  std::string* error) const;
    bool ValidateQualifiedTableIdentifier(const std::string& table_identifier,
                                          std::string* quoted_identifier,
                                          std::string* error) const;
    static std::string QuoteIdentifier(const std::string& identifier);
    std::string BuildConnInfo() const;
    static std::string EscapeConnInfoValue(const std::string& value);
    static std::vector<std::unordered_map<std::string, std::string>> ParseRows(
        const LibpqApi& api,
        void* result_ptr);
    static bool IsCommandOk(const LibpqApi& api, void* result_ptr);
    static bool IsTuplesOk(const LibpqApi& api, void* result_ptr);
    static std::string ResultStatusText(const LibpqApi& api, void* result_ptr);

    bool Connect(void** out_conn, std::string* error) const;
    bool ExecuteStatement(const std::string& sql,
                          const std::vector<std::string>& params,
                          bool expect_tuples,
                          std::vector<std::unordered_map<std::string, std::string>>* out_rows,
                          std::string* error) const;

    TimescaleConnectionConfig config_;
};

}  // namespace quant_hft
