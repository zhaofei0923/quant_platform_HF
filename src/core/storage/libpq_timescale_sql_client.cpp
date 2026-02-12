#include "quant_hft/core/libpq_timescale_sql_client.h"

#include <dlfcn.h>

#include <algorithm>
#include <cctype>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace quant_hft {

extern "C" {
struct pg_conn;
struct pg_result;
}

using PGconn = pg_conn;
using PGresult = pg_result;
using Oid = unsigned int;

struct LibpqTimescaleSqlClient::LibpqApi {
    using PQconnectdbFn = PGconn* (*)(const char* conninfo);
    using PQstatusFn = int (*)(const PGconn* conn);
    using PQerrorMessageFn = char* (*)(const PGconn* conn);
    using PQfinishFn = void (*)(PGconn* conn);
    using PQexecFn = PGresult* (*)(PGconn* conn, const char* query);
    using PQexecParamsFn = PGresult* (*)(PGconn* conn,
                                         const char* command,
                                         int n_params,
                                         const Oid* param_types,
                                         const char* const* param_values,
                                         const int* param_lengths,
                                         const int* param_formats,
                                         int result_format);
    using PQresultStatusFn = int (*)(const PGresult* result);
    using PQresStatusFn = const char* (*)(int status);
    using PQresultErrorMessageFn = char* (*)(const PGresult* result);
    using PQclearFn = void (*)(PGresult* result);
    using PQntuplesFn = int (*)(const PGresult* result);
    using PQnfieldsFn = int (*)(const PGresult* result);
    using PQfnameFn = char* (*)(const PGresult* result, int field_num);
    using PQgetvalueFn = char* (*)(const PGresult* result, int row_num, int field_num);
    using PQgetisnullFn = int (*)(const PGresult* result, int row_num, int field_num);

    bool available{false};
    std::string load_error;
    void* dl_handle{nullptr};

    PQconnectdbFn PQconnectdb{nullptr};
    PQstatusFn PQstatus{nullptr};
    PQerrorMessageFn PQerrorMessage{nullptr};
    PQfinishFn PQfinish{nullptr};
    PQexecFn PQexec{nullptr};
    PQexecParamsFn PQexecParams{nullptr};
    PQresultStatusFn PQresultStatus{nullptr};
    PQresStatusFn PQresStatus{nullptr};
    PQresultErrorMessageFn PQresultErrorMessage{nullptr};
    PQclearFn PQclear{nullptr};
    PQntuplesFn PQntuples{nullptr};
    PQnfieldsFn PQnfields{nullptr};
    PQfnameFn PQfname{nullptr};
    PQgetvalueFn PQgetvalue{nullptr};
    PQgetisnullFn PQgetisnull{nullptr};

    ~LibpqApi() {
        if (dl_handle != nullptr) {
            (void)::dlclose(dl_handle);
        }
    }
};

namespace {

constexpr int kConnectionOk = 0;

template <typename Fn>
bool LoadSymbol(void* handle, const char* name, Fn* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "symbol output pointer is null";
        }
        return false;
    }

    void* raw = ::dlsym(handle, name);
    if (raw == nullptr) {
        if (error != nullptr) {
            *error = std::string("failed to load symbol ") + name;
        }
        return false;
    }
    *out = reinterpret_cast<Fn>(raw);
    return true;
}

LibpqTimescaleSqlClient::LibpqApi LoadLibpqApi() {
    LibpqTimescaleSqlClient::LibpqApi api;
    const char* candidates[] = {"libpq.so.5", "libpq.so"};
    for (const char* lib_name : candidates) {
        api.dl_handle = ::dlopen(lib_name, RTLD_NOW | RTLD_LOCAL);
        if (api.dl_handle != nullptr) {
            break;
        }
    }
    if (api.dl_handle == nullptr) {
        const char* dl_error = ::dlerror();
        api.load_error = dl_error != nullptr ? dl_error : "unable to load libpq";
        return api;
    }

    std::string error;
    if (!LoadSymbol(api.dl_handle, "PQconnectdb", &api.PQconnectdb, &error) ||
        !LoadSymbol(api.dl_handle, "PQstatus", &api.PQstatus, &error) ||
        !LoadSymbol(api.dl_handle, "PQerrorMessage", &api.PQerrorMessage, &error) ||
        !LoadSymbol(api.dl_handle, "PQfinish", &api.PQfinish, &error) ||
        !LoadSymbol(api.dl_handle, "PQexec", &api.PQexec, &error) ||
        !LoadSymbol(api.dl_handle, "PQexecParams", &api.PQexecParams, &error) ||
        !LoadSymbol(api.dl_handle, "PQresultStatus", &api.PQresultStatus, &error) ||
        !LoadSymbol(api.dl_handle, "PQresStatus", &api.PQresStatus, &error) ||
        !LoadSymbol(api.dl_handle, "PQresultErrorMessage", &api.PQresultErrorMessage, &error) ||
        !LoadSymbol(api.dl_handle, "PQclear", &api.PQclear, &error) ||
        !LoadSymbol(api.dl_handle, "PQntuples", &api.PQntuples, &error) ||
        !LoadSymbol(api.dl_handle, "PQnfields", &api.PQnfields, &error) ||
        !LoadSymbol(api.dl_handle, "PQfname", &api.PQfname, &error) ||
        !LoadSymbol(api.dl_handle, "PQgetvalue", &api.PQgetvalue, &error) ||
        !LoadSymbol(api.dl_handle, "PQgetisnull", &api.PQgetisnull, &error)) {
        api.load_error = error;
        (void)::dlclose(api.dl_handle);
        api.dl_handle = nullptr;
        return api;
    }

    api.available = true;
    return api;
}

std::string ConnOrResultError(const LibpqTimescaleSqlClient::LibpqApi& api,
                              const PGconn* conn,
                              const PGresult* result,
                              const std::string& fallback) {
    if (result != nullptr && api.PQresultErrorMessage != nullptr) {
        const char* result_error = api.PQresultErrorMessage(result);
        if (result_error != nullptr && *result_error != '\0') {
            return std::string(result_error);
        }
    }
    if (conn != nullptr && api.PQerrorMessage != nullptr) {
        const char* conn_error = api.PQerrorMessage(conn);
        if (conn_error != nullptr && *conn_error != '\0') {
            return std::string(conn_error);
        }
    }
    return fallback;
}

}  // namespace

LibpqTimescaleSqlClient::LibpqTimescaleSqlClient(TimescaleConnectionConfig config)
    : config_(std::move(config)) {}

const LibpqTimescaleSqlClient::LibpqApi& LibpqTimescaleSqlClient::Api() {
    static const LibpqApi api = LoadLibpqApi();
    return api;
}

bool LibpqTimescaleSqlClient::ValidateSimpleIdentifier(const std::string& identifier,
                                                       const std::string& field_name,
                                                       std::string* error) const {
    if (identifier.empty()) {
        if (error != nullptr) {
            *error = "empty " + field_name + " identifier";
        }
        return false;
    }
    const unsigned char first = static_cast<unsigned char>(identifier.front());
    if (!(std::isalpha(first) || identifier.front() == '_')) {
        if (error != nullptr) {
            *error = "invalid " + field_name + " identifier: " + identifier;
        }
        return false;
    }
    for (char ch : identifier) {
        const unsigned char c = static_cast<unsigned char>(ch);
        if (!(std::isalnum(c) || ch == '_')) {
            if (error != nullptr) {
                *error = "invalid " + field_name + " identifier: " + identifier;
            }
            return false;
        }
    }
    return true;
}

std::string LibpqTimescaleSqlClient::QuoteIdentifier(const std::string& identifier) {
    return "\"" + identifier + "\"";
}

bool LibpqTimescaleSqlClient::ValidateQualifiedTableIdentifier(
    const std::string& table_identifier,
    std::string* quoted_identifier,
    std::string* error) const {
    if (quoted_identifier == nullptr) {
        if (error != nullptr) {
            *error = "quoted_identifier is null";
        }
        return false;
    }
    quoted_identifier->clear();

    if (table_identifier.empty()) {
        if (error != nullptr) {
            *error = "empty table identifier";
        }
        return false;
    }

    std::vector<std::string> segments;
    std::size_t start = 0;
    while (start < table_identifier.size()) {
        const auto dot = table_identifier.find('.', start);
        const auto end = dot == std::string::npos ? table_identifier.size() : dot;
        if (end == start) {
            if (error != nullptr) {
                *error = "invalid table identifier: " + table_identifier;
            }
            return false;
        }
        segments.push_back(table_identifier.substr(start, end - start));
        if (dot == std::string::npos) {
            break;
        }
        start = dot + 1;
    }

    if (segments.empty() || segments.size() > 2) {
        if (error != nullptr) {
            *error = "invalid table identifier: " + table_identifier;
        }
        return false;
    }

    for (const auto& segment : segments) {
        if (!ValidateSimpleIdentifier(segment, "table", error)) {
            return false;
        }
    }

    if (segments.size() == 1) {
        *quoted_identifier = QuoteIdentifier(segments[0]);
    } else {
        *quoted_identifier = QuoteIdentifier(segments[0]) + "." + QuoteIdentifier(segments[1]);
    }
    return true;
}

std::string LibpqTimescaleSqlClient::EscapeConnInfoValue(const std::string& value) {
    std::string out;
    out.reserve(value.size() + 8);
    for (char ch : value) {
        if (ch == '\\' || ch == '\'') {
            out.push_back('\\');
        }
        out.push_back(ch);
    }
    return out;
}

std::string LibpqTimescaleSqlClient::BuildConnInfo() const {
    if (!config_.dsn.empty()) {
        return config_.dsn;
    }

    auto append_field = [](std::ostringstream* stream,
                           const std::string& key,
                           const std::string& value) {
        if (stream == nullptr || value.empty()) {
            return;
        }
        *stream << key << "='" << EscapeConnInfoValue(value) << "' ";
    };

    std::ostringstream conn_info;
    append_field(&conn_info, "host", config_.host);
    conn_info << "port='" << config_.port << "' ";
    append_field(&conn_info, "dbname", config_.database);
    append_field(&conn_info, "user", config_.user);
    append_field(&conn_info, "password", config_.password);
    append_field(&conn_info, "sslmode", config_.ssl_mode);
    conn_info << "connect_timeout='"
              << std::max(1, config_.connect_timeout_ms / 1000) << "'";
    return conn_info.str();
}

bool LibpqTimescaleSqlClient::Connect(void** out_conn, std::string* error) const {
    if (out_conn == nullptr) {
        if (error != nullptr) {
            *error = "out_conn is null";
        }
        return false;
    }
    *out_conn = nullptr;

    const auto& api = Api();
    if (!api.available) {
        if (error != nullptr) {
            *error = "libpq unavailable: " + api.load_error;
        }
        return false;
    }

    PGconn* conn = api.PQconnectdb(BuildConnInfo().c_str());
    if (conn == nullptr) {
        if (error != nullptr) {
            *error = "PQconnectdb returned null";
        }
        return false;
    }
    if (api.PQstatus(conn) != kConnectionOk) {
        if (error != nullptr) {
            *error = ConnOrResultError(api, conn, nullptr, "PQconnectdb failed");
        }
        api.PQfinish(conn);
        return false;
    }
    *out_conn = conn;
    return true;
}

bool LibpqTimescaleSqlClient::IsCommandOk(const LibpqApi& api, void* result_ptr) {
    return ResultStatusText(api, result_ptr) == "PGRES_COMMAND_OK";
}

bool LibpqTimescaleSqlClient::IsTuplesOk(const LibpqApi& api, void* result_ptr) {
    const auto status = ResultStatusText(api, result_ptr);
    return status == "PGRES_TUPLES_OK" || status == "PGRES_SINGLE_TUPLE" ||
           status == "PGRES_TUPLES_CHUNK";
}

std::string LibpqTimescaleSqlClient::ResultStatusText(const LibpqApi& api,
                                                      void* result_ptr) {
    auto* result = static_cast<PGresult*>(result_ptr);
    if (result == nullptr) {
        return "PGRES_NULL";
    }
    const int status = api.PQresultStatus(result);
    const char* status_text = api.PQresStatus(status);
    if (status_text == nullptr || *status_text == '\0') {
        return "PGRES_UNKNOWN";
    }
    return std::string(status_text);
}

std::vector<std::unordered_map<std::string, std::string>>
LibpqTimescaleSqlClient::ParseRows(const LibpqApi& api, void* result_ptr) {
    auto* result = static_cast<PGresult*>(result_ptr);
    if (result == nullptr) {
        return {};
    }

    const int rows = std::max(0, api.PQntuples(result));
    const int fields = std::max(0, api.PQnfields(result));

    std::vector<std::unordered_map<std::string, std::string>> out;
    out.reserve(static_cast<std::size_t>(rows));
    for (int row = 0; row < rows; ++row) {
        std::unordered_map<std::string, std::string> item;
        for (int col = 0; col < fields; ++col) {
            const char* name = api.PQfname(result, col);
            if (name == nullptr) {
                continue;
            }
            if (api.PQgetisnull(result, row, col) != 0) {
                item[name] = "";
                continue;
            }
            const char* value = api.PQgetvalue(result, row, col);
            item[name] = value != nullptr ? std::string(value) : "";
        }
        out.push_back(std::move(item));
    }
    return out;
}

bool LibpqTimescaleSqlClient::ExecuteStatement(
    const std::string& sql,
    const std::vector<std::string>& params,
    bool expect_tuples,
    std::vector<std::unordered_map<std::string, std::string>>* out_rows,
    std::string* error) const {
    void* conn_raw = nullptr;
    if (!Connect(&conn_raw, error)) {
        return false;
    }

    const auto& api = Api();
    auto* conn = static_cast<PGconn*>(conn_raw);
    std::unique_ptr<PGconn, LibpqApi::PQfinishFn> conn_guard(conn, api.PQfinish);

    PGresult* result = nullptr;
    if (params.empty()) {
        result = api.PQexec(conn, sql.c_str());
    } else {
        std::vector<const char*> values;
        values.reserve(params.size());
        for (const auto& param : params) {
            values.push_back(param.c_str());
        }
        result = api.PQexecParams(conn,
                                  sql.c_str(),
                                  static_cast<int>(values.size()),
                                  nullptr,
                                  values.data(),
                                  nullptr,
                                  nullptr,
                                  0);
    }

    if (result == nullptr) {
        if (error != nullptr) {
            *error = ConnOrResultError(api, conn, nullptr, "PQexec/PQexecParams failed");
        }
        return false;
    }

    std::unique_ptr<PGresult, LibpqApi::PQclearFn> result_guard(result, api.PQclear);
    const bool ok = expect_tuples ? IsTuplesOk(api, result) : IsCommandOk(api, result);
    if (!ok) {
        if (error != nullptr) {
            *error = ConnOrResultError(api,
                                       conn,
                                       result,
                                       "unexpected result status: " +
                                           ResultStatusText(api, result));
        }
        return false;
    }

    if (expect_tuples && out_rows != nullptr) {
        *out_rows = ParseRows(api, result);
    }
    return true;
}

bool LibpqTimescaleSqlClient::InsertRow(
    const std::string& table,
    const std::unordered_map<std::string, std::string>& row,
    std::string* error) {
    if (row.empty()) {
        if (error != nullptr) {
            *error = "empty row";
        }
        return false;
    }
    std::string sql_table;
    if (!ValidateQualifiedTableIdentifier(table, &sql_table, error)) {
        return false;
    }

    std::vector<std::pair<std::string, std::string>> ordered(row.begin(), row.end());
    std::sort(ordered.begin(),
              ordered.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
    for (const auto& [column, value] : ordered) {
        (void)value;
        if (!ValidateSimpleIdentifier(column, "column", error)) {
            return false;
        }
    }

    std::ostringstream sql;
    sql << "INSERT INTO " << sql_table << " (";
    for (std::size_t i = 0; i < ordered.size(); ++i) {
        if (i > 0) {
            sql << ",";
        }
        sql << QuoteIdentifier(ordered[i].first);
    }
    sql << ") VALUES (";
    for (std::size_t i = 0; i < ordered.size(); ++i) {
        if (i > 0) {
            sql << ",";
        }
        sql << "$" << (i + 1);
    }
    sql << ")";

    std::vector<std::string> params;
    params.reserve(ordered.size());
    for (const auto& [column, value] : ordered) {
        (void)column;
        params.push_back(value);
    }
    return ExecuteStatement(sql.str(), params, false, nullptr, error);
}

bool LibpqTimescaleSqlClient::UpsertRow(
    const std::string& table,
    const std::unordered_map<std::string, std::string>& row,
    const std::vector<std::string>& conflict_keys,
    const std::vector<std::string>& update_keys,
    std::string* error) {
    if (row.empty()) {
        if (error != nullptr) {
            *error = "empty row";
        }
        return false;
    }
    if (conflict_keys.empty()) {
        if (error != nullptr) {
            *error = "empty conflict_keys";
        }
        return false;
    }

    std::string sql_table;
    if (!ValidateQualifiedTableIdentifier(table, &sql_table, error)) {
        return false;
    }

    std::vector<std::pair<std::string, std::string>> ordered(row.begin(), row.end());
    std::sort(ordered.begin(),
              ordered.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
    for (const auto& [column, value] : ordered) {
        (void)value;
        if (!ValidateSimpleIdentifier(column, "column", error)) {
            return false;
        }
    }

    for (const auto& key : conflict_keys) {
        if (!ValidateSimpleIdentifier(key, "conflict key", error)) {
            return false;
        }
        if (row.find(key) == row.end()) {
            if (error != nullptr) {
                *error = "missing conflict key in row: " + key;
            }
            return false;
        }
    }

    std::vector<std::string> effective_update_keys;
    if (!update_keys.empty()) {
        effective_update_keys = update_keys;
    } else {
        effective_update_keys.reserve(ordered.size());
        for (const auto& [column, value] : ordered) {
            (void)value;
            if (std::find(conflict_keys.begin(), conflict_keys.end(), column) ==
                conflict_keys.end()) {
                effective_update_keys.push_back(column);
            }
        }
    }
    for (const auto& key : effective_update_keys) {
        if (!ValidateSimpleIdentifier(key, "update key", error)) {
            return false;
        }
        if (row.find(key) == row.end()) {
            if (error != nullptr) {
                *error = "missing update key in row: " + key;
            }
            return false;
        }
    }

    std::ostringstream sql;
    sql << "INSERT INTO " << sql_table << " (";
    for (std::size_t i = 0; i < ordered.size(); ++i) {
        if (i > 0) {
            sql << ",";
        }
        sql << QuoteIdentifier(ordered[i].first);
    }
    sql << ") VALUES (";
    for (std::size_t i = 0; i < ordered.size(); ++i) {
        if (i > 0) {
            sql << ",";
        }
        sql << "$" << (i + 1);
    }
    sql << ") ON CONFLICT (";
    for (std::size_t i = 0; i < conflict_keys.size(); ++i) {
        if (i > 0) {
            sql << ",";
        }
        sql << QuoteIdentifier(conflict_keys[i]);
    }
    sql << ") ";
    if (effective_update_keys.empty()) {
        sql << "DO NOTHING";
    } else {
        sql << "DO UPDATE SET ";
        for (std::size_t i = 0; i < effective_update_keys.size(); ++i) {
            if (i > 0) {
                sql << ",";
            }
            sql << QuoteIdentifier(effective_update_keys[i]) << " = EXCLUDED."
                << QuoteIdentifier(effective_update_keys[i]);
        }
    }

    std::vector<std::string> params;
    params.reserve(ordered.size());
    for (const auto& [column, value] : ordered) {
        (void)column;
        params.push_back(value);
    }
    return ExecuteStatement(sql.str(), params, false, nullptr, error);
}

std::vector<std::unordered_map<std::string, std::string>>
LibpqTimescaleSqlClient::QueryRows(const std::string& table,
                                   const std::string& key,
                                   const std::string& value,
                                   std::string* error) const {
    std::string sql_table;
    if (!ValidateQualifiedTableIdentifier(table, &sql_table, error) ||
        !ValidateSimpleIdentifier(key, "column", error)) {
        return {};
    }

    std::vector<std::unordered_map<std::string, std::string>> rows;
    const std::string sql =
        "SELECT * FROM " + sql_table + " WHERE " + QuoteIdentifier(key) + " = $1";
    if (!ExecuteStatement(sql, {value}, true, &rows, error)) {
        return {};
    }
    return rows;
}

std::vector<std::unordered_map<std::string, std::string>>
LibpqTimescaleSqlClient::QueryAllRows(const std::string& table, std::string* error) const {
    std::string sql_table;
    if (!ValidateQualifiedTableIdentifier(table, &sql_table, error)) {
        return {};
    }

    std::vector<std::unordered_map<std::string, std::string>> rows;
    const std::string sql = "SELECT * FROM " + sql_table;
    if (!ExecuteStatement(sql, {}, true, &rows, error)) {
        return {};
    }
    return rows;
}

bool LibpqTimescaleSqlClient::Ping(std::string* error) const {
    std::vector<std::unordered_map<std::string, std::string>> rows;
    if (!ExecuteStatement("SELECT 1", {}, true, &rows, error)) {
        return false;
    }
    if (rows.empty()) {
        if (error != nullptr) {
            *error = "SELECT 1 returned no rows";
        }
        return false;
    }
    return true;
}

}  // namespace quant_hft
