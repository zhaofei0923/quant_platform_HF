#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace quant_hft {

struct QueryResultMetadata {
    int request_id{0};
    std::uint64_t generation{0};
    std::string query_name;
    std::string instrument_id;
    std::string account_id;
    std::string trading_day;
    std::string source;
    bool full_account{false};
    bool complete{false};
    bool success{false};
    int error_code{0};
    std::string error;
};

template <typename Row>
struct QueryResult {
    QueryResultMetadata metadata;
    std::vector<Row> rows;
};

}  // namespace quant_hft
