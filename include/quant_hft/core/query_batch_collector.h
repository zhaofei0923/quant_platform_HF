#pragma once

#include <map>
#include <mutex>
#include <optional>
#include <utility>

#include "quant_hft/contracts/query_result.h"

namespace quant_hft {

// Each response batch owns its rows. Finishing a request detaches the immutable result before
// the next request may start. A successful empty full-account result is distinct from failure.
template <typename Row>
class QueryBatchCollector {
   public:
    explicit QueryBatchCollector(std::size_t max_rows = 250'000, std::size_t max_batches = 128)
        : max_rows_(max_rows), max_batches_(max_batches) {}

    bool Begin(QueryResultMetadata metadata) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (batches_.size() >= max_batches_) return false;
        const Key key{metadata.generation, metadata.request_id};
        metadata.success = true;
        return batches_.emplace(key, QueryResult<Row>{std::move(metadata), {}}).second;
    }

    std::optional<QueryResult<Row>> Accept(int request_id, std::uint64_t generation, const Row* row,
                                           int error_code, const std::string& error, bool last) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = batches_.find(Key{generation, request_id});
        if (it == batches_.end()) {
            return std::nullopt;
        }
        auto& result = it->second;
        if (error_code != 0 && result.metadata.success) {
            result.metadata.success = false;
            result.metadata.error_code = error_code;
            result.metadata.error = error;
        } else if (row != nullptr && error_code == 0 && result.metadata.success) {
            if (result.rows.size() >= max_rows_) {
                result.metadata.success = false;
                result.metadata.error_code = -1;
                result.metadata.error = "query batch row limit exceeded";
            } else {
                result.rows.push_back(*row);
            }
        }
        if (!last) {
            return std::nullopt;
        }
        result.metadata.complete = true;
        auto completed = std::move(result);
        batches_.erase(it);
        return completed;
    }

    void Reset() {
        std::lock_guard<std::mutex> lock(mutex_);
        batches_.clear();
    }

   private:
    using Key = std::pair<std::uint64_t, int>;
    std::mutex mutex_;
    std::map<Key, QueryResult<Row>> batches_;
    std::size_t max_rows_;
    std::size_t max_batches_;
};

}  // namespace quant_hft
