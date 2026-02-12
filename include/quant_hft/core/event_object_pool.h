#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <queue>
#include <vector>

namespace quant_hft {

class EventObjectPool {
public:
    using Buffer = std::vector<std::uint8_t>;

    struct Stats {
        std::size_t capacity{0};
        std::size_t buffer_size{0};
        std::size_t created_slots{0};
        std::size_t in_use_slots{0};
        std::size_t reused_slots{0};
        std::size_t fallback_allocations{0};
    };

    EventObjectPool(std::size_t capacity, std::size_t buffer_size = 1024);

    std::shared_ptr<Buffer> Acquire(std::size_t min_capacity = 0);
    Stats Snapshot() const;

private:
    void Release(std::size_t slot_index);

    mutable std::mutex mutex_;
    std::size_t capacity_{0};
    std::size_t buffer_size_{0};
    std::vector<std::unique_ptr<Buffer>> slots_;
    std::queue<std::size_t> free_slots_;
    std::size_t in_use_slots_{0};
    std::size_t reused_slots_{0};
    std::size_t fallback_allocations_{0};
};

}  // namespace quant_hft
