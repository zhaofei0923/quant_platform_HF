#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <queue>
#include <vector>

namespace quant_hft {

struct ObjectPoolStats {
    std::size_t capacity{0};
    std::size_t created_slots{0};
    std::size_t in_use_slots{0};
    std::size_t available_slots{0};
    std::size_t reused_slots{0};
    std::size_t fallback_allocations{0};
};

class ObjectPool {
public:
    using Buffer = std::vector<std::uint8_t>;

    ObjectPool(std::size_t capacity, std::size_t buffer_size = 256);

    std::shared_ptr<Buffer> Acquire();
    ObjectPoolStats Snapshot() const;

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
