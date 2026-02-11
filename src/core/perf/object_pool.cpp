#include "quant_hft/core/object_pool.h"

#include <algorithm>
#include <utility>

namespace quant_hft {

ObjectPool::ObjectPool(std::size_t capacity, std::size_t buffer_size)
    : capacity_(std::max<std::size_t>(1, capacity)),
      buffer_size_(std::max<std::size_t>(1, buffer_size)) {
    slots_.reserve(capacity_);
}

std::shared_ptr<ObjectPool::Buffer> ObjectPool::Acquire() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!free_slots_.empty()) {
            const std::size_t index = free_slots_.front();
            free_slots_.pop();
            ++in_use_slots_;
            ++reused_slots_;
            Buffer* ptr = slots_[index].get();
            return std::shared_ptr<Buffer>(
                ptr,
                [this, index](Buffer*) {
                    this->Release(index);
                });
        }

        if (slots_.size() < capacity_) {
            const std::size_t index = slots_.size();
            auto slot = std::make_unique<Buffer>(buffer_size_);
            Buffer* ptr = slot.get();
            slots_.push_back(std::move(slot));
            ++in_use_slots_;
            return std::shared_ptr<Buffer>(
                ptr,
                [this, index](Buffer*) {
                    this->Release(index);
                });
        }

        ++fallback_allocations_;
    }

    // If pool capacity is exhausted, return a non-pooled fallback buffer.
    return std::make_shared<Buffer>(buffer_size_);
}

ObjectPoolStats ObjectPool::Snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    ObjectPoolStats stats;
    stats.capacity = capacity_;
    stats.created_slots = slots_.size();
    stats.in_use_slots = in_use_slots_;
    stats.available_slots = free_slots_.size();
    stats.reused_slots = reused_slots_;
    stats.fallback_allocations = fallback_allocations_;
    return stats;
}

void ObjectPool::Release(std::size_t slot_index) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (slot_index >= slots_.size()) {
        return;
    }
    Buffer& slot = *slots_[slot_index];
    slot.assign(buffer_size_, 0U);
    if (in_use_slots_ > 0) {
        --in_use_slots_;
    }
    free_slots_.push(slot_index);
}

}  // namespace quant_hft
