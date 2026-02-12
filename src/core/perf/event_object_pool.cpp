#include "quant_hft/core/event_object_pool.h"

#include <algorithm>

namespace quant_hft {

EventObjectPool::EventObjectPool(std::size_t capacity, std::size_t buffer_size)
    : capacity_(std::max<std::size_t>(1, capacity)),
      buffer_size_(std::max<std::size_t>(1, buffer_size)) {
    slots_.reserve(capacity_);
}

std::shared_ptr<EventObjectPool::Buffer> EventObjectPool::Acquire(std::size_t min_capacity) {
    const auto required_capacity = std::max(buffer_size_, min_capacity);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!free_slots_.empty()) {
            const auto slot_index = free_slots_.front();
            free_slots_.pop();
            ++reused_slots_;
            ++in_use_slots_;
            Buffer* ptr = slots_[slot_index].get();
            if (ptr->size() < required_capacity) {
                ptr->resize(required_capacity);
            }
            return std::shared_ptr<Buffer>(ptr, [this, slot_index](Buffer*) { Release(slot_index); });
        }
        if (slots_.size() < capacity_) {
            const std::size_t slot_index = slots_.size();
            auto slot = std::make_unique<Buffer>(required_capacity, 0U);
            Buffer* ptr = slot.get();
            slots_.push_back(std::move(slot));
            ++in_use_slots_;
            return std::shared_ptr<Buffer>(ptr, [this, slot_index](Buffer*) { Release(slot_index); });
        }
        ++fallback_allocations_;
    }
    return std::make_shared<Buffer>(required_capacity, 0U);
}

EventObjectPool::Stats EventObjectPool::Snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    Stats stats;
    stats.capacity = capacity_;
    stats.buffer_size = buffer_size_;
    stats.created_slots = slots_.size();
    stats.in_use_slots = in_use_slots_;
    stats.reused_slots = reused_slots_;
    stats.fallback_allocations = fallback_allocations_;
    return stats;
}

void EventObjectPool::Release(std::size_t slot_index) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (slot_index >= slots_.size()) {
        return;
    }
    auto& slot = *slots_[slot_index];
    std::fill(slot.begin(), slot.end(), 0U);
    if (in_use_slots_ > 0) {
        --in_use_slots_;
    }
    free_slots_.push(slot_index);
}

}  // namespace quant_hft
