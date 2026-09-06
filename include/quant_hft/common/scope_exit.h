#pragma once

#include <functional>
#include <utility>

namespace quant_hft {

class ScopeExit {
   public:
    explicit ScopeExit(std::function<void()> action) : action_(std::move(action)) {}
    ~ScopeExit() {
        if (action_) action_();
    }
    ScopeExit(const ScopeExit&) = delete;
    ScopeExit& operator=(const ScopeExit&) = delete;
    void Run() {
        auto action = std::move(action_);
        if (action) action();
    }

   private:
    std::function<void()> action_;
};

}  // namespace quant_hft
