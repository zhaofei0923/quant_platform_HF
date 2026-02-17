#include "quant_hft/strategy/strategy_registry.h"

namespace quant_hft {

StrategyRegistry& StrategyRegistry::Instance() {
    static StrategyRegistry registry;
    return registry;
}

bool StrategyRegistry::RegisterFactory(const std::string& strategy_factory,
                                       Factory factory,
                                       std::string* error) {
    if (strategy_factory.empty()) {
        if (error != nullptr) {
            *error = "strategy_factory must not be empty";
        }
        return false;
    }
    if (!factory) {
        if (error != nullptr) {
            *error = "strategy_factory registration requires a valid factory function";
        }
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const auto [it, inserted] = factories_.emplace(strategy_factory, std::move(factory));
    if (!inserted) {
        if (error != nullptr) {
            *error = "strategy_factory already registered: " + strategy_factory;
        }
        return false;
    }
    return true;
}

std::unique_ptr<ILiveStrategy> StrategyRegistry::Create(const std::string& strategy_factory) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = factories_.find(strategy_factory);
    if (it == factories_.end()) {
        return nullptr;
    }
    return it->second();
}

bool StrategyRegistry::HasFactory(const std::string& strategy_factory) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return factories_.find(strategy_factory) != factories_.end();
}

}  // namespace quant_hft
