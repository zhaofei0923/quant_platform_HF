#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "quant_hft/strategy/live_strategy.h"

namespace quant_hft {

class StrategyRegistry {
public:
    using Factory = std::function<std::unique_ptr<ILiveStrategy>()>;

    static StrategyRegistry& Instance();

    bool RegisterFactory(const std::string& strategy_factory,
                         Factory factory,
                         std::string* error = nullptr);

    std::unique_ptr<ILiveStrategy> Create(const std::string& strategy_factory) const;
    bool HasFactory(const std::string& strategy_factory) const;

private:
    StrategyRegistry() = default;

    mutable std::mutex mutex_;
    std::unordered_map<std::string, Factory> factories_;
};

#define QUANT_HFT_REGISTER_LIVE_STRATEGY(FACTORY_NAME, STRATEGY_TYPE)                              \
    namespace {                                                                                     \
    [[maybe_unused]] const bool kRegisteredLiveStrategy_##STRATEGY_TYPE = []() {                   \
        return ::quant_hft::StrategyRegistry::Instance().RegisterFactory(                           \
            FACTORY_NAME, []() { return std::make_unique<STRATEGY_TYPE>(); }, nullptr);            \
    }();                                                                                            \
    }  // namespace

}  // namespace quant_hft
