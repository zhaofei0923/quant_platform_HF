#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "quant_hft/strategy/atomic_strategy.h"
#include "quant_hft/strategy/composite_strategy.h"

namespace quant_hft {

class AtomicFactory {
   public:
    using Creator = std::function<std::unique_ptr<IAtomicStrategy>()>;

    static AtomicFactory& Instance();

    bool Register(const std::string& type, Creator creator, std::string* error = nullptr);
    std::unique_ptr<IAtomicStrategy> Create(const AtomicStrategyDefinition& definition,
                                            std::string* error = nullptr) const;
    bool Has(const std::string& type) const;

   private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, Creator> creators_;
};

#define QUANT_HFT_REGISTER_ATOMIC_STRATEGY(TYPE_NAME, STRATEGY_CLASS)                 \
    namespace {                                                                       \
    [[maybe_unused]] const bool kRegisteredAtomicStrategy_##STRATEGY_CLASS = []() {   \
        return ::quant_hft::AtomicFactory::Instance().Register(                       \
            TYPE_NAME, []() { return std::make_unique<STRATEGY_CLASS>(); }, nullptr); \
    }();                                                                              \
    }  // namespace

}  // namespace quant_hft
