#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

using AtomicParams = std::unordered_map<std::string, std::string>;

struct AtomicStrategyContext {
    std::string account_id;
    std::unordered_map<std::string, std::int32_t> net_positions;
    std::unordered_map<std::string, double> avg_open_prices;
};

class IAtomicStrategy {
   public:
    virtual ~IAtomicStrategy() = default;

    virtual void Init(const AtomicParams& params) = 0;
    virtual std::string GetId() const = 0;
    virtual void Reset() = 0;
};

class IOpeningStrategy : public IAtomicStrategy {
   public:
    ~IOpeningStrategy() override = default;
    virtual std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                              const AtomicStrategyContext& ctx) = 0;
};

class IStopLossStrategy : public IAtomicStrategy {
   public:
    ~IStopLossStrategy() override = default;
    virtual std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                              const AtomicStrategyContext& ctx) = 0;
};

class ITakeProfitStrategy : public IAtomicStrategy {
   public:
    ~ITakeProfitStrategy() override = default;
    virtual std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                              const AtomicStrategyContext& ctx) = 0;
};

class ITimeFilterStrategy : public IAtomicStrategy {
   public:
    ~ITimeFilterStrategy() override = default;
    virtual bool AllowOpening(EpochNanos now_ns) = 0;
};

class IRiskControlStrategy : public IAtomicStrategy {
   public:
    ~IRiskControlStrategy() override = default;
    virtual std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                              const AtomicStrategyContext& ctx) = 0;
};

class IAtomicOrderAware {
   public:
    virtual ~IAtomicOrderAware() = default;
    virtual void OnOrderEvent(const OrderEvent& event, const AtomicStrategyContext& ctx) = 0;
};

}  // namespace quant_hft
