#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

enum class SignalMergeRule : std::uint8_t {
    kPriority = 0,
};

class ISignalMerger {
   public:
    virtual ~ISignalMerger() = default;
    virtual std::vector<SignalIntent> Merge(const std::vector<SignalIntent>& signals) const = 0;
};

class PrioritySignalMerger final : public ISignalMerger {
   public:
    std::vector<SignalIntent> Merge(const std::vector<SignalIntent>& signals) const override;
};

std::unique_ptr<ISignalMerger> CreateSignalMerger(SignalMergeRule rule, std::string* error);
std::unique_ptr<ISignalMerger> CreateSignalMerger(const std::string& rule, std::string* error);

}  // namespace quant_hft
