#pragma once

#include <string>

#include "quant_hft/strategy/atomic_strategy.h"

namespace quant_hft {

class TimeFilter : public ITimeFilterStrategy {
   public:
    TimeFilter() = default;

    void Init(const AtomicParams& params) override;
    std::string GetId() const override;
    void Reset() override;
    bool AllowOpening(EpochNanos now_ns) override;

   private:
    std::string id_{"TimeFilter"};
    int start_hour_{0};
    int end_hour_{0};
    int timezone_offset_hours_{0};
};

}  // namespace quant_hft
