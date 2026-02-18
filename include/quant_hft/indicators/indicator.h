#pragma once

#include <optional>

namespace quant_hft {

class Indicator {
   public:
    virtual ~Indicator() = default;

    virtual void Update(double high, double low, double close, double volume = 0.0) = 0;
    virtual std::optional<double> Value() const = 0;
    virtual bool IsReady() const = 0;
    virtual void Reset() = 0;
};

}  // namespace quant_hft
