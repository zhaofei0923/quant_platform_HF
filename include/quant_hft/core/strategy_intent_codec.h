#pragma once

#include <string>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

class StrategyIntentCodec {
public:
    static bool DecodeSignalIntent(const std::string& strategy_id,
                                   const std::string& encoded,
                                   SignalIntent* out,
                                   std::string* error);
    static bool ParseSide(const std::string& text, Side* out);
    static bool ParseOffset(const std::string& text, OffsetFlag* out);
    static std::string ToSideString(Side side);
    static std::string ToOffsetString(OffsetFlag offset);
};

}  // namespace quant_hft
