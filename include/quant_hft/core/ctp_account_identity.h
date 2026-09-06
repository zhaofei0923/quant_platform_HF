#pragma once

#include <string>

namespace quant_hft {
// A response may identify its investor explicitly. UserID is a login operator,
// and is deliberately not an input to account resolution.
inline std::string ResolveCtpInvestorId(const std::string& reported_investor,
                                        const std::string& configured_investor) {
    return reported_investor.empty() ? configured_investor : reported_investor;
}
}  // namespace quant_hft
