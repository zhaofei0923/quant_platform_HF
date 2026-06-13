#pragma once

#include <string>
#include <vector>

#include "quant_hft/contracts/types.h"
#include "quant_hft/services/ctp_position_ledger.h"
#include "quant_hft/services/execution_planner.h"

namespace quant_hft {

std::string InferCtpExchangeIdFromInstrumentId(const std::string& instrument_id);
std::string CtpHedgeFlagToText(HedgeFlag hedge_flag);
bool RequiresExplicitCtpCloseOffset(const std::string& exchange_id);

bool ResolveCtpCloseOffsets(const std::vector<PlannedOrder>& input,
                            const CtpPositionLedger& position_ledger,
                            std::vector<PlannedOrder>* output, std::string* error);

}  // namespace quant_hft
