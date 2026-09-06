#pragma once

#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/contracts/query_result.h"
#include "quant_hft/interfaces/trading_domain_store.h"

namespace quant_hft {

struct AccountingPolicyRuntimeIdentity {
    std::string environment;
    std::string broker_id;
    std::string account_id;
};

struct AccountingPolicyInstrument {
    std::string instrument_id;
    std::string exchange_id;
    HedgeFlag hedge_flag{HedgeFlag::kSpeculation};
};

// A manual verification record is necessary but insufficient: exact same-day broker
// query values must agree before readiness or verified valuation is returned.
class VerifiedTradeAccountingPolicyRegistry {
   public:
    bool LoadFromFile(const std::string& path, const AccountingPolicyRuntimeIdentity& identity,
                      std::string* error);
    bool LoadFromJson(const std::string& text, const AccountingPolicyRuntimeIdentity& identity,
                      std::string* error);
    void BeginSession(std::uint64_t generation);
    void ObserveInstrumentQuery(const QueryResult<InstrumentMetaSnapshot>& result);
    void ObserveCommissionQuery(const QueryResult<InstrumentCommissionRateSnapshot>& result);
    void ObserveOrderCommissionQuery(const QueryResult<InstrumentOrderCommRateSnapshot>& result);
    bool ReadyFor(const std::vector<AccountingPolicyInstrument>& instruments,
                  const std::string& trading_day, std::string* error) const;
    TradeAccountingPolicy Resolve(const Trade& trade, std::string* error = nullptr) const;
    // Only main's validated pre-start WAL range may use restored historical evidence.
    TradeAccountingPolicy ResolveHistorical(const Trade& trade, std::string* error = nullptr) const;
    using EvidenceState = std::unordered_map<std::string, std::string>;
    bool ExportValidatedEvidence(EvidenceState* state, std::string* error) const;
    bool RestoreValidatedEvidence(const EvidenceState& state, std::string* error);

   private:
    struct Record {
        AccountingPolicyInstrument instrument;
        std::string trading_day;
        std::string commission_instrument_id;
        double multiplier{0};
        TradeAccountingPolicy policy;
        std::string descriptor;
    };
    template <typename Row>
    struct Evidence {
        QueryResultMetadata metadata;
        std::vector<Row> rows;
    };
    bool MatchLocked(const Record& record, std::string* error) const;
    const Record* FindLocked(const AccountingPolicyInstrument& instrument,
                             const std::string& day) const;
    bool MetadataMatches(const QueryResultMetadata& metadata, const std::string& day) const;
    AccountingPolicyRuntimeIdentity identity_;
    bool loaded_{false};
    std::uint64_t session_generation_{0};
    std::vector<Record> records_;
    std::map<std::string, Evidence<InstrumentMetaSnapshot>> instruments_;
    std::map<std::string, Evidence<InstrumentCommissionRateSnapshot>> commissions_;
    std::map<std::string, Evidence<InstrumentOrderCommRateSnapshot>> order_commissions_;
    std::map<std::string, EvidenceState> historical_evidence_;
    mutable std::mutex mutex_;
};

}  // namespace quant_hft
