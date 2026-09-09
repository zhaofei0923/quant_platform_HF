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

struct SimNowGenericCloseConvention {
    // CTP instrument/commission queries do not prove how a generic Close fill is allocated
    // between today's and yesterday's positions. Non-explicit-close exchanges therefore require
    // a separately reviewed, versioned convention before broker-observed accounting can open.
    std::string exchange_id;
    GenericClosePriority priority{GenericClosePriority::kUnspecified};
    std::string evidence_source;
    std::string evidence_version;
    std::string evidence_sha256;
};

// Loads only the separately reviewed generic-Close conventions. Fee rates never come from this
// file. The path must be absolute and name a regular, non-symlink file no larger than 64 KiB.
bool LoadSimNowGenericCloseConventionsFromFile(
    const std::string& path, std::vector<SimNowGenericCloseConvention>* conventions,
    std::string* error);

struct SimNowBrokerObservedAccountingOptions {
    // SimNow may not expose InstrumentOrderCommRate. This opt-in treats an entirely absent or
    // successfully completed empty exact-contract response as zero. A failed, mismatched, or
    // nonzero response never falls back to the assumption.
    bool allow_assumed_zero_order_fees{false};
    // Optional explicit gates for DCE/CZCE/GFEX generic Close allocation. SHFE/INE do not use
    // these records because their offsets identify today/yesterday directly. Missing, duplicate,
    // unsupported, unversioned, or undigested conventions fail closed.
    std::vector<SimNowGenericCloseConvention> generic_close_conventions;
};

// A manual verification record is necessary but insufficient: exact same-day broker
// query values must agree before readiness or verified valuation is returned.
class VerifiedTradeAccountingPolicyRegistry {
   public:
    bool LoadFromFile(const std::string& path, const AccountingPolicyRuntimeIdentity& identity,
                      std::string* error);
    bool LoadFromJson(const std::string& text, const AccountingPolicyRuntimeIdentity& identity,
                      std::string* error);
    // Enables a policy assembled only from fresh, exact-contract CTP query results. This mode is
    // deliberately restricted to SimNow. SHFE/INE use explicit offsets; DCE/CZCE/GFEX also
    // require a separately controlled generic-close convention. It cannot be enabled for
    // production.
    bool EnableSimNowBrokerObservedMode(
        const AccountingPolicyRuntimeIdentity& identity,
        const SimNowBrokerObservedAccountingOptions& options, std::string* error);
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
        bool broker_observed{false};
        bool order_fee_assumed_zero{false};
        std::string close_convention_evidence_sha256;
    };
    template <typename Row>
    struct Evidence {
        QueryResultMetadata metadata;
        std::vector<Row> rows;
    };
    bool MatchLocked(const Record& record, std::string* error) const;
    bool MatchBrokerObservedLocked(const Record& record, std::string* error) const;
    bool DeriveBrokerObservedRecordLocked(const AccountingPolicyInstrument& instrument,
                                          const std::string& day, Record* record,
                                          std::string* error) const;
    const Record* BuildBrokerObservedRecordLocked(const AccountingPolicyInstrument& instrument,
                                                  const std::string& day,
                                                  std::string* error) const;
    const Record* FindLocked(const AccountingPolicyInstrument& instrument,
                             const std::string& day) const;
    const Record* FindHistoricalLocked(const AccountingPolicyInstrument& instrument,
                                       const std::string& day) const;
    bool MetadataMatches(const QueryResultMetadata& metadata, const std::string& day) const;
    AccountingPolicyRuntimeIdentity identity_;
    bool loaded_{false};
    bool simnow_broker_observed_mode_{false};
    bool allow_simnow_assumed_zero_order_fees_{false};
    std::map<std::string, SimNowGenericCloseConvention> simnow_generic_close_conventions_;
    std::uint64_t session_generation_{0};
    std::vector<Record> records_;
    std::map<std::string, Evidence<InstrumentMetaSnapshot>> instruments_;
    std::map<std::string, Evidence<InstrumentCommissionRateSnapshot>> commissions_;
    std::map<std::string, Evidence<InstrumentOrderCommRateSnapshot>> order_commissions_;
    std::map<std::string, Evidence<InstrumentMetaSnapshot>> broker_observed_instruments_;
    std::map<std::string, Evidence<InstrumentCommissionRateSnapshot>>
        broker_observed_commissions_;
    std::map<std::string, Evidence<InstrumentOrderCommRateSnapshot>>
        broker_observed_order_commissions_;
    mutable std::vector<Record> broker_observed_records_;
    std::vector<Record> historical_broker_observed_records_;
    std::map<std::string, EvidenceState> historical_evidence_;
    mutable std::mutex mutex_;
};

}  // namespace quant_hft
