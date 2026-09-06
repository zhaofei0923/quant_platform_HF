#include "quant_hft/services/verified_trade_accounting_policy.h"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <set>
#include <sstream>
#include <stdexcept>

#include "quant_hft/core/simple_json.h"

namespace quant_hft {
namespace {
using Json = simple_json::Value;

bool Fail(std::string* error, const std::string& message) {
    if (error) *error = message;
    return false;
}

bool ValidDay(const std::string& day) {
    if (day.size() != 8 ||
        !std::all_of(day.begin(), day.end(), [](unsigned char c) { return c >= '0' && c <= '9'; }))
        return false;
    const int year = std::stoi(day.substr(0, 4));
    const int month = std::stoi(day.substr(4, 2));
    const int date = std::stoi(day.substr(6, 2));
    if (year == 0 || month < 1 || month > 12) return false;
    const int lengths[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    const bool leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    return date > 0 && date <= lengths[month - 1] + (month == 2 && leap ? 1 : 0);
}

void Keys(const Json& value, const std::set<std::string>& expected) {
    if (!value.IsObject() || value.object_value.size() != expected.size())
        throw std::runtime_error("policy object has missing or unknown fields");
    for (const auto& item : value.object_value)
        if (!expected.count(item.first))
            throw std::runtime_error("unknown policy field: " + item.first);
}

std::string String(const Json& value, const std::string& key) {
    const auto* field = value.Find(key);
    if (!field || !field->IsString() || field->string_value.empty())
        throw std::runtime_error("nonempty policy string required: " + key);
    return field->string_value;
}

double Number(const Json& value, const std::string& key) {
    const auto* field = value.Find(key);
    if (!field || !field->IsNumber() || !std::isfinite(field->number_value) ||
        field->number_value < 0)
        throw std::runtime_error("finite nonnegative policy number required: " + key);
    return field->number_value;
}

TradeFeeRate Rate(const Json& value, const std::string& key) {
    const auto* rate = value.Find(key);
    if (!rate) throw std::runtime_error("missing fee rate: " + key);
    Keys(*rate, {"by_money", "by_volume"});
    return {Number(*rate, "by_money"), Number(*rate, "by_volume")};
}

std::string Canonical(const Json& value) {
    std::ostringstream out;
    out << std::setprecision(17);
    if (value.IsString())
        out << std::quoted(value.string_value);
    else if (value.IsNumber())
        out << value.number_value;
    else if (value.IsBool())
        out << (value.bool_value ? "true" : "false");
    else if (value.IsObject()) {
        out << '{';
        bool first = true;
        for (const auto& item : value.object_value) {
            if (!first) out << ',';
            first = false;
            out << std::quoted(item.first) << ':' << Canonical(item.second);
        }
        out << '}';
    } else
        throw std::runtime_error("unsupported canonical policy value");
    return out.str();
}

std::string Scope(const AccountingPolicyInstrument& instrument, const std::string& day) {
    std::ostringstream out;
    for (const auto& text : {day, instrument.instrument_id, instrument.exchange_id})
        out << text.size() << ':' << text;
    out << ':' << static_cast<int>(instrument.hedge_flag);
    return out.str();
}

std::string CtpHedge(HedgeFlag flag) {
    switch (flag) {
        case HedgeFlag::kSpeculation:
            return "1";
        case HedgeFlag::kArbitrage:
            return "2";
        case HedgeFlag::kHedge:
            return "3";
    }
    return {};
}

bool Newer(const QueryResultMetadata& incoming, const QueryResultMetadata& stored) {
    return incoming.generation > stored.generation ||
           (incoming.generation == stored.generation && incoming.request_id >= stored.request_id);
}

std::string QueryReceipt(const QueryResultMetadata& metadata) {
    return std::to_string(metadata.generation) + ":" + std::to_string(metadata.request_id);
}
bool ValidQueryReceipt(const std::string& text) {
    if (text.find_first_not_of("0123456789:") != std::string::npos) return false;
    const auto delimiter = text.find(':');
    if (delimiter == std::string::npos) return false;
    try {
        std::size_t first = 0, second = 0;
        const auto generation = std::stoull(text.substr(0, delimiter), &first);
        const auto request = std::stoull(text.substr(delimiter + 1), &second);
        return generation > 0 && request > 0 && first == delimiter &&
               second == text.size() - delimiter - 1;
    } catch (...) {
        return false;
    }
}
}  // namespace

bool VerifiedTradeAccountingPolicyRegistry::LoadFromFile(
    const std::string& path, const AccountingPolicyRuntimeIdentity& identity, std::string* error) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        std::lock_guard<std::mutex> lock(mutex_);
        loaded_ = false;
        return Fail(error, "verified accounting policy file unavailable");
    }
    std::string text;
    char chunk[4096];
    while (input.read(chunk, sizeof(chunk)) || input.gcount()) {
        text.append(chunk, static_cast<std::size_t>(input.gcount()));
        if (text.size() > 1024 * 1024) {
            std::lock_guard<std::mutex> lock(mutex_);
            loaded_ = false;
            return Fail(error, "verified accounting policy exceeds 1 MiB");
        }
    }
    if (input.bad()) {
        std::lock_guard<std::mutex> lock(mutex_);
        loaded_ = false;
        return Fail(error, "failed reading accounting policy");
    }
    return LoadFromJson(text, identity, error);
}

bool VerifiedTradeAccountingPolicyRegistry::LoadFromJson(
    const std::string& text, const AccountingPolicyRuntimeIdentity& identity, std::string* error) {
    std::lock_guard<std::mutex> lock(mutex_);
    loaded_ = false;
    session_generation_ = 0;
    records_.clear();
    instruments_.clear();
    commissions_.clear();
    order_commissions_.clear();
    historical_evidence_.clear();
    if (error) error->clear();
    try {
        if (text.size() > 1024 * 1024) throw std::runtime_error("policy exceeds 1 MiB");
        Json root;
        std::string parse_error;
        if (!simple_json::ParseStrict(text, &root, &parse_error))
            throw std::runtime_error(parse_error);
        Keys(root, {"schema_version", "environment", "broker_id", "account_id", "records"});
        if (Number(root, "schema_version") != 1 || identity.broker_id.empty() ||
            identity.account_id.empty() ||
            (identity.environment != "simnow" && identity.environment != "prod" &&
             identity.environment != "sim") ||
            String(root, "environment") != identity.environment ||
            String(root, "broker_id") != identity.broker_id ||
            String(root, "account_id") != identity.account_id)
            throw std::runtime_error("policy runtime identity/schema mismatch");
        const auto* records = root.Find("records");
        if (!records || !records->IsArray() || records->array_value.empty() ||
            records->array_value.size() > 512)
            throw std::runtime_error("policy requires 1..512 exact records");
        std::set<std::string> scopes;
        for (const auto& item : records->array_value) {
            Keys(item, {"verified",
                        "instrument_id",
                        "exchange_id",
                        "hedge_flag",
                        "trading_day",
                        "commission_instrument_id",
                        "policy_source",
                        "policy_version",
                        "sample_sha256",
                        "contract_multiplier",
                        "multiplier_source",
                        "commission_source",
                        "profit_basis",
                        "profit_source",
                        "fee_model",
                        "fee_date_basis",
                        "fee_allocation_source",
                        "fee_allocation_version",
                        "generic_close_priority",
                        "close_priority_source",
                        "close_priority_version",
                        "order_fee_policy",
                        "order_fee_source",
                        "open_fee",
                        "close_fee",
                        "close_today_fee"});
            const auto* verified = item.Find("verified");
            if (!verified->IsBool() || !verified->bool_value)
                throw std::runtime_error("manual verification required");
            Record record;
            record.instrument.instrument_id = String(item, "instrument_id");
            record.instrument.exchange_id = String(item, "exchange_id");
            const auto hedge = Number(item, "hedge_flag");
            if (hedge != std::floor(hedge) || hedge > 2)
                throw std::runtime_error("unsupported hedge flag");
            record.instrument.hedge_flag = static_cast<HedgeFlag>(static_cast<int>(hedge));
            record.trading_day = String(item, "trading_day");
            if (!ValidDay(record.trading_day))
                throw std::runtime_error("invalid policy trading day");
            if (!scopes.insert(Scope(record.instrument, record.trading_day)).second)
                throw std::runtime_error("duplicate exact accounting policy scope");
            record.commission_instrument_id = String(item, "commission_instrument_id");
            record.multiplier = Number(item, "contract_multiplier");
            if (record.multiplier <= 0 || record.multiplier != std::floor(record.multiplier))
                throw std::runtime_error("positive integer contract multiplier required");
            const auto digest = String(item, "sample_sha256");
            if (digest.size() != 64 ||
                !std::all_of(digest.begin(), digest.end(),
                             [](unsigned char c) { return std::isxdigit(c) != 0; }))
                throw std::runtime_error("sample SHA256 required");
            if (String(item, "fee_model") != "money_plus_volume_v1" ||
                String(item, "fee_date_basis") != "close_allocation_v1" ||
                String(item, "profit_basis") != "opening_lot_v1" ||
                String(item, "order_fee_policy") != "verified_zero_v1")
                throw std::runtime_error("unsupported verified fee/profit/date basis");
            auto& policy = record.policy;
            policy.contract_multiplier = record.multiplier;
            policy.valuation_inputs_verified = true;
            policy.fee_model = TradeFeeModel::kMoneyPlusVolumeV1;
            policy.open_fee = Rate(item, "open_fee");
            policy.close_fee = Rate(item, "close_fee");
            policy.close_today_fee = Rate(item, "close_today_fee");
            policy.fee_date_basis = String(item, "fee_date_basis");
            policy.fee_allocation_source = String(item, "fee_allocation_source");
            policy.fee_allocation_version = String(item, "fee_allocation_version");
            policy.close_rule_source = String(item, "close_priority_source");
            policy.close_rule_version = String(item, "close_priority_version");
            const auto priority = String(item, "generic_close_priority");
            const bool explicit_exchange =
                record.instrument.exchange_id == "SHFE" || record.instrument.exchange_id == "INE";
            if (explicit_exchange && priority != "exchange_explicit")
                throw std::runtime_error("SHFE/INE require explicit exchange offsets");
            if (!explicit_exchange) {
                if (priority == "today_first")
                    policy.generic_close_priority = GenericClosePriority::kTodayFirst;
                else if (priority == "yesterday_first")
                    policy.generic_close_priority = GenericClosePriority::kYesterdayFirst;
                else
                    throw std::runtime_error("verified generic close priority required");
            }
            policy.valuation_source = String(item, "policy_source") + "@" +
                                      String(item, "policy_version") + ";sample_sha256=" + digest +
                                      ";multiplier=" + String(item, "multiplier_source") +
                                      ";commission=" + String(item, "commission_source") +
                                      ";profit=" + String(item, "profit_source") +
                                      ";order_fee=" + String(item, "order_fee_source");
            record.descriptor = Canonical(item);
            records_.push_back(std::move(record));
        }
        identity_ = identity;
        loaded_ = true;
        return true;
    } catch (const std::exception& ex) {
        return Fail(error, ex.what());
    }
}

bool VerifiedTradeAccountingPolicyRegistry::MetadataMatches(const QueryResultMetadata& metadata,
                                                            const std::string& day) const {
    return metadata.success && metadata.complete && metadata.error_code == 0 &&
           metadata.generation > 0 &&
           (session_generation_ == 0 || metadata.generation == session_generation_) &&
           metadata.request_id > 0 && metadata.account_id == identity_.account_id &&
           metadata.trading_day == day &&
           metadata.source == (identity_.environment == "sim" ? "simulated" : "ctp");
}

void VerifiedTradeAccountingPolicyRegistry::BeginSession(std::uint64_t generation) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (generation == session_generation_) return;
    session_generation_ = generation;
    instruments_.clear();
    commissions_.clear();
    order_commissions_.clear();
}

void VerifiedTradeAccountingPolicyRegistry::ObserveInstrumentQuery(
    const QueryResult<InstrumentMetaSnapshot>& result) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (session_generation_ != 0 && result.metadata.generation != session_generation_) return;
    for (const auto& record : records_) {
        if (!result.metadata.instrument_id.empty() &&
            result.metadata.instrument_id != record.instrument.instrument_id)
            continue;
        auto& evidence = instruments_[Scope(record.instrument, record.trading_day)];
        if (Newer(result.metadata, evidence.metadata)) evidence = {result.metadata, result.rows};
    }
}
void VerifiedTradeAccountingPolicyRegistry::ObserveCommissionQuery(
    const QueryResult<InstrumentCommissionRateSnapshot>& result) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (session_generation_ != 0 && result.metadata.generation != session_generation_) return;
    for (const auto& record : records_) {
        if (!result.metadata.instrument_id.empty() &&
            result.metadata.instrument_id != record.instrument.instrument_id)
            continue;
        auto& evidence = commissions_[Scope(record.instrument, record.trading_day)];
        if (Newer(result.metadata, evidence.metadata)) evidence = {result.metadata, result.rows};
    }
}
void VerifiedTradeAccountingPolicyRegistry::ObserveOrderCommissionQuery(
    const QueryResult<InstrumentOrderCommRateSnapshot>& result) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (session_generation_ != 0 && result.metadata.generation != session_generation_) return;
    for (const auto& record : records_) {
        if (!result.metadata.instrument_id.empty() &&
            result.metadata.instrument_id != record.instrument.instrument_id)
            continue;
        auto& evidence = order_commissions_[Scope(record.instrument, record.trading_day)];
        if (Newer(result.metadata, evidence.metadata)) evidence = {result.metadata, result.rows};
    }
}

const VerifiedTradeAccountingPolicyRegistry::Record*
VerifiedTradeAccountingPolicyRegistry::FindLocked(const AccountingPolicyInstrument& instrument,
                                                  const std::string& day) const {
    if (!loaded_) return nullptr;
    for (const auto& record : records_)
        if (Scope(record.instrument, record.trading_day) == Scope(instrument, day)) return &record;
    return nullptr;
}

bool VerifiedTradeAccountingPolicyRegistry::MatchLocked(const Record& record,
                                                        std::string* error) const {
    const auto key = Scope(record.instrument, record.trading_day);
    const auto instrument = instruments_.find(key);
    const auto commission = commissions_.find(key);
    const auto order_fee = order_commissions_.find(key);
    if (instrument == instruments_.end() || commission == commissions_.end() ||
        order_fee == order_commissions_.end())
        return Fail(error, "same-day instrument/commission/order-fee query evidence required");
    if (instrument->second.metadata.generation != commission->second.metadata.generation ||
        instrument->second.metadata.generation != order_fee->second.metadata.generation)
        return Fail(error, "accounting queries belong to different sessions");
    if (!MetadataMatches(instrument->second.metadata, record.trading_day) ||
        !MetadataMatches(commission->second.metadata, record.trading_day) ||
        !MetadataMatches(order_fee->second.metadata, record.trading_day))
        return Fail(error, "query identity/day/source/completion mismatch");
    const auto source = identity_.environment == "sim" ? "simulated" : "ctp";
    int matched = 0;
    for (const auto& row : instrument->second.rows) {
        if (row.instrument_id != record.instrument.instrument_id) continue;
        ++matched;
        if (row.exchange_id != record.instrument.exchange_id ||
            row.volume_multiple != record.multiplier || row.source != source)
            return Fail(error, "queried instrument differs from verified policy");
    }
    if (matched != 1) return Fail(error, "exactly one instrument query row required");
    matched = 0;
    for (const auto& row : commission->second.rows) {
        if (row.instrument_id != record.commission_instrument_id) continue;
        ++matched;
        const auto& policy = record.policy;
        if (record.commission_instrument_id != record.instrument.instrument_id &&
            commission->second.metadata.instrument_id != record.instrument.instrument_id)
            return Fail(error, "product commission requires explicit exact-contract query scope");
        if (row.account_id != identity_.account_id || row.investor_id != identity_.account_id ||
            row.exchange_id != record.instrument.exchange_id || row.source != source ||
            row.open_ratio_by_money != policy.open_fee.by_money ||
            row.open_ratio_by_volume != policy.open_fee.by_volume ||
            row.close_ratio_by_money != policy.close_fee.by_money ||
            row.close_ratio_by_volume != policy.close_fee.by_volume ||
            row.close_today_ratio_by_money != policy.close_today_fee.by_money ||
            row.close_today_ratio_by_volume != policy.close_today_fee.by_volume)
            return Fail(error, "queried commission differs from verified policy");
    }
    if (matched != 1) return Fail(error, "exactly one commission query row required");
    matched = 0;
    for (const auto& row : order_fee->second.rows) {
        if (row.instrument_id != record.instrument.instrument_id) continue;
        ++matched;
        if (row.account_id != identity_.account_id || row.investor_id != identity_.account_id ||
            row.exchange_id != record.instrument.exchange_id ||
            row.hedge_flag != CtpHedge(record.instrument.hedge_flag) || row.source != source ||
            row.order_comm_by_volume != 0 || row.order_action_comm_by_volume != 0)
            return Fail(error, "nonzero or mismatched order/cancel fees are unsupported");
    }
    if (matched != 1) return Fail(error, "exactly one order-fee query row required");
    return true;
}

bool VerifiedTradeAccountingPolicyRegistry::ReadyFor(
    const std::vector<AccountingPolicyInstrument>& instruments, const std::string& trading_day,
    std::string* error) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (error) error->clear();
    if (!loaded_ || !ValidDay(trading_day) || instruments.empty())
        return Fail(error, "verified policy and current exact instrument set required");
    for (const auto& instrument : instruments) {
        const auto* record = FindLocked(instrument, trading_day);
        if (!record) return Fail(error, "no verified accounting policy for exact scope/day");
        if (!MatchLocked(*record, error)) return false;
    }
    return true;
}

TradeAccountingPolicy VerifiedTradeAccountingPolicyRegistry::Resolve(const Trade& trade,
                                                                     std::string* error) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (error) error->clear();
    const auto* record =
        FindLocked({trade.symbol, trade.exchange, trade.hedge_flag}, trade.trading_day);
    if (!record || trade.account_id != identity_.account_id ||
        trade.broker_id != identity_.broker_id || !MatchLocked(*record, error)) {
        if (error && error->empty()) *error = "trade has no exact verified accounting policy";
        return {};
    }
    return record->policy;
}

TradeAccountingPolicy VerifiedTradeAccountingPolicyRegistry::ResolveHistorical(
    const Trade& trade, std::string* error) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto* record =
        FindLocked({trade.symbol, trade.exchange, trade.hedge_flag}, trade.trading_day);
    if (!record || trade.account_id != identity_.account_id ||
        trade.broker_id != identity_.broker_id || !historical_evidence_.count(record->descriptor)) {
        Fail(error, "historical WAL trade lacks durable matching policy evidence");
        return {};
    }
    if (error) error->clear();
    return record->policy;
}

bool VerifiedTradeAccountingPolicyRegistry::ExportValidatedEvidence(EvidenceState* state,
                                                                    std::string* error) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!loaded_ || !state) return Fail(error, "policy evidence output/loaded policy required");
    auto receipts = historical_evidence_;
    for (const auto& record : records_) {
        if (!MatchLocked(record, nullptr)) continue;
        const auto key = Scope(record.instrument, record.trading_day);
        receipts[record.descriptor] = {
            {"instrument", QueryReceipt(instruments_.at(key).metadata)},
            {"commission", QueryReceipt(commissions_.at(key).metadata)},
            {"order_fee", QueryReceipt(order_commissions_.at(key).metadata)}};
    }
    state->clear();
    (*state)["schema_version"] = "1";
    (*state)["environment"] = identity_.environment;
    (*state)["broker_id"] = identity_.broker_id;
    (*state)["account_id"] = identity_.account_id;
    (*state)["count"] = std::to_string(receipts.size());
    std::size_t index = 0;
    for (const auto& receipt : receipts) {
        const auto prefix = "record." + std::to_string(index++) + ".";
        (*state)[prefix + "policy"] = receipt.first;
        for (const auto& item : receipt.second) (*state)[prefix + item.first] = item.second;
    }
    return true;
}

bool VerifiedTradeAccountingPolicyRegistry::RestoreValidatedEvidence(const EvidenceState& state,
                                                                     std::string* error) {
    std::lock_guard<std::mutex> lock(mutex_);
    historical_evidence_.clear();
    try {
        if (!loaded_ || state.at("schema_version") != "1" ||
            state.at("environment") != identity_.environment ||
            state.at("broker_id") != identity_.broker_id ||
            state.at("account_id") != identity_.account_id)
            throw std::runtime_error("policy evidence identity/schema mismatch");
        std::size_t consumed = 0;
        const auto count = std::stoull(state.at("count"), &consumed);
        if (count > 512 || consumed != state.at("count").size() || state.size() != 5 + count * 4)
            throw std::runtime_error("invalid policy evidence count/fields");
        std::map<std::string, EvidenceState> restored;
        for (std::size_t i = 0; i < count; ++i) {
            const auto prefix = "record." + std::to_string(i) + ".";
            const auto descriptor = state.at(prefix + "policy");
            const auto found =
                std::find_if(records_.begin(), records_.end(),
                             [&](const Record& record) { return record.descriptor == descriptor; });
            if (found == records_.end()) continue;  // Removed policies cannot authorize recovery.
            EvidenceState receipt;
            for (const auto* name : {"instrument", "commission", "order_fee"}) {
                const auto value = state.at(prefix + name);
                if (!ValidQueryReceipt(value))
                    throw std::runtime_error("invalid persisted query verification receipt");
                receipt[name] = value;
            }
            if (!restored.emplace(descriptor, std::move(receipt)).second)
                throw std::runtime_error("duplicate persisted policy evidence");
        }
        historical_evidence_ = std::move(restored);
        return true;
    } catch (const std::exception& ex) {
        return Fail(error, ex.what());
    }
}

}  // namespace quant_hft
