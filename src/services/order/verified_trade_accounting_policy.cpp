#include "quant_hft/services/verified_trade_accounting_policy.h"

#include <algorithm>
#include <cmath>
#include <filesystem>
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

constexpr const char* kBrokerObservedPolicyVersion = "simnow-broker-observed-v2";
constexpr const char* kLegacyBrokerObservedPolicyVersion = "simnow-broker-observed-v1";
constexpr const char* kVerifiedZeroOrderFees = "ctp_verified_zero_v1";
constexpr const char* kAssumedZeroOrderFees = "simnow_assumed_zero_v1";
constexpr const char* kAssumedZeroOrderFeeReceipt = "simnow-assumed-zero:explicit-v1";

bool ValidSha256(const std::string& digest) {
    return digest.size() == 64 &&
           std::all_of(digest.begin(), digest.end(),
                       [](unsigned char c) { return std::isxdigit(c) != 0; });
}

bool ValidEvidenceLabel(const std::string& value) {
    return !value.empty() && value.size() <= 256 &&
           std::all_of(value.begin(), value.end(), [](unsigned char c) {
               return c >= 0x20 && c != 0x7f;
           });
}

bool ExplicitCloseExchange(const std::string& exchange_id) {
    return exchange_id == "SHFE" || exchange_id == "INE";
}

bool GenericCloseExchange(const std::string& exchange_id) {
    return exchange_id == "DCE" || exchange_id == "CZCE" || exchange_id == "GFEX";
}

bool SupportedBrokerObservedExchange(const std::string& exchange_id) {
    return ExplicitCloseExchange(exchange_id) || GenericCloseExchange(exchange_id);
}

bool ValidateGenericCloseConvention(const SimNowGenericCloseConvention& convention,
                                    std::string* error) {
    if (!GenericCloseExchange(convention.exchange_id))
        return Fail(error, "generic-close convention is only valid for DCE/CZCE/GFEX");
    if (convention.priority != GenericClosePriority::kTodayFirst &&
        convention.priority != GenericClosePriority::kYesterdayFirst)
        return Fail(error, "generic-close convention requires an explicit priority");
    if (!ValidEvidenceLabel(convention.evidence_source) ||
        !ValidEvidenceLabel(convention.evidence_version) ||
        !ValidSha256(convention.evidence_sha256))
        return Fail(error, "generic-close convention requires source/version/SHA-256 evidence");
    return true;
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

void AppendDescriptorField(std::ostringstream* out, const std::string& value) {
    *out << value.size() << ':' << value;
}

std::string LegacyBrokerObservedDescriptor(const AccountingPolicyInstrument& instrument,
                                           const std::string& day, double multiplier,
                                           const TradeAccountingPolicy& policy,
                                           bool order_fee_assumed_zero) {
    std::ostringstream out;
    out << std::setprecision(17) << kLegacyBrokerObservedPolicyVersion << '|';
    AppendDescriptorField(&out, instrument.instrument_id);
    AppendDescriptorField(&out, instrument.exchange_id);
    out << '|' << static_cast<int>(instrument.hedge_flag) << '|';
    AppendDescriptorField(&out, day);
    out << '|' << multiplier << '|' << policy.open_fee.by_money << '|'
        << policy.open_fee.by_volume << '|' << policy.close_fee.by_money << '|'
        << policy.close_fee.by_volume << '|' << policy.close_today_fee.by_money << '|'
        << policy.close_today_fee.by_volume << '|'
        << (order_fee_assumed_zero ? kAssumedZeroOrderFees : kVerifiedZeroOrderFees);
    return out.str();
}

std::string BrokerObservedDescriptor(const AccountingPolicyInstrument& instrument,
                                     const std::string& day, double multiplier,
                                     const TradeAccountingPolicy& policy,
                                     bool order_fee_assumed_zero,
                                     const std::string& close_convention_evidence_sha256) {
    std::ostringstream out;
    out << std::setprecision(17) << kBrokerObservedPolicyVersion << '|';
    AppendDescriptorField(&out, instrument.instrument_id);
    AppendDescriptorField(&out, instrument.exchange_id);
    out << '|' << static_cast<int>(instrument.hedge_flag) << '|';
    AppendDescriptorField(&out, day);
    out << '|' << multiplier << '|' << policy.open_fee.by_money << '|'
        << policy.open_fee.by_volume << '|' << policy.close_fee.by_money << '|'
        << policy.close_fee.by_volume << '|' << policy.close_today_fee.by_money << '|'
        << policy.close_today_fee.by_volume << '|'
        << (order_fee_assumed_zero ? kAssumedZeroOrderFees : kVerifiedZeroOrderFees) << '|'
        << static_cast<int>(policy.generic_close_priority) << '|';
    AppendDescriptorField(&out, policy.close_rule_source);
    AppendDescriptorField(&out, policy.close_rule_version);
    AppendDescriptorField(&out, close_convention_evidence_sha256);
    return out.str();
}

bool FiniteNonnegative(double value) { return std::isfinite(value) && value >= 0; }

// CTP may omit ExchangeID from a fee row even though its request envelope is scoped to one exact
// contract.  This fallback is valid only where the caller has already verified a unique,
// same-session Instrument row and uses that row as the exchange identity anchor.
bool FeeRowExchangeMatchesInstrument(const std::string& fee_exchange_id,
                                     const std::string& instrument_exchange_id) {
    return !instrument_exchange_id.empty() &&
           (fee_exchange_id.empty() || fee_exchange_id == instrument_exchange_id);
}

double ParseFiniteNonnegative(const std::string& text, const std::string& name) {
    std::size_t consumed = 0;
    const double value = std::stod(text, &consumed);
    if (consumed != text.size() || !FiniteNonnegative(value))
        throw std::runtime_error("invalid broker-observed evidence number: " + name);
    return value;
}

int ParseInteger(const std::string& text, const std::string& name) {
    std::size_t consumed = 0;
    const long value = std::stol(text, &consumed);
    if (consumed != text.size() || value < 0 || value > 2)
        throw std::runtime_error("invalid broker-observed evidence integer: " + name);
    return static_cast<int>(value);
}
}  // namespace

bool LoadSimNowGenericCloseConventionsFromFile(
    const std::string& path_text, std::vector<SimNowGenericCloseConvention>* conventions,
    std::string* error) {
    if (error) error->clear();
    if (conventions == nullptr) return Fail(error, "generic-close convention output is required");
    try {
        const std::filesystem::path path(path_text);
        if (path_text.empty() || !path.is_absolute())
            throw std::runtime_error("generic-close convention path must be absolute");
        std::error_code status_error;
        const auto status = std::filesystem::symlink_status(path, status_error);
        if (status_error || std::filesystem::is_symlink(status) ||
            !std::filesystem::is_regular_file(status))
            throw std::runtime_error(
                "generic-close convention path must be a regular non-symlink file");
        constexpr std::uintmax_t kMaximumBytes = 64U * 1024U;
        const auto size = std::filesystem::file_size(path, status_error);
        if (status_error || size > kMaximumBytes)
            throw std::runtime_error("generic-close convention file exceeds 64 KiB");
        std::ifstream input(path, std::ios::binary);
        if (!input) throw std::runtime_error("generic-close convention file unavailable");
        std::string text;
        char chunk[4096];
        while (input.read(chunk, sizeof(chunk)) || input.gcount()) {
            text.append(chunk, static_cast<std::size_t>(input.gcount()));
            if (text.size() > kMaximumBytes)
                throw std::runtime_error("generic-close convention file exceeds 64 KiB");
        }
        if (input.bad()) throw std::runtime_error("failed reading generic-close convention file");

        Json root;
        std::string parse_error;
        if (!simple_json::ParseStrict(text, &root, &parse_error))
            throw std::runtime_error(parse_error);
        Keys(root, {"schema_version", "environment", "conventions"});
        if (Number(root, "schema_version") != 1 || String(root, "environment") != "simnow")
            throw std::runtime_error("generic-close convention identity/schema mismatch");
        const auto* items = root.Find("conventions");
        if (!items || !items->IsArray() || items->array_value.empty() ||
            items->array_value.size() > 3)
            throw std::runtime_error("generic-close convention file requires 1..3 records");

        std::vector<SimNowGenericCloseConvention> parsed;
        std::set<std::string> exchanges;
        parsed.reserve(items->array_value.size());
        for (const auto& item : items->array_value) {
            Keys(item, {"exchange_id", "generic_close_priority", "evidence_source",
                        "evidence_version", "evidence_sha256"});
            SimNowGenericCloseConvention convention;
            convention.exchange_id = String(item, "exchange_id");
            const auto priority = String(item, "generic_close_priority");
            if (priority == "today_first")
                convention.priority = GenericClosePriority::kTodayFirst;
            else if (priority == "yesterday_first")
                convention.priority = GenericClosePriority::kYesterdayFirst;
            else
                throw std::runtime_error(
                    "generic-close priority must be today_first or yesterday_first");
            convention.evidence_source = String(item, "evidence_source");
            convention.evidence_version = String(item, "evidence_version");
            convention.evidence_sha256 = String(item, "evidence_sha256");
            std::string validation_error;
            if (!ValidateGenericCloseConvention(convention, &validation_error))
                throw std::runtime_error(validation_error);
            if (!exchanges.insert(convention.exchange_id).second)
                throw std::runtime_error("duplicate generic-close exchange convention");
            parsed.push_back(std::move(convention));
        }
        *conventions = std::move(parsed);
        return true;
    } catch (const std::exception& ex) {
        return Fail(error, ex.what());
    }
}

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
    simnow_broker_observed_mode_ = false;
    allow_simnow_assumed_zero_order_fees_ = false;
    simnow_generic_close_conventions_.clear();
    session_generation_ = 0;
    records_.clear();
    instruments_.clear();
    commissions_.clear();
    order_commissions_.clear();
    broker_observed_instruments_.clear();
    broker_observed_commissions_.clear();
    broker_observed_order_commissions_.clear();
    broker_observed_records_.clear();
    historical_broker_observed_records_.clear();
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

bool VerifiedTradeAccountingPolicyRegistry::EnableSimNowBrokerObservedMode(
    const AccountingPolicyRuntimeIdentity& identity,
    const SimNowBrokerObservedAccountingOptions& options, std::string* error) {
    std::lock_guard<std::mutex> lock(mutex_);
    loaded_ = false;
    simnow_broker_observed_mode_ = false;
    allow_simnow_assumed_zero_order_fees_ = false;
    simnow_generic_close_conventions_.clear();
    session_generation_ = 0;
    records_.clear();
    instruments_.clear();
    commissions_.clear();
    order_commissions_.clear();
    broker_observed_instruments_.clear();
    broker_observed_commissions_.clear();
    broker_observed_order_commissions_.clear();
    broker_observed_records_.clear();
    historical_broker_observed_records_.clear();
    historical_evidence_.clear();
    if (error) error->clear();
    if (identity.environment != "simnow")
        return Fail(error, "broker-observed accounting is restricted to SimNow");
    if (identity.broker_id.empty() || identity.account_id.empty())
        return Fail(error, "broker-observed accounting requires broker and account identity");
    if (options.generic_close_conventions.size() > 3)
        return Fail(error, "at most one generic-close convention per supported exchange");
    std::map<std::string, SimNowGenericCloseConvention> conventions;
    for (const auto& convention : options.generic_close_conventions) {
        if (!ValidateGenericCloseConvention(convention, error)) return false;
        if (!conventions.emplace(convention.exchange_id, convention).second)
            return Fail(error, "duplicate generic-close exchange convention");
    }
    identity_ = identity;
    allow_simnow_assumed_zero_order_fees_ = options.allow_assumed_zero_order_fees;
    simnow_generic_close_conventions_ = std::move(conventions);
    simnow_broker_observed_mode_ = true;
    loaded_ = true;
    return true;
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
    broker_observed_instruments_.clear();
    broker_observed_commissions_.clear();
    broker_observed_order_commissions_.clear();
    broker_observed_records_.clear();
}

void VerifiedTradeAccountingPolicyRegistry::ObserveInstrumentQuery(
    const QueryResult<InstrumentMetaSnapshot>& result) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (session_generation_ != 0 && result.metadata.generation != session_generation_) return;
    if (simnow_broker_observed_mode_) {
        if (result.metadata.instrument_id.empty()) return;
        auto& evidence = broker_observed_instruments_[result.metadata.instrument_id];
        if (Newer(result.metadata, evidence.metadata)) evidence = {result.metadata, result.rows};
        broker_observed_records_.erase(
            std::remove_if(broker_observed_records_.begin(), broker_observed_records_.end(),
                           [&](const Record& record) {
                               return record.instrument.instrument_id ==
                                      result.metadata.instrument_id;
                           }),
            broker_observed_records_.end());
        return;
    }
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
    if (simnow_broker_observed_mode_) {
        if (result.metadata.instrument_id.empty()) return;
        auto& evidence = broker_observed_commissions_[result.metadata.instrument_id];
        if (Newer(result.metadata, evidence.metadata)) evidence = {result.metadata, result.rows};
        broker_observed_records_.erase(
            std::remove_if(broker_observed_records_.begin(), broker_observed_records_.end(),
                           [&](const Record& record) {
                               return record.instrument.instrument_id ==
                                      result.metadata.instrument_id;
                           }),
            broker_observed_records_.end());
        return;
    }
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
    if (simnow_broker_observed_mode_) {
        if (result.metadata.instrument_id.empty()) return;
        auto& evidence = broker_observed_order_commissions_[result.metadata.instrument_id];
        if (Newer(result.metadata, evidence.metadata)) evidence = {result.metadata, result.rows};
        broker_observed_records_.erase(
            std::remove_if(broker_observed_records_.begin(), broker_observed_records_.end(),
                           [&](const Record& record) {
                               return record.instrument.instrument_id ==
                                      result.metadata.instrument_id;
                           }),
            broker_observed_records_.end());
        return;
    }
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
    for (const auto& record : broker_observed_records_)
        if (Scope(record.instrument, record.trading_day) == Scope(instrument, day)) return &record;
    return nullptr;
}

const VerifiedTradeAccountingPolicyRegistry::Record*
VerifiedTradeAccountingPolicyRegistry::FindHistoricalLocked(
    const AccountingPolicyInstrument& instrument, const std::string& day) const {
    for (const auto& record : records_)
        if (Scope(record.instrument, record.trading_day) == Scope(instrument, day)) return &record;
    const Record* match = nullptr;
    for (const auto& record : historical_broker_observed_records_) {
        if (Scope(record.instrument, record.trading_day) != Scope(instrument, day)) continue;
        if (match && match->descriptor != record.descriptor) return nullptr;
        match = &record;
    }
    return match;
}

bool VerifiedTradeAccountingPolicyRegistry::DeriveBrokerObservedRecordLocked(
    const AccountingPolicyInstrument& instrument, const std::string& day, Record* record,
    std::string* error) const {
    if (!simnow_broker_observed_mode_ || identity_.environment != "simnow" || !record ||
        !ValidDay(day) || instrument.instrument_id.empty() ||
        !SupportedBrokerObservedExchange(instrument.exchange_id) ||
        CtpHedge(instrument.hedge_flag).empty())
        return Fail(error,
                    "SimNow broker-observed accounting requires an exact supported scope/day");
    const SimNowGenericCloseConvention* generic_close_convention = nullptr;
    if (GenericCloseExchange(instrument.exchange_id)) {
        const auto found = simnow_generic_close_conventions_.find(instrument.exchange_id);
        if (found == simnow_generic_close_conventions_.end())
            return Fail(error,
                        "generic Close accounting requires reviewed exchange convention evidence");
        generic_close_convention = &found->second;
    }
    if (session_generation_ == 0)
        return Fail(error, "broker-observed accounting requires an active query session");

    const auto instrument_evidence = broker_observed_instruments_.find(instrument.instrument_id);
    const auto commission_evidence = broker_observed_commissions_.find(instrument.instrument_id);
    if (instrument_evidence == broker_observed_instruments_.end() ||
        commission_evidence == broker_observed_commissions_.end())
        return Fail(error,
                    "same-session exact instrument and commission query evidence required");

    const auto exact_metadata = [&](const QueryResultMetadata& metadata,
                                    const std::string& query_name) {
        return MetadataMatches(metadata, day) && metadata.generation == session_generation_ &&
               metadata.instrument_id == instrument.instrument_id && !metadata.full_account &&
               metadata.query_name == query_name;
    };
    if (instrument_evidence->second.metadata.generation !=
            commission_evidence->second.metadata.generation ||
        !exact_metadata(instrument_evidence->second.metadata, "Instrument") ||
        !exact_metadata(commission_evidence->second.metadata, "InstrumentCommissionRate"))
        return Fail(error, "broker-observed queries differ in session/scope/day/identity");
    if (instrument_evidence->second.rows.size() != 1)
        return Fail(error, "exactly one exact-contract instrument row required");
    const auto& meta = instrument_evidence->second.rows.front();
    if (meta.instrument_id != instrument.instrument_id ||
        meta.exchange_id != instrument.exchange_id || meta.volume_multiple <= 0 ||
        meta.source != "ctp" || meta.ts_ns <= 0)
        return Fail(error, "queried instrument is not a complete exact CTP contract fact");
    if (commission_evidence->second.rows.size() != 1)
        return Fail(error, "exactly one request-scoped commission row required");
    const auto& commission = commission_evidence->second.rows.front();
    const bool mapped_product = !meta.product_id.empty() && commission.instrument_id == meta.product_id;
    if ((commission.instrument_id != instrument.instrument_id && !mapped_product) ||
        commission.account_id != identity_.account_id ||
        commission.investor_id != identity_.account_id ||
        !FeeRowExchangeMatchesInstrument(commission.exchange_id, meta.exchange_id) ||
        commission.source != "ctp" ||
        commission.ts_ns <= 0 || !FiniteNonnegative(commission.open_ratio_by_money) ||
        !FiniteNonnegative(commission.open_ratio_by_volume) ||
        !FiniteNonnegative(commission.close_ratio_by_money) ||
        !FiniteNonnegative(commission.close_ratio_by_volume) ||
        !FiniteNonnegative(commission.close_today_ratio_by_money) ||
        !FiniteNonnegative(commission.close_today_ratio_by_volume))
        return Fail(error, "queried commission is not a complete exact-account CTP fact");

    bool order_fee_assumed_zero = false;
    const auto order_fee = broker_observed_order_commissions_.find(instrument.instrument_id);
    if (order_fee == broker_observed_order_commissions_.end()) {
        if (!allow_simnow_assumed_zero_order_fees_)
            return Fail(error, "exact order-fee query evidence required");
        order_fee_assumed_zero = true;
    } else {
        if (order_fee->second.metadata.generation !=
                instrument_evidence->second.metadata.generation ||
            !exact_metadata(order_fee->second.metadata, "InstrumentOrderCommRate"))
            return Fail(error, "order-fee query failed or differs in session/scope/day/identity");
        if (order_fee->second.rows.empty()) {
            if (!allow_simnow_assumed_zero_order_fees_)
                return Fail(error, "exactly one order-fee query row required");
            order_fee_assumed_zero = true;
        } else {
            if (order_fee->second.rows.size() != 1)
                return Fail(error, "exactly one order-fee query row required");
            const auto& row = order_fee->second.rows.front();
            if (row.account_id != identity_.account_id ||
                row.investor_id != identity_.account_id ||
                row.instrument_id != instrument.instrument_id ||
                !FeeRowExchangeMatchesInstrument(row.exchange_id, meta.exchange_id) ||
                row.hedge_flag != CtpHedge(instrument.hedge_flag) || row.source != "ctp" ||
                row.ts_ns <= 0 || !FiniteNonnegative(row.order_comm_by_volume) ||
                !FiniteNonnegative(row.order_action_comm_by_volume) ||
                row.order_comm_by_volume != 0 || row.order_action_comm_by_volume != 0)
                return Fail(error, "nonzero or mismatched order/cancel fees are unsupported");
        }
    }

    Record derived;
    derived.instrument = instrument;
    derived.trading_day = day;
    derived.commission_instrument_id = commission.instrument_id;
    derived.multiplier = static_cast<double>(meta.volume_multiple);
    derived.broker_observed = true;
    derived.order_fee_assumed_zero = order_fee_assumed_zero;
    auto& policy = derived.policy;
    policy.valuation_inputs_verified = true;
    policy.contract_multiplier = derived.multiplier;
    policy.fee_model = TradeFeeModel::kMoneyPlusVolumeV1;
    policy.open_fee = {commission.open_ratio_by_money, commission.open_ratio_by_volume};
    policy.close_fee = {commission.close_ratio_by_money, commission.close_ratio_by_volume};
    policy.close_today_fee = {commission.close_today_ratio_by_money,
                              commission.close_today_ratio_by_volume};
    policy.fee_date_basis = "close_allocation_v1";
    policy.fee_allocation_version = kBrokerObservedPolicyVersion;
    policy.close_rule_version = kBrokerObservedPolicyVersion;
    if (generic_close_convention == nullptr) {
        policy.fee_allocation_source = "simnow_ctp_explicit_offset_and_commission_query";
        policy.close_rule_source = "ctp_explicit_offset";
    } else {
        policy.generic_close_priority = generic_close_convention->priority;
        policy.fee_allocation_source =
            "simnow_ctp_generic_close_convention_and_commission_query";
        policy.close_rule_source = generic_close_convention->evidence_source;
        policy.close_rule_version = generic_close_convention->evidence_version;
        derived.close_convention_evidence_sha256 =
            generic_close_convention->evidence_sha256;
    }
    policy.valuation_source =
        std::string(kBrokerObservedPolicyVersion) + ";instrument=ctp_same_session_exact" +
        ";commission=ctp_same_session_exact" +
        ";fee_exchange=exact_instrument_query_anchor;order_fee=" +
        (order_fee_assumed_zero ? kAssumedZeroOrderFees : kVerifiedZeroOrderFees) +
        (generic_close_convention == nullptr
             ? ";close_convention=ctp_explicit_offset"
             : ";close_convention=" + generic_close_convention->evidence_source + "@" +
                   generic_close_convention->evidence_version + ";close_convention_sha256=" +
                   generic_close_convention->evidence_sha256);
    derived.descriptor = BrokerObservedDescriptor(instrument, day, derived.multiplier, policy,
                                                  order_fee_assumed_zero,
                                                  derived.close_convention_evidence_sha256);
    *record = std::move(derived);
    return true;
}

const VerifiedTradeAccountingPolicyRegistry::Record*
VerifiedTradeAccountingPolicyRegistry::BuildBrokerObservedRecordLocked(
    const AccountingPolicyInstrument& instrument, const std::string& day,
    std::string* error) const {
    Record derived;
    if (!DeriveBrokerObservedRecordLocked(instrument, day, &derived, error)) return nullptr;
    broker_observed_records_.erase(
        std::remove_if(broker_observed_records_.begin(), broker_observed_records_.end(),
                       [&](const Record& candidate) {
                           return Scope(candidate.instrument, candidate.trading_day) ==
                                  Scope(instrument, day);
                       }),
        broker_observed_records_.end());
    broker_observed_records_.push_back(std::move(derived));
    return &broker_observed_records_.back();
}

bool VerifiedTradeAccountingPolicyRegistry::MatchBrokerObservedLocked(
    const Record& record, std::string* error) const {
    Record current;
    return DeriveBrokerObservedRecordLocked(record.instrument, record.trading_day, &current,
                                            error) &&
           (current.descriptor == record.descriptor ||
            Fail(error, "broker-observed policy changed after query evidence was captured"));
}

bool VerifiedTradeAccountingPolicyRegistry::MatchLocked(const Record& record,
                                                        std::string* error) const {
    if (record.broker_observed) return MatchBrokerObservedLocked(record, error);
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
        const auto* record = simnow_broker_observed_mode_
                                 ? BuildBrokerObservedRecordLocked(instrument, trading_day, error)
                                 : FindLocked(instrument, trading_day);
        if (!record) {
            if (error && !error->empty()) return false;
            return Fail(error, "no verified accounting policy for exact scope/day");
        }
        if (!MatchLocked(*record, error)) return false;
    }
    return true;
}

TradeAccountingPolicy VerifiedTradeAccountingPolicyRegistry::Resolve(const Trade& trade,
                                                                     std::string* error) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (error) error->clear();
    const AccountingPolicyInstrument scope{trade.symbol, trade.exchange, trade.hedge_flag};
    const auto* record = simnow_broker_observed_mode_
                             ? BuildBrokerObservedRecordLocked(scope, trade.trading_day, error)
                             : FindLocked(scope, trade.trading_day);
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
    const auto* record = FindHistoricalLocked(
        {trade.symbol, trade.exchange, trade.hedge_flag}, trade.trading_day);
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
    if (simnow_broker_observed_mode_) {
        auto receipts = historical_evidence_;
        std::map<std::string, Record> records_by_descriptor;
        std::map<std::string, std::string> descriptor_by_scope;
        const auto add_record = [&](const Record& record) {
            const auto scope = Scope(record.instrument, record.trading_day);
            const auto scoped = descriptor_by_scope.find(scope);
            if (scoped != descriptor_by_scope.end() && scoped->second != record.descriptor)
                return false;
            descriptor_by_scope[scope] = record.descriptor;
            records_by_descriptor[record.descriptor] = record;
            return true;
        };
        for (const auto& record : historical_broker_observed_records_)
            if (!add_record(record))
                return Fail(error, "conflicting persisted broker-observed policies for one scope");
        for (const auto& record : broker_observed_records_) {
            if (!MatchLocked(record, nullptr)) continue;
            if (!add_record(record))
                return Fail(error,
                            "broker-observed policy changed within the persisted trading day");
            const auto instrument = broker_observed_instruments_.find(
                record.instrument.instrument_id);
            const auto commission = broker_observed_commissions_.find(
                record.instrument.instrument_id);
            if (instrument == broker_observed_instruments_.end() ||
                commission == broker_observed_commissions_.end())
                return Fail(error, "broker-observed evidence disappeared during export");
            EvidenceState receipt = {
                {"instrument", QueryReceipt(instrument->second.metadata)},
                {"commission", QueryReceipt(commission->second.metadata)},
                {"order_fee", kAssumedZeroOrderFeeReceipt}};
            if (!record.order_fee_assumed_zero) {
                const auto order_fee = broker_observed_order_commissions_.find(
                    record.instrument.instrument_id);
                if (order_fee == broker_observed_order_commissions_.end())
                    return Fail(error, "verified order-fee evidence disappeared during export");
                receipt["order_fee"] = QueryReceipt(order_fee->second.metadata);
            }
            receipts[record.descriptor] = std::move(receipt);
        }
        for (const auto& receipt : receipts)
            if (!records_by_descriptor.count(receipt.first))
                return Fail(error, "persisted broker-observed receipt has no matching policy");

        state->clear();
        (*state)["schema_version"] = "3";
        (*state)["environment"] = identity_.environment;
        (*state)["broker_id"] = identity_.broker_id;
        (*state)["account_id"] = identity_.account_id;
        (*state)["count"] = std::to_string(receipts.size());
        std::size_t index = 0;
        const auto number = [](double value) {
            std::ostringstream out;
            out << std::setprecision(17) << value;
            return out.str();
        };
        for (const auto& receipt : receipts) {
            const auto prefix = "record." + std::to_string(index++) + ".";
            const auto& record = records_by_descriptor.at(receipt.first);
            (*state)[prefix + "policy"] = record.descriptor;
            (*state)[prefix + "instrument"] = receipt.second.at("instrument");
            (*state)[prefix + "commission"] = receipt.second.at("commission");
            (*state)[prefix + "order_fee"] = receipt.second.at("order_fee");
            (*state)[prefix + "instrument_id"] = record.instrument.instrument_id;
            (*state)[prefix + "exchange_id"] = record.instrument.exchange_id;
            (*state)[prefix + "hedge_flag"] =
                std::to_string(static_cast<int>(record.instrument.hedge_flag));
            (*state)[prefix + "trading_day"] = record.trading_day;
            (*state)[prefix + "contract_multiplier"] = number(record.multiplier);
            (*state)[prefix + "open_by_money"] = number(record.policy.open_fee.by_money);
            (*state)[prefix + "open_by_volume"] = number(record.policy.open_fee.by_volume);
            (*state)[prefix + "close_by_money"] = number(record.policy.close_fee.by_money);
            (*state)[prefix + "close_by_volume"] = number(record.policy.close_fee.by_volume);
            (*state)[prefix + "close_today_by_money"] =
                number(record.policy.close_today_fee.by_money);
            (*state)[prefix + "close_today_by_volume"] =
                number(record.policy.close_today_fee.by_volume);
            (*state)[prefix + "order_fee_basis"] = record.order_fee_assumed_zero
                                                        ? kAssumedZeroOrderFees
                                                        : kVerifiedZeroOrderFees;
            (*state)[prefix + "generic_close_priority"] =
                std::to_string(static_cast<int>(record.policy.generic_close_priority));
            (*state)[prefix + "close_rule_source"] = record.policy.close_rule_source;
            (*state)[prefix + "close_rule_version"] = record.policy.close_rule_version;
            (*state)[prefix + "close_convention_evidence_sha256"] =
                record.close_convention_evidence_sha256;
        }
        return true;
    }
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
    historical_broker_observed_records_.clear();
    try {
        if (!loaded_ ||
            state.at("environment") != identity_.environment ||
            state.at("broker_id") != identity_.broker_id ||
            state.at("account_id") != identity_.account_id)
            throw std::runtime_error("policy evidence identity/schema mismatch");
        const auto& schema_version = state.at("schema_version");
        if (schema_version == "2" || schema_version == "3") {
            if (!simnow_broker_observed_mode_ || identity_.environment != "simnow")
                throw std::runtime_error(
                    "broker-observed evidence can only restore in SimNow broker-observed mode");
            const bool legacy = schema_version == "2";
            std::size_t consumed = 0;
            const auto count = std::stoull(state.at("count"), &consumed);
            const std::size_t fields_per_record = legacy ? 16 : 20;
            if (count > 512 || consumed != state.at("count").size() ||
                state.size() != 5 + count * fields_per_record)
                throw std::runtime_error("invalid broker-observed evidence count/fields");
            std::map<std::string, EvidenceState> restored;
            std::set<std::string> scopes;
            for (std::size_t i = 0; i < count; ++i) {
                const auto prefix = "record." + std::to_string(i) + ".";
                Record record;
                record.instrument.instrument_id = state.at(prefix + "instrument_id");
                record.instrument.exchange_id = state.at(prefix + "exchange_id");
                record.instrument.hedge_flag = static_cast<HedgeFlag>(
                    ParseInteger(state.at(prefix + "hedge_flag"), "hedge_flag"));
                record.trading_day = state.at(prefix + "trading_day");
                record.multiplier = ParseFiniteNonnegative(
                    state.at(prefix + "contract_multiplier"), "contract_multiplier");
                if (record.instrument.instrument_id.empty() ||
                    (legacy ? !ExplicitCloseExchange(record.instrument.exchange_id)
                            : !SupportedBrokerObservedExchange(record.instrument.exchange_id)) ||
                    !ValidDay(record.trading_day) || record.multiplier <= 0 ||
                    record.multiplier != std::floor(record.multiplier))
                    throw std::runtime_error("invalid broker-observed policy scope");
                record.commission_instrument_id = record.instrument.instrument_id;
                record.broker_observed = true;
                auto& policy = record.policy;
                policy.valuation_inputs_verified = true;
                policy.contract_multiplier = record.multiplier;
                policy.fee_model = TradeFeeModel::kMoneyPlusVolumeV1;
                policy.open_fee = {
                    ParseFiniteNonnegative(state.at(prefix + "open_by_money"), "open_by_money"),
                    ParseFiniteNonnegative(state.at(prefix + "open_by_volume"),
                                           "open_by_volume")};
                policy.close_fee = {
                    ParseFiniteNonnegative(state.at(prefix + "close_by_money"),
                                           "close_by_money"),
                    ParseFiniteNonnegative(state.at(prefix + "close_by_volume"),
                                           "close_by_volume")};
                policy.close_today_fee = {
                    ParseFiniteNonnegative(state.at(prefix + "close_today_by_money"),
                                           "close_today_by_money"),
                    ParseFiniteNonnegative(state.at(prefix + "close_today_by_volume"),
                                           "close_today_by_volume")};
                const auto& order_fee_basis = state.at(prefix + "order_fee_basis");
                record.order_fee_assumed_zero = order_fee_basis == kAssumedZeroOrderFees;
                if (order_fee_basis != kVerifiedZeroOrderFees &&
                    order_fee_basis != kAssumedZeroOrderFees)
                    throw std::runtime_error("invalid broker-observed order-fee basis");
                if (record.order_fee_assumed_zero &&
                    !allow_simnow_assumed_zero_order_fees_)
                    throw std::runtime_error(
                        "assumed-zero order-fee evidence requires the explicit SimNow flag");
                policy.fee_date_basis = "close_allocation_v1";
                policy.fee_allocation_version = kBrokerObservedPolicyVersion;
                if (legacy) {
                    policy.fee_allocation_source =
                        "simnow_ctp_explicit_offset_and_commission_query";
                    policy.close_rule_source = "ctp_explicit_offset";
                    policy.close_rule_version = kBrokerObservedPolicyVersion;
                    const auto legacy_descriptor = LegacyBrokerObservedDescriptor(
                        record.instrument, record.trading_day, record.multiplier, policy,
                        record.order_fee_assumed_zero);
                    if (state.at(prefix + "policy") != legacy_descriptor)
                        throw std::runtime_error("broker-observed policy descriptor mismatch");
                } else {
                    policy.generic_close_priority = static_cast<GenericClosePriority>(
                        ParseInteger(state.at(prefix + "generic_close_priority"),
                                     "generic_close_priority"));
                    policy.close_rule_source = state.at(prefix + "close_rule_source");
                    policy.close_rule_version = state.at(prefix + "close_rule_version");
                    record.close_convention_evidence_sha256 =
                        state.at(prefix + "close_convention_evidence_sha256");
                    if (ExplicitCloseExchange(record.instrument.exchange_id)) {
                        if (policy.generic_close_priority != GenericClosePriority::kUnspecified ||
                            policy.close_rule_source != "ctp_explicit_offset" ||
                            policy.close_rule_version != kBrokerObservedPolicyVersion ||
                            !record.close_convention_evidence_sha256.empty())
                            throw std::runtime_error(
                                "invalid explicit-offset exchange close convention");
                        policy.fee_allocation_source =
                            "simnow_ctp_explicit_offset_and_commission_query";
                    } else {
                        if (policy.generic_close_priority ==
                            GenericClosePriority::kUnspecified)
                            throw std::runtime_error(
                                "generic Close evidence requires an explicit priority");
                        const auto configured = simnow_generic_close_conventions_.find(
                            record.instrument.exchange_id);
                        if (configured == simnow_generic_close_conventions_.end() ||
                            configured->second.priority != policy.generic_close_priority ||
                            configured->second.evidence_source != policy.close_rule_source ||
                            configured->second.evidence_version != policy.close_rule_version ||
                            configured->second.evidence_sha256 !=
                                record.close_convention_evidence_sha256 ||
                            !ValidSha256(record.close_convention_evidence_sha256))
                            throw std::runtime_error(
                                "persisted generic-close convention differs from current evidence");
                        policy.fee_allocation_source =
                            "simnow_ctp_generic_close_convention_and_commission_query";
                    }
                }
                policy.valuation_source =
                    std::string(kBrokerObservedPolicyVersion) +
                    ";instrument=ctp_same_session_exact;commission=ctp_same_session_exact" +
                    ";fee_exchange=exact_instrument_query_anchor" +
                    ";order_fee=" + order_fee_basis +
                    (ExplicitCloseExchange(record.instrument.exchange_id)
                         ? ";close_convention=ctp_explicit_offset"
                         : ";close_convention=" + policy.close_rule_source + "@" +
                               policy.close_rule_version + ";close_convention_sha256=" +
                               record.close_convention_evidence_sha256);
                record.descriptor = BrokerObservedDescriptor(
                    record.instrument, record.trading_day, record.multiplier, policy,
                    record.order_fee_assumed_zero, record.close_convention_evidence_sha256);
                if (!legacy && state.at(prefix + "policy") != record.descriptor)
                    throw std::runtime_error("broker-observed policy descriptor mismatch");
                const auto instrument_receipt = state.at(prefix + "instrument");
                const auto commission_receipt = state.at(prefix + "commission");
                const auto order_fee_receipt = state.at(prefix + "order_fee");
                if (!ValidQueryReceipt(instrument_receipt) ||
                    !ValidQueryReceipt(commission_receipt) ||
                    (record.order_fee_assumed_zero
                         ? order_fee_receipt != kAssumedZeroOrderFeeReceipt
                         : !ValidQueryReceipt(order_fee_receipt)))
                    throw std::runtime_error(
                        "invalid persisted broker-observed query verification receipt");
                if (!scopes.insert(Scope(record.instrument, record.trading_day)).second)
                    throw std::runtime_error("duplicate broker-observed policy scope");
                EvidenceState receipt = {{"instrument", instrument_receipt},
                                         {"commission", commission_receipt},
                                         {"order_fee", order_fee_receipt}};
                if (!restored.emplace(record.descriptor, std::move(receipt)).second)
                    throw std::runtime_error("duplicate persisted broker-observed policy");
                historical_broker_observed_records_.push_back(std::move(record));
            }
            historical_evidence_ = std::move(restored);
            return true;
        }
        if (schema_version != "1")
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
