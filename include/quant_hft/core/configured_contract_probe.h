#pragma once

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <limits>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "quant_hft/contracts/query_result.h"
#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct ConfiguredContractProbeContext {
    std::string instrument_id;
    std::string product_id;
    std::string account_id;
    std::string investor_id;
    std::string trading_day;
    std::string source{"ctp"};
    std::string hedge_flag{"1"};
    std::string expected_exchange_id;
};

struct ConfiguredProbeProductScope {
    std::string product_id;
    std::string exchange_id;
};

struct ConfiguredContractProbeEvidence {
    std::string instrument_id;
    std::string product_id;
    std::string exchange_id;
    std::string commission_scope;
    std::string order_comm_status;
    std::size_t metadata_rows{0};
    std::size_t commission_rows{0};
    std::size_t order_comm_rows{0};
};

class ConfiguredUniverseProbeHealthLatch {
   public:
    void Observe(bool healthy) { all_healthy_ = all_healthy_ && healthy; }
    bool all_healthy() const noexcept { return all_healthy_; }

   private:
    bool all_healthy_{true};
};

namespace configured_contract_probe_detail {

inline bool Fail(std::string* error, const std::string& reason) {
    if (error != nullptr) {
        *error = reason;
    }
    return false;
}

inline std::string Trim(std::string value) {
    const auto first = std::find_if_not(value.begin(), value.end(), [](unsigned char ch) {
        return std::isspace(ch) != 0;
    });
    const auto last = std::find_if_not(value.rbegin(), value.rend(), [](unsigned char ch) {
                          return std::isspace(ch) != 0;
                      }).base();
    if (first >= last) {
        return {};
    }
    return std::string(first, last);
}

inline std::string LowerAscii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

inline bool FiniteNonnegative(double value) {
    return std::isfinite(value) && value >= 0.0;
}

inline bool IsSupportedCommodityExchange(const std::string& exchange_id) {
    return exchange_id == "SHFE" || exchange_id == "INE" || exchange_id == "DCE" ||
           exchange_id == "CZCE" || exchange_id == "GFEX";
}

inline bool ValidateEnvelope(const QueryResultMetadata& metadata, int request_id,
                             const std::string& query_name,
                             const ConfiguredContractProbeContext& context,
                             std::string* error) {
    if (!metadata.complete) {
        return Fail(error, query_name + " response is incomplete");
    }
    if (!metadata.success || metadata.error_code != 0 || !metadata.error.empty()) {
        return Fail(error, query_name + " response reports an error");
    }
    if (request_id <= 0 || metadata.request_id != request_id || metadata.generation == 0 ||
        metadata.query_name != query_name || metadata.instrument_id != context.instrument_id ||
        metadata.full_account) {
        return Fail(error, query_name + " request scope does not match the configured contract");
    }
    if (context.account_id.empty() || metadata.account_id != context.account_id ||
        metadata.trading_day != context.trading_day || metadata.source != context.source) {
        return Fail(error, query_name + " response identity does not match this session");
    }
    return true;
}

inline bool ValidateIdentity(const std::string& account_id, const std::string& investor_id,
                             const std::string& source,
                             const ConfiguredContractProbeContext& context) {
    return account_id == context.account_id && investor_id == context.investor_id &&
           source == context.source;
}

// Some CTP fronts omit ExchangeID from request-scoped fee rows.  The caller may bind an omitted
// value only after the exact-contract Instrument response for the same query generation has been
// validated.  A populated value remains authoritative and must agree with that response.
inline bool FeeRowExchangeMatchesInstrument(const std::string& fee_exchange_id,
                                             const std::string& instrument_exchange_id) {
    return !instrument_exchange_id.empty() &&
           (fee_exchange_id.empty() || fee_exchange_id == instrument_exchange_id);
}

}  // namespace configured_contract_probe_detail

inline std::string ConfiguredContractProductId(const std::string& instrument_id) {
    std::string symbol = configured_contract_probe_detail::Trim(instrument_id);
    const auto dot = symbol.find('.');
    if (dot != std::string::npos) {
        symbol = symbol.substr(dot + 1);
    }
    std::string product_id;
    for (unsigned char ch : symbol) {
        if (std::isalpha(ch) == 0) {
            break;
        }
        product_id.push_back(static_cast<char>(std::tolower(ch)));
    }
    return product_id;
}

// CTP_SIM_INSTRUMENTS is authoritative when present.  CTP_SIM_INSTRUMENT remains a
// backward-compatible single-contract fallback, followed by the YAML list.
inline bool ResolveConfiguredProbeInstruments(const std::string& instruments_env,
                                              const std::string& legacy_instrument_env,
                                              const std::vector<std::string>& yaml_instruments,
                                              const std::string& fallback_instrument,
                                              std::vector<std::string>* instruments,
                                              std::string* error) {
    if (instruments == nullptr) {
        return configured_contract_probe_detail::Fail(error, "instrument output is null");
    }
    instruments->clear();

    std::vector<std::string> candidates;
    if (!configured_contract_probe_detail::Trim(instruments_env).empty()) {
        std::size_t begin = 0;
        while (begin <= instruments_env.size()) {
            const auto comma = instruments_env.find(',', begin);
            const auto length = comma == std::string::npos ? std::string::npos : comma - begin;
            candidates.push_back(instruments_env.substr(begin, length));
            if (comma == std::string::npos) {
                break;
            }
            begin = comma + 1;
        }
    } else if (!configured_contract_probe_detail::Trim(legacy_instrument_env).empty()) {
        candidates.push_back(legacy_instrument_env);
    } else if (!yaml_instruments.empty()) {
        candidates = yaml_instruments;
    } else {
        candidates.push_back(fallback_instrument);
    }

    std::unordered_set<std::string> seen;
    for (auto candidate : candidates) {
        candidate = configured_contract_probe_detail::Trim(std::move(candidate));
        if (candidate.empty()) {
            continue;
        }
        if (ConfiguredContractProductId(candidate).empty()) {
            return configured_contract_probe_detail::Fail(
                error, "configured instrument has no alphabetic product prefix");
        }
        if (!seen.insert(candidate).second) {
            return configured_contract_probe_detail::Fail(error,
                                                          "duplicate configured instrument");
        }
        instruments->push_back(std::move(candidate));
    }
    if (instruments->empty()) {
        return configured_contract_probe_detail::Fail(error,
                                                      "configured instrument list is empty");
    }
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

inline bool ResolveConfiguredProbeProductScopes(
    const std::string& product_scope_env, const std::string& expected_contract_count_env,
    const std::vector<std::string>& configured_instruments,
    std::vector<ConfiguredProbeProductScope>* product_scopes, std::string* error) {
    using configured_contract_probe_detail::Fail;
    if (product_scopes == nullptr) {
        return Fail(error, "product scope output is null");
    }
    product_scopes->clear();

    const auto expected_text =
        configured_contract_probe_detail::Trim(expected_contract_count_env);
    if (!expected_text.empty()) {
        std::size_t expected_count = 0;
        for (unsigned char ch : expected_text) {
            if (std::isdigit(ch) == 0) {
                return Fail(error, "expected contract count must be a positive integer");
            }
            const auto digit = static_cast<std::size_t>(ch - '0');
            if (expected_count >
                (std::numeric_limits<std::size_t>::max() - digit) / 10U) {
                return Fail(error, "expected contract count is too large");
            }
            expected_count = expected_count * 10U + digit;
        }
        if (expected_count == 0U || expected_count != configured_instruments.size()) {
            return Fail(error, "expected contract count does not match configured instruments");
        }
        if (configured_contract_probe_detail::Trim(product_scope_env).empty()) {
            return Fail(error, "controlled configured universe requires SIMNOW_PRODUCT_SCOPE");
        }
    }

    if (configured_contract_probe_detail::Trim(product_scope_env).empty()) {
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }

    std::unordered_set<std::string> configured_products;
    for (const auto& instrument_id : configured_instruments) {
        const auto product_id = ConfiguredContractProductId(instrument_id);
        if (product_id.empty()) {
            return Fail(error, "configured instrument has no product for scope validation");
        }
        configured_products.insert(configured_contract_probe_detail::LowerAscii(product_id));
    }

    std::unordered_set<std::string> seen_products;
    std::size_t begin = 0;
    while (begin <= product_scope_env.size()) {
        const auto comma = product_scope_env.find(',', begin);
        const auto length = comma == std::string::npos ? std::string::npos : comma - begin;
        const auto member =
            configured_contract_probe_detail::Trim(product_scope_env.substr(begin, length));
        if (member.empty()) {
            return Fail(error, "product scope contains an empty member");
        }
        const auto colon = member.find(':');
        if (colon == std::string::npos || member.find(':', colon + 1) != std::string::npos) {
            return Fail(error, "product scope member must use product:EXCHANGE");
        }
        auto product_id = configured_contract_probe_detail::Trim(member.substr(0, colon));
        auto exchange_id = configured_contract_probe_detail::Trim(member.substr(colon + 1));
        if (product_id.empty() || std::isalpha(static_cast<unsigned char>(product_id.front())) == 0 ||
            !std::all_of(product_id.begin(), product_id.end(), [](unsigned char ch) {
                return std::isalnum(ch) != 0;
            })) {
            return Fail(error, "product scope contains an invalid product");
        }
        if (!configured_contract_probe_detail::IsSupportedCommodityExchange(exchange_id)) {
            return Fail(error, "product scope contains an unsupported exchange");
        }
        product_id = configured_contract_probe_detail::LowerAscii(std::move(product_id));
        if (!seen_products.insert(product_id).second) {
            return Fail(error, "product scope contains a duplicate product");
        }
        if (configured_products.find(product_id) == configured_products.end()) {
            return Fail(error, "product scope contains a product outside configured instruments");
        }
        product_scopes->push_back({std::move(product_id), std::move(exchange_id)});
        if (comma == std::string::npos) {
            break;
        }
        begin = comma + 1;
    }

    if (seen_products.size() != configured_products.size()) {
        return Fail(error, "product scope does not cover every configured instrument");
    }
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

inline std::string ExpectedExchangeForConfiguredProduct(
    const std::vector<ConfiguredProbeProductScope>& product_scopes,
    const std::string& product_id) {
    const auto normalized = configured_contract_probe_detail::LowerAscii(product_id);
    const auto match = std::find_if(product_scopes.begin(), product_scopes.end(),
                                    [&](const auto& scope) {
                                        return scope.product_id == normalized;
                                    });
    return match == product_scopes.end() ? std::string{} : match->exchange_id;
}

inline bool ValidateConfiguredContractProbeEvidence(
    const ConfiguredContractProbeContext& context, int metadata_request_id,
    const QueryResult<InstrumentMetaSnapshot>& metadata_result, int commission_request_id,
    const QueryResult<InstrumentCommissionRateSnapshot>& commission_result,
    int order_comm_request_id,
    const QueryResult<InstrumentOrderCommRateSnapshot>& order_comm_result,
    ConfiguredContractProbeEvidence* evidence, std::string* error) {
    using configured_contract_probe_detail::Fail;
    if (evidence == nullptr) {
        return Fail(error, "probe evidence output is null");
    }
    *evidence = {};
    if (context.instrument_id.empty() || context.product_id.empty() ||
        context.investor_id.empty() || context.trading_day.size() != 8U) {
        return Fail(error, "configured contract probe context is incomplete");
    }
    if (!configured_contract_probe_detail::ValidateEnvelope(
            metadata_result.metadata, metadata_request_id, "Instrument", context, error) ||
        !configured_contract_probe_detail::ValidateEnvelope(
            commission_result.metadata, commission_request_id, "InstrumentCommissionRate",
            context, error) ||
        !configured_contract_probe_detail::ValidateEnvelope(
            order_comm_result.metadata, order_comm_request_id, "InstrumentOrderCommRate", context,
            error)) {
        return false;
    }
    if (metadata_result.metadata.generation != commission_result.metadata.generation ||
        metadata_result.metadata.generation != order_comm_result.metadata.generation) {
        return Fail(error, "configured contract queries span different sessions");
    }

    if (metadata_result.rows.size() != 1U) {
        return Fail(error, "exactly one instrument metadata row is required");
    }
    const auto& metadata = metadata_result.rows.front();
    if (metadata.instrument_id != context.instrument_id || metadata.exchange_id.empty() ||
        (!context.expected_exchange_id.empty() &&
         metadata.exchange_id != context.expected_exchange_id) ||
        configured_contract_probe_detail::LowerAscii(metadata.product_id) !=
            configured_contract_probe_detail::LowerAscii(context.product_id) ||
        metadata.volume_multiple <= 0 || !std::isfinite(metadata.price_tick) ||
        metadata.price_tick <= 0.0 || !metadata.is_trading || metadata.product_class != "1" ||
        metadata.ts_ns <= 0 || metadata.source != context.source) {
        return Fail(error, "instrument metadata does not identify a tradable configured future");
    }

    if (commission_result.rows.size() != 1U) {
        return Fail(error, "exactly one commission row is required");
    }
    const auto& commission = commission_result.rows.front();
    const bool exact_commission = commission.instrument_id == context.instrument_id;
    const bool product_commission =
        configured_contract_probe_detail::LowerAscii(commission.instrument_id) ==
        configured_contract_probe_detail::LowerAscii(context.product_id);
    if ((!exact_commission && !product_commission) ||
        !configured_contract_probe_detail::ValidateIdentity(
            commission.account_id, commission.investor_id, commission.source, context) ||
        !configured_contract_probe_detail::FeeRowExchangeMatchesInstrument(
            commission.exchange_id, metadata.exchange_id) ||
        commission.ts_ns <= 0 ||
        !configured_contract_probe_detail::FiniteNonnegative(commission.open_ratio_by_money) ||
        !configured_contract_probe_detail::FiniteNonnegative(commission.open_ratio_by_volume) ||
        !configured_contract_probe_detail::FiniteNonnegative(commission.close_ratio_by_money) ||
        !configured_contract_probe_detail::FiniteNonnegative(commission.close_ratio_by_volume) ||
        !configured_contract_probe_detail::FiniteNonnegative(
            commission.close_today_ratio_by_money) ||
        !configured_contract_probe_detail::FiniteNonnegative(
            commission.close_today_ratio_by_volume)) {
        return Fail(error, "commission row does not match the configured contract identity");
    }

    std::string order_comm_status = "successful_empty";
    if (order_comm_result.rows.size() > 1U) {
        return Fail(error, "at most one order commission row is allowed");
    }
    if (!order_comm_result.rows.empty()) {
        const auto& order_comm = order_comm_result.rows.front();
        if (!configured_contract_probe_detail::ValidateIdentity(
                order_comm.account_id, order_comm.investor_id, order_comm.source, context) ||
            order_comm.instrument_id != context.instrument_id ||
            !configured_contract_probe_detail::FeeRowExchangeMatchesInstrument(
                order_comm.exchange_id, metadata.exchange_id) ||
            order_comm.hedge_flag != context.hedge_flag || order_comm.ts_ns <= 0 ||
            !configured_contract_probe_detail::FiniteNonnegative(
                order_comm.order_comm_by_volume) ||
            !configured_contract_probe_detail::FiniteNonnegative(
                order_comm.order_action_comm_by_volume)) {
            return Fail(error, "order commission row does not match the configured contract identity");
        }
        order_comm_status = "verified";
    }

    evidence->instrument_id = context.instrument_id;
    evidence->product_id = context.product_id;
    evidence->exchange_id = metadata.exchange_id;
    evidence->commission_scope = exact_commission ? "contract" : "product";
    evidence->order_comm_status = std::move(order_comm_status);
    evidence->metadata_rows = metadata_result.rows.size();
    evidence->commission_rows = commission_result.rows.size();
    evidence->order_comm_rows = order_comm_result.rows.size();
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

inline bool ConfiguredUniverseProbeReady(
    const std::vector<std::string>& configured_instruments,
    const std::vector<ConfiguredContractProbeEvidence>& evidence, std::string* error) {
    using configured_contract_probe_detail::Fail;
    if (configured_instruments.empty()) {
        return Fail(error, "configured universe is empty");
    }
    if (configured_instruments.size() != evidence.size()) {
        return Fail(error, "not every configured contract has validated evidence");
    }
    std::unordered_set<std::string> observed;
    for (const auto& item : evidence) {
        if (item.instrument_id.empty() || item.exchange_id.empty() || item.product_id.empty() ||
            item.metadata_rows != 1U || item.commission_rows != 1U ||
            (item.order_comm_status != "verified" &&
             item.order_comm_status != "successful_empty") ||
            !observed.insert(item.instrument_id).second) {
            return Fail(error, "configured universe evidence is incomplete or duplicated");
        }
    }
    for (const auto& instrument_id : configured_instruments) {
        if (observed.find(instrument_id) == observed.end()) {
            return Fail(error, "configured universe evidence is missing a contract");
        }
    }
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

}  // namespace quant_hft
