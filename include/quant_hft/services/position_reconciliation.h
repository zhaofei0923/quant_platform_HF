#pragma once

#include <algorithm>
#include <cctype>
#include <map>
#include <string>
#include <vector>

#include "quant_hft/contracts/query_result.h"
#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct PositionReconciliationResult {
    bool matched{false};
    std::vector<std::string> differences;
    std::string error;
};

// A broker query is evidence to compare with the committed domain, never an instruction to
// replace attributed positions. Compare gross holdings so offsetting long/short mismatches
// cannot disappear in a net position check.
inline PositionReconciliationResult ReconcileBrokerPositions(
    const std::string& account, const QueryResult<InvestorPositionSnapshot>& query,
    const std::vector<Position>& domain) {
    PositionReconciliationResult result;
    if (!query.metadata.success || !query.metadata.complete || !query.metadata.full_account) {
        result.error = "successful complete full-account query required";
        return result;
    }
    using Buckets = std::map<std::string, std::int64_t>;
    Buckets broker;
    Buckets local;
    const auto lower = [](std::string value) {
        for (auto& ch : value) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        return value;
    };
    const auto key = [&](const std::string& instrument, const std::string& exchange,
                         HedgeFlag hedge, bool is_long, bool today) {
        return std::to_string(instrument.size()) + ":" + instrument + ":" + lower(exchange) + ":" +
               std::to_string(static_cast<int>(hedge)) + ":" + (is_long ? "long" : "short") + ":" +
               (today ? "today" : "yesterday");
    };
    for (const auto& row : query.rows) {
        const auto direction = lower(row.posi_direction);
        const auto hedge_text = lower(row.hedge_flag);
        const auto date = lower(row.position_date);
        if (row.account_id != account || row.instrument_id.empty() || row.exchange_id.empty() ||
            row.position < 0 || row.today_position < 0 ||
            (direction != "2" && direction != "3" && direction != "long" && direction != "short") ||
            (hedge_text != "1" && hedge_text != "2" && hedge_text != "3" &&
             hedge_text != "speculation" && hedge_text != "arbitrage" && hedge_text != "hedge")) {
            result.error = "invalid broker position identity or quantity";
            return result;
        }
        const bool is_long = direction == "2" || direction == "long";
        const HedgeFlag hedge =
            hedge_text == "2" || hedge_text == "arbitrage" ? HedgeFlag::kArbitrage
            : hedge_text == "3" || hedge_text == "hedge"   ? HedgeFlag::kHedge
                                                           : HedgeFlag::kSpeculation;
        if (lower(row.exchange_id) == "shfe" || lower(row.exchange_id) == "ine") {
            if (date != "1" && date != "2" && date != "today" && date != "yesterday") {
                result.error = "missing explicit SHFE/INE position date";
                return result;
            }
            broker[key(row.instrument_id, row.exchange_id, hedge, is_long,
                       date == "1" || date == "today")] += row.position;
        } else {
            if (row.today_position > row.position) {
                result.error = "broker TodayPosition exceeds Position";
                return result;
            }
            broker[key(row.instrument_id, row.exchange_id, hedge, is_long, true)] +=
                row.today_position;
            broker[key(row.instrument_id, row.exchange_id, hedge, is_long, false)] +=
                row.position - row.today_position;
        }
    }
    for (const auto& position : domain) {
        if (position.account_id != account || position.strategy_id.empty() ||
            position.long_qty != position.long_today_qty + position.long_yd_qty ||
            position.short_qty != position.short_today_qty + position.short_yd_qty) {
            result.error = "invalid or unattributed domain position";
            return result;
        }
        local[key(position.symbol, position.exchange, position.hedge_flag, true, true)] +=
            position.long_today_qty;
        local[key(position.symbol, position.exchange, position.hedge_flag, true, false)] +=
            position.long_yd_qty;
        local[key(position.symbol, position.exchange, position.hedge_flag, false, true)] +=
            position.short_today_qty;
        local[key(position.symbol, position.exchange, position.hedge_flag, false, false)] +=
            position.short_yd_qty;
    }
    for (const auto& item : broker) local.emplace(item.first, 0);
    for (const auto& item : local) {
        if (broker[item.first] != item.second) {
            result.differences.push_back(item.first + ":domain=" + std::to_string(item.second) +
                                         ",broker=" + std::to_string(broker[item.first]));
        }
    }
    result.matched = result.differences.empty();
    if (!result.matched)
        result.error = "broker/domain position mismatch; baseline or reconciliation required";
    return result;
}

}  // namespace quant_hft
