#include "quant_hft/services/ctp_position_ledger.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <sstream>

namespace quant_hft {

namespace {

std::string LowerAscii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

bool IsCancelActionFeedback(const OrderEvent& event) {
    return event.event_source == "OnRspOrderAction" || event.event_source == "OnErrRtnOrderAction";
}

bool IsKnownPositionDirection(const std::string& raw) {
    const auto normalized = LowerAscii(raw);
    return normalized == "2" || normalized == "3" || normalized == "long" ||
           normalized == "short" || normalized == "l" || normalized == "s";
}

std::string DirectionToText(PositionDirection direction) {
    return direction == PositionDirection::kLong ? "long" : "short";
}

std::string OffsetToText(OffsetFlag offset) {
    switch (offset) {
        case OffsetFlag::kOpen:
            return "open";
        case OffsetFlag::kCloseToday:
            return "close_today";
        case OffsetFlag::kCloseYesterday:
            return "close_yesterday";
        case OffsetFlag::kClose:
        default:
            return "close";
    }
}

}  // namespace

std::size_t CtpPositionLedger::PositionKeyHasher::operator()(const PositionKey& key) const {
    const auto h1 = std::hash<std::string>{}(key.account_id);
    const auto h2 = std::hash<std::string>{}(key.instrument_id);
    const auto h3 = std::hash<std::string>{}(key.exchange_id);
    const auto h4 = std::hash<std::string>{}(key.hedge_flag);
    const auto h5 = std::hash<int>{}(static_cast<int>(key.direction));
    const auto h6 = std::hash<std::string>{}(key.position_date);
    return h1 ^ (h2 << 1) ^ (h3 << 2) ^ (h4 << 3) ^ (h5 << 4) ^ (h6 << 5);
}

bool CtpPositionLedger::ApplyInvestorPositionSnapshot(const InvestorPositionSnapshot& snapshot,
                                                      std::string* error) {
    std::vector<std::pair<PositionKey, PositionBucket>> buckets;
    if (!BuildSnapshotBuckets(snapshot, &buckets, error)) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [key, bucket] : buckets) positions_[key] = bucket;
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

bool CtpPositionLedger::BuildSnapshotBuckets(
    const InvestorPositionSnapshot& snapshot,
    std::vector<std::pair<PositionKey, PositionBucket>>* out, std::string* error) {
    if (snapshot.account_id.empty() || snapshot.instrument_id.empty() ||
        !IsKnownPositionDirection(snapshot.posi_direction) || snapshot.position < 0) {
        if (error != nullptr) *error = "invalid broker position snapshot identity/quantity";
        return false;
    }
    const auto direction = ParsePositionDirection(snapshot.posi_direction);
    const auto exchange = NormalizeExchangeId(snapshot.exchange_id, snapshot.instrument_id);
    const auto date = NormalizePositionDate(snapshot.position_date);
    // CTP LongFrozen describes pending buys; ShortFrozen describes pending sells.
    // Long positions are closed by sells; short positions by buys. An unrelated
    // opposite field must not be substituted when the relevant field is zero.
    const int frozen = ClampNonNegative(
        direction == PositionDirection::kLong ? snapshot.short_frozen : snapshot.long_frozen);
    const auto add = [&](const std::string& bucket_date, int volume) {
        out->push_back({MakeKey(snapshot.account_id, snapshot.instrument_id, exchange,
                                snapshot.hedge_flag, direction, bucket_date),
                        {volume, std::min(volume, frozen), snapshot.ts_ns}});
    };
    if (exchange != "SHFE" && exchange != "INE" && date == "today") {
        if (snapshot.today_position < 0 || snapshot.today_position > snapshot.position) {
            if (error != nullptr) *error = "TodayPosition exceeds current aggregate Position";
            return false;
        }
        // YdPosition is the beginning-of-day quantity, not necessarily the remaining
        // yesterday inventory. Total frozen has no reliable per-day attribution;
        // conservatively subtract it from each dated bucket when admitting closes.
        add("today", snapshot.today_position);
        add("yesterday", snapshot.position - snapshot.today_position);
    } else {
        add(date, snapshot.position);
    }
    return true;
}

bool CtpPositionLedger::ReplaceInvestorPositionSnapshotBatch(
    const std::string& account_id, const std::vector<InvestorPositionSnapshot>& snapshots,
    std::string* error) {
    if (account_id.empty()) {
        if (error != nullptr) {
            *error = "account_id is required for broker position replacement";
        }
        return false;
    }

    std::unordered_map<PositionKey, PositionBucket, PositionKeyHasher> replacement;
    replacement.reserve(snapshots.size());
    for (const auto& snapshot : snapshots) {
        if (snapshot.account_id != account_id || snapshot.instrument_id.empty() ||
            !IsKnownPositionDirection(snapshot.posi_direction)) {
            if (error != nullptr) {
                *error = "broker position batch contains an invalid or foreign-account snapshot";
            }
            return false;
        }

        std::vector<std::pair<PositionKey, PositionBucket>> buckets;
        if (!BuildSnapshotBuckets(snapshot, &buckets, error)) return false;
        for (const auto& [key, bucket] : buckets) {
            if (!replacement.emplace(key, bucket).second) {
                if (error != nullptr) {
                    *error = "broker position batch contains a duplicate normalized position key";
                }
                return false;
            }
        }
    }

    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = positions_.begin(); it != positions_.end();) {
        if (it->first.account_id == account_id) {
            it = positions_.erase(it);
        } else {
            ++it;
        }
    }
    positions_.insert(replacement.begin(), replacement.end());
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

bool CtpPositionLedger::RegisterOrderIntent(const CtpOrderIntentForLedger& intent,
                                            std::string* error) {
    if (intent.client_order_id.empty() || intent.account_id.empty() ||
        intent.instrument_id.empty() || intent.requested_volume <= 0) {
        if (error != nullptr) {
            *error = "intent fields are invalid";
        }
        return false;
    }

    PendingOrderState pending;
    pending.intent = intent;
    pending.position_date = ResolvePositionDateForIntent(intent);

    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_orders_.find(intent.client_order_id) != pending_orders_.end()) {
        if (error != nullptr) {
            *error = "duplicate client_order_id";
        }
        return false;
    }

    if (IsCloseOffset(intent.offset)) {
        struct BucketDiagnostic {
            std::string position_date;
            std::int32_t position{0};
            std::int32_t frozen{0};
            std::int32_t closable{0};
        };

        std::vector<BucketDiagnostic> diagnostics;
        const auto position_dates = ResolvePositionDatesForIntent(intent);
        std::int32_t remaining = intent.requested_volume;
        std::int32_t total_closable = 0;
        for (const auto& position_date : position_dates) {
            const auto key = MakeKey(intent.account_id, intent.instrument_id, intent.exchange_id,
                                     intent.hedge_flag, intent.direction, position_date);
            BucketDiagnostic diagnostic;
            diagnostic.position_date = position_date;
            const auto bucket_it = positions_.find(key);
            if (bucket_it != positions_.end()) {
                diagnostic.position = bucket_it->second.position;
                diagnostic.frozen = bucket_it->second.frozen;
                diagnostic.closable = std::max(0, diagnostic.position - diagnostic.frozen);
            }
            diagnostics.push_back(diagnostic);
            total_closable += diagnostic.closable;
            if (remaining > 0 && diagnostic.closable > 0) {
                const auto freeze = std::min(remaining, diagnostic.closable);
                pending.close_allocations.push_back(CloseAllocation{position_date, freeze});
                remaining -= freeze;
            }
        }

        if (remaining > 0) {
            if (error != nullptr) {
                std::ostringstream message;
                message << "insufficient closable volume"
                        << " account_id=" << intent.account_id
                        << " instrument_id=" << intent.instrument_id << " exchange_id="
                        << NormalizeExchangeId(intent.exchange_id, intent.instrument_id)
                        << " hedge_flag=" << NormalizeHedgeFlag(intent.hedge_flag)
                        << " direction=" << DirectionToText(intent.direction)
                        << " offset=" << OffsetToText(intent.offset)
                        << " requested=" << intent.requested_volume
                        << " total_closable=" << total_closable;
                for (const auto& diagnostic : diagnostics) {
                    message << ' ' << diagnostic.position_date
                            << "_position=" << diagnostic.position << ' '
                            << diagnostic.position_date << "_frozen=" << diagnostic.frozen << ' '
                            << diagnostic.position_date << "_closable=" << diagnostic.closable;
                }
                *error = message.str();
            }
            return false;
        }

        const auto now_ns = NowEpochNanos();
        for (const auto& allocation : pending.close_allocations) {
            const auto key = MakeKey(intent.account_id, intent.instrument_id, intent.exchange_id,
                                     intent.hedge_flag, intent.direction, allocation.position_date);
            auto bucket_it = positions_.find(key);
            if (bucket_it != positions_.end()) {
                bucket_it->second.frozen += allocation.frozen_volume;
                bucket_it->second.last_update_ts_ns = now_ns;
            }
        }
    }

    pending_orders_.emplace(intent.client_order_id, std::move(pending));
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

bool CtpPositionLedger::ApplyOrderEvent(const OrderEvent& event, std::string* error) {
    if (IsCancelActionFeedback(event)) {
        if (error != nullptr) {
            error->clear();
        }
        return true;
    }
    if (event.client_order_id.empty()) {
        if (error != nullptr) {
            *error = "event.client_order_id is required";
        }
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto pending_it = pending_orders_.find(event.client_order_id);
    if (pending_it == pending_orders_.end()) {
        if (error != nullptr) {
            *error = "order intent not registered";
        }
        return false;
    }

    auto& pending = pending_it->second;
    if (event.filled_volume < pending.last_filled_volume) {
        if (error != nullptr) {
            *error = "filled_volume cannot decrease";
        }
        return false;
    }

    if (committed_trade_accounting_) {
        pending.last_filled_volume = event.filled_volume;
        if (IsTerminalStatus(event.status)) {
            pending.terminal = true;
            // Retain reservations for fills reported by the order channel but not
            // yet booked by the trade channel. A terminal report is not a fill.
            int retain = std::max(0, pending.last_filled_volume - pending.booked_trade_volume);
            if (IsCloseOffset(pending.intent.offset)) {
                for (auto& allocation : pending.close_allocations) {
                    const int kept = std::min(retain, allocation.frozen_volume);
                    const int released = allocation.frozen_volume - kept;
                    auto& bucket =
                        positions_[MakeKey(pending.intent.account_id, pending.intent.instrument_id,
                                           pending.intent.exchange_id, pending.intent.hedge_flag,
                                           pending.intent.direction, allocation.position_date)];
                    bucket.frozen = std::max(0, bucket.frozen - released);
                    allocation.frozen_volume = kept;
                    retain -= kept;
                }
            }
            if (pending.booked_trade_volume >= pending.last_filled_volume)
                pending_orders_.erase(pending_it);
        }
        if (error != nullptr) error->clear();
        return true;
    }

    const auto delta_filled = event.filled_volume - pending.last_filled_volume;
    if (delta_filled > 0) {
        if (IsCloseOffset(pending.intent.offset)) {
            auto remaining_delta = delta_filled;
            for (auto& allocation : pending.close_allocations) {
                if (remaining_delta <= 0) {
                    break;
                }
                const auto release = std::min(remaining_delta, allocation.frozen_volume);
                if (release <= 0) {
                    continue;
                }
                const auto key = MakeKey(pending.intent.account_id, pending.intent.instrument_id,
                                         pending.intent.exchange_id, pending.intent.hedge_flag,
                                         pending.intent.direction, allocation.position_date);
                auto& bucket = positions_[key];
                bucket.position = std::max(0, bucket.position - release);
                bucket.frozen = std::max(0, bucket.frozen - release);
                bucket.last_update_ts_ns = event.ts_ns;
                allocation.frozen_volume -= release;
                remaining_delta -= release;
            }
        } else {
            const auto key = MakeKey(pending.intent.account_id, pending.intent.instrument_id,
                                     pending.intent.exchange_id, pending.intent.hedge_flag,
                                     pending.intent.direction, pending.position_date);
            auto& bucket = positions_[key];
            bucket.position += delta_filled;
            bucket.last_update_ts_ns = event.ts_ns;
        }
    }
    pending.last_filled_volume = event.filled_volume;

    if (IsTerminalStatus(event.status)) {
        if (IsCloseOffset(pending.intent.offset)) {
            for (auto& allocation : pending.close_allocations) {
                if (allocation.frozen_volume <= 0) {
                    continue;
                }
                const auto key = MakeKey(pending.intent.account_id, pending.intent.instrument_id,
                                         pending.intent.exchange_id, pending.intent.hedge_flag,
                                         pending.intent.direction, allocation.position_date);
                auto& bucket = positions_[key];
                bucket.frozen = std::max(0, bucket.frozen - allocation.frozen_volume);
                bucket.last_update_ts_ns = event.ts_ns;
                allocation.frozen_volume = 0;
            }
        }
        pending_orders_.erase(pending_it);
    }

    if (error != nullptr) {
        error->clear();
    }
    return true;
}

void CtpPositionLedger::UseCommittedTradeAccounting(bool enabled) {
    std::lock_guard<std::mutex> lock(mutex_);
    committed_trade_accounting_ = enabled;
}

bool CtpPositionLedger::HasUnbookedFills(const std::string& account_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [id, pending] : pending_orders_) {
        (void)id;
        if (pending.intent.account_id == account_id &&
            pending.last_filled_volume > pending.booked_trade_volume)
            return true;
    }
    return false;
}

bool CtpPositionLedger::ApplyCommittedTrade(const std::string& identity, const Trade& trade,
                                            const quant_hft::CloseAllocation& allocation,
                                            std::string* error) {
    if (identity.empty() || trade.account_id.empty() || trade.symbol.empty() ||
        trade.quantity <= 0) {
        if (error != nullptr) *error = "committed trade identity and positive quantity required";
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (!committed_trade_accounting_) {
        if (error != nullptr) *error = "committed trade accounting mode not enabled";
        return false;
    }
    if (applied_trade_identities_.count(identity)) return true;
    const bool close = IsCloseOffset(trade.offset);
    const auto direction =
        (trade.side == Side::kBuy) != close ? PositionDirection::kLong : PositionDirection::kShort;
    const std::string hedge = trade.hedge_flag == HedgeFlag::kHedge       ? "3"
                              : trade.hedge_flag == HedgeFlag::kArbitrage ? "2"
                                                                          : "1";
    const auto today_key =
        MakeKey(trade.account_id, trade.symbol, trade.exchange, hedge, direction, "today");
    const auto yesterday_key =
        MakeKey(trade.account_id, trade.symbol, trade.exchange, hedge, direction, "yesterday");
    if (close) {
        const auto td = positions_.find(today_key);
        const auto yd = positions_.find(yesterday_key);
        const int today = td == positions_.end() ? 0 : td->second.position;
        const int yesterday = yd == positions_.end() ? 0 : yd->second.position;
        if (allocation.today < 0 || allocation.yesterday < 0 ||
            allocation.today + allocation.yesterday != trade.quantity || allocation.today > today ||
            allocation.yesterday > yesterday) {
            if (error != nullptr)
                *error = "broker projection disagrees with committed close allocation";
            return false;
        }
    }
    auto pending_it = pending_orders_.find(trade.order_id);
    if (close) {
        for (const auto& dated : {std::make_pair("today", allocation.today),
                                  std::make_pair("yesterday", allocation.yesterday)}) {
            if (dated.second == 0) continue;
            auto& bucket =
                positions_[dated.first == std::string("today") ? today_key : yesterday_key];
            bucket.position -= dated.second;
            bucket.last_update_ts_ns = trade.trade_ts_ns;
            int remaining = dated.second;
            if (pending_it != pending_orders_.end()) {
                for (auto& reservation : pending_it->second.close_allocations) {
                    if (reservation.position_date != dated.first) continue;
                    const int released = std::min(remaining, reservation.frozen_volume);
                    bucket.frozen = std::max(0, bucket.frozen - released);
                    reservation.frozen_volume -= released;
                    remaining -= released;
                }
            }
            bucket.frozen = std::min(bucket.frozen, bucket.position);
        }
    } else {
        auto& bucket = positions_[today_key];
        bucket.position += trade.quantity;
        bucket.last_update_ts_ns = trade.trade_ts_ns;
    }
    if (pending_it != pending_orders_.end()) {
        pending_it->second.booked_trade_volume += trade.quantity;
        if (pending_it->second.terminal &&
            pending_it->second.booked_trade_volume >= pending_it->second.last_filled_volume)
            pending_orders_.erase(pending_it);
    }
    applied_trade_identities_.insert(identity);
    if (error != nullptr) error->clear();
    return true;
}

CtpPositionView CtpPositionLedger::GetPosition(const std::string& account_id,
                                               const std::string& instrument_id,
                                               PositionDirection direction,
                                               const std::string& position_date,
                                               const std::string& exchange_id,
                                               const std::string& hedge_flag) const {
    CtpPositionView view;
    view.account_id = account_id;
    view.instrument_id = instrument_id;
    view.exchange_id = NormalizeExchangeId(exchange_id, instrument_id);
    view.hedge_flag = NormalizeHedgeFlag(hedge_flag);
    view.direction = direction;
    view.position_date = NormalizePositionDate(position_date);
    view.last_update_ts_ns = NowEpochNanos();

    const auto key = MakeKey(account_id, instrument_id, view.exchange_id, hedge_flag, direction,
                             view.position_date);
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = positions_.find(key);
    if (it == positions_.end()) {
        return view;
    }
    view.position = it->second.position;
    view.frozen = it->second.frozen;
    view.closable = std::max(0, it->second.position - it->second.frozen);
    view.last_update_ts_ns = it->second.last_update_ts_ns;
    return view;
}

std::int32_t CtpPositionLedger::GetClosableVolume(const std::string& account_id,
                                                  const std::string& instrument_id,
                                                  PositionDirection direction,
                                                  const std::string& position_date,
                                                  const std::string& exchange_id,
                                                  const std::string& hedge_flag) const {
    const auto snapshot =
        GetPosition(account_id, instrument_id, direction, position_date, exchange_id, hedge_flag);
    return snapshot.closable;
}

bool CtpPositionLedger::IsCloseOffset(OffsetFlag offset) {
    return offset == OffsetFlag::kClose || offset == OffsetFlag::kCloseToday ||
           offset == OffsetFlag::kCloseYesterday;
}

bool CtpPositionLedger::IsTerminalStatus(OrderStatus status) {
    return status == OrderStatus::kFilled || status == OrderStatus::kCanceled ||
           status == OrderStatus::kRejected;
}

std::string CtpPositionLedger::NormalizePositionDate(const std::string& raw) {
    if (raw.empty()) {
        return "today";
    }
    const auto normalized = LowerAscii(raw);
    if (normalized == "1" || normalized == "today" || normalized == "td") {
        return "today";
    }
    if (normalized == "2" || normalized == "yesterday" || normalized == "yd") {
        return "yesterday";
    }
    return normalized;
}

std::string CtpPositionLedger::NormalizeExchangeId(const std::string& raw,
                                                   const std::string& instrument_id) {
    if (!raw.empty()) {
        return raw;
    }
    const auto dot = instrument_id.find('.');
    if (dot == std::string::npos || dot == 0) {
        return "";
    }
    return instrument_id.substr(0, dot);
}

std::string CtpPositionLedger::NormalizeHedgeFlag(const std::string& raw) {
    if (raw.empty()) {
        return "1";
    }
    const auto normalized = LowerAscii(raw);
    if (normalized == "speculation" || normalized == "spec" || normalized == "s") {
        return "1";
    }
    if (normalized == "hedge" || normalized == "h") {
        return "3";
    }
    if (normalized == "arbitrage" || normalized == "a") {
        return "2";
    }
    return raw;
}

std::string CtpPositionLedger::ResolvePositionDateForIntent(const CtpOrderIntentForLedger& intent) {
    if (intent.offset == OffsetFlag::kCloseToday) {
        return "today";
    }
    if (intent.offset == OffsetFlag::kCloseYesterday) {
        return "yesterday";
    }
    return NormalizePositionDate(intent.position_date);
}

std::vector<std::string> CtpPositionLedger::ResolvePositionDatesForIntent(
    const CtpOrderIntentForLedger& intent) {
    if (intent.offset == OffsetFlag::kCloseToday) {
        return {"today"};
    }
    if (intent.offset == OffsetFlag::kCloseYesterday) {
        return {"yesterday"};
    }
    if (!intent.position_date.empty()) {
        return {NormalizePositionDate(intent.position_date)};
    }
    return {"today", "yesterday"};
}

PositionDirection CtpPositionLedger::ParsePositionDirection(const std::string& raw) {
    const auto normalized = LowerAscii(raw);
    if (normalized == "2" || normalized == "long" || normalized == "l") {
        return PositionDirection::kLong;
    }
    return PositionDirection::kShort;
}

CtpPositionLedger::PositionKey CtpPositionLedger::MakeKey(
    const std::string& account_id, const std::string& instrument_id, const std::string& exchange_id,
    const std::string& hedge_flag, PositionDirection direction, const std::string& position_date) {
    PositionKey key;
    key.account_id = account_id;
    key.instrument_id = instrument_id;
    key.exchange_id = NormalizeExchangeId(exchange_id, instrument_id);
    key.hedge_flag = NormalizeHedgeFlag(hedge_flag);
    key.direction = direction;
    key.position_date = NormalizePositionDate(position_date);
    return key;
}

std::int32_t CtpPositionLedger::ClampNonNegative(std::int32_t value) {
    return value < 0 ? 0 : value;
}

}  // namespace quant_hft
