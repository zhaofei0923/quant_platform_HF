#include "quant_hft/monitoring/dashboard_snapshot.h"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iterator>
#include <locale>
#include <mutex>
#include <optional>
#include <sstream>
#include <thread>
#include <vector>

namespace quant_hft {
namespace {

std::int64_t NowMillis() noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

template <std::size_t N>
bool CopyText(std::array<char, N>* destination, const std::string& text) noexcept {
    if (text.size() >= N || text.find('\0') != std::string::npos) return false;
    destination->fill('\0');
    std::copy(text.begin(), text.end(), destination->begin());
    return true;
}

bool SafeComponent(const std::string& text) {
    return !text.empty() && text.size() < 128 && text != "." && text != ".." &&
           std::all_of(text.begin(), text.end(), [](unsigned char ch) {
               return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
                      (ch >= '0' && ch <= '9') || ch == '_' || ch == '-' || ch == '.';
           });
}

bool ValidDay(const std::string& text) noexcept {
    return text.size() == 8 &&
           std::all_of(text.begin(), text.end(), [](char ch) { return ch >= '0' && ch <= '9'; });
}

bool ValidMarketPrice(double value) noexcept {
    return std::isfinite(value) && value > 0.0 && std::fabs(value) < 1e100;
}

constexpr std::size_t kMarketIndexSlots = 4096;
static_assert((kMarketIndexSlots & (kMarketIndexSlots - 1)) == 0,
              "market index capacity must be a power of two");

std::uint64_t HashInstrument(const std::string& instrument_id) noexcept {
    std::uint64_t hash = 14695981039346656037ULL;
    for (const unsigned char ch : instrument_id) {
        hash ^= ch;
        hash *= 1099511628211ULL;
    }
    return hash;
}

void Quote(std::ostream& out, const char* text) {
    out << '"';
    for (const auto* cursor = reinterpret_cast<const unsigned char*>(text); *cursor; ++cursor) {
        const auto ch = *cursor;
        if (ch == '"' || ch == '\\') {
            out << '\\' << static_cast<char>(ch);
        } else if (ch < 0x20) {
            const char* digits = "0123456789abcdef";
            out << "\\u00" << digits[ch >> 4] << digits[ch & 15];
        } else {
            out << static_cast<char>(ch);
        }
    }
    out << '"';
}

bool WriteAtomic(const std::string& path, const std::string& contents, mode_t mode) {
    std::string pattern = path + ".tmp.XXXXXX";
    std::vector<char> temporary(pattern.begin(), pattern.end());
    temporary.push_back('\0');
    const int fd = ::mkstemp(temporary.data());
    if (fd < 0) return false;
    bool ok = ::fchmod(fd, mode) == 0;
    std::size_t offset = 0;
    while (ok && offset < contents.size()) {
        const auto count = ::write(fd, contents.data() + offset, contents.size() - offset);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) {
            ok = false;
        } else {
            offset += static_cast<std::size_t>(count);
        }
    }
    if (ok) ok = ::fsync(fd) == 0;
    if (::close(fd) != 0) ok = false;
    if (ok) ok = ::rename(temporary.data(), path.c_str()) == 0;
    if (!ok) {
        (void)::unlink(temporary.data());
        return false;
    }
    const auto parent = std::filesystem::path(path).parent_path().string();
    const int parent_fd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    ok = parent_fd >= 0 && ::fsync(parent_fd) == 0;
    if (parent_fd >= 0) (void)::close(parent_fd);
    return ok;
}

enum class Quality { kMissing, kOk, kFailed };
enum class Failure {
    kNone,
    kQueryFailed,
    kIncomplete,
    kIdentity,
    kInvalid,
    kPositionCapacity,
    kMarketCapacity,
    kRiskCapacity,
    kOld
};

const char* QualityName(Quality quality) {
    switch (quality) {
        case Quality::kMissing:
            return "missing";
        case Quality::kOk:
            return "ok";
        case Quality::kFailed:
            return "failed";
    }
    return "missing";
}

const char* FailureName(Failure failure) {
    switch (failure) {
        case Failure::kNone:
            return "";
        case Failure::kQueryFailed:
            return "query_failed";
        case Failure::kIncomplete:
            return "query_incomplete";
        case Failure::kIdentity:
            return "identity_mismatch";
        case Failure::kInvalid:
            return "invalid_fields";
        case Failure::kPositionCapacity:
            return "position_capacity_exceeded";
        case Failure::kMarketCapacity:
            return "market_capacity_exceeded";
        case Failure::kRiskCapacity:
            return "strategy_risk_capacity_exceeded";
        case Failure::kOld:
            return "out_of_order";
    }
    return "invalid_fields";
}

struct Block {
    Quality quality{Quality::kMissing};
    Failure error{Failure::kNone};
    bool has_data{false};
    std::int64_t as_of_ms{0};
    std::int64_t last_attempt_ms{0};
    std::uint64_t generation{0};
    std::array<char, 64> source{};
    std::array<char, 9> trading_day{};
};

struct CachedAccount {
    Block block;
    std::array<double, 9> values{};
};

struct CachedPosition {
    std::array<char, 128> instrument_id{};
    std::array<char, 32> exchange_id{};
    std::array<char, 16> posi_direction{};
    std::array<char, 16> hedge_flag{};
    std::array<char, 16> position_date{};
    std::array<std::int32_t, 7> counts{};
    std::array<double, 7> values{};
};

struct CachedMarketQuote {
    std::array<char, 128> instrument_id{};
    std::array<char, 32> exchange_id{};
    std::array<char, 9> trading_day{};
    double last_price{0.0};
    double bid_price_1{0.0};
    double ask_price_1{0.0};
    bool has_bid{false};
    bool has_ask{false};
    std::int64_t bid_volume_1{0};
    std::int64_t ask_volume_1{0};
    std::int64_t volume{0};
    std::int64_t open_interest{0};
    EpochNanos exchange_ts_ns{0};
    EpochNanos recv_ts_ns{0};
};

struct CachedStrategyRisk {
    std::array<char, 128> strategy_id{};
    std::array<char, 128> owner_strategy_id{};
    std::array<char, 128> instrument_id{};
    std::int32_t net{0};
    std::array<double, 5> prices{};
    std::array<bool, 5> has_price{};
    StrategyStopKind stop_kind{StrategyStopKind::kNone};
    EpochNanos as_of_ns{0};
};

constexpr const char* kAccountFields[] = {"balance",       "available",    "curr_margin",
                                          "frozen_margin", "frozen_cash",  "frozen_commission",
                                          "commission",    "close_profit", "position_profit"};
constexpr const char* kPositionCounts[] = {"position",    "today_position", "yd_position",
                                           "long_frozen", "short_frozen",   "open_volume",
                                           "close_volume"};
constexpr const char* kPositionValues[] = {
    "position_cost",         "open_cost", "position_profit", "close_profit", "margin_rate_by_money",
    "margin_rate_by_volume", "use_margin"};

void RenderBlock(std::ostream& out, const Block& block,
                 std::optional<std::uint64_t> dropped_updates = std::nullopt) {
    out << "{\"quality\":";
    Quote(out, QualityName(block.quality));
    out << ",\"source\":";
    Quote(out, block.source.data());
    out << ",\"as_of_ms\":" << block.as_of_ms << ",\"trading_day\":";
    Quote(out, block.trading_day.data());
    out << ",\"generation\":" << block.generation;
    if (dropped_updates.has_value()) {
        out << ",\"dropped_updates\":" << *dropped_updates;
    }
    out << ",\"last_attempt_ms\":" << block.last_attempt_ms << ",\"error_code\":";
    Quote(out, FailureName(block.error));
    out << ",\"data\":";
}

void RenderNumberOrNull(std::ostream& out, bool present, double value) {
    if (present)
        out << value;
    else
        out << "null";
}

const char* StopKindName(StrategyStopKind kind) {
    switch (kind) {
        case StrategyStopKind::kInitial:
            return "initial";
        case StrategyStopKind::kTrailing:
            return "trailing";
        case StrategyStopKind::kNone:
            return "";
    }
    return "";
}

}  // namespace

struct DashboardSnapshotWriter::Impl {
    RuntimeIdentity identity;
    std::string output;
    std::int64_t started_at_ms{0};
    std::mutex cache_mutex;
    CachedAccount account;
    Block positions;
    std::size_t position_count{0};
    std::unique_ptr<std::array<CachedPosition, kMaxPositions>> position_rows{
        std::make_unique<std::array<CachedPosition, kMaxPositions>>()};
    std::unique_ptr<std::array<CachedPosition, kMaxPositions>> staging_rows{
        std::make_unique<std::array<CachedPosition, kMaxPositions>>()};
    std::mutex market_mutex;
    Block market_quotes;
    std::size_t market_quote_count{0};
    std::unique_ptr<std::array<CachedMarketQuote, kMaxMarketQuotes>> market_quote_rows{
        std::make_unique<std::array<CachedMarketQuote, kMaxMarketQuotes>>()};
    std::unique_ptr<std::array<std::int32_t, kMarketIndexSlots>> market_quote_index{
        std::make_unique<std::array<std::int32_t, kMarketIndexSlots>>()};
    std::mutex strategy_risk_mutex;
    Block strategy_risk;
    std::size_t strategy_risk_count{0};
    std::unique_ptr<std::array<CachedStrategyRisk, kMaxStrategyRiskRows>> strategy_risk_rows{
        std::make_unique<std::array<CachedStrategyRisk, kMaxStrategyRiskRows>>()};
    std::unique_ptr<std::array<CachedStrategyRisk, kMaxStrategyRiskRows>>
        strategy_risk_staging_rows{
            std::make_unique<std::array<CachedStrategyRisk, kMaxStrategyRiskRows>>()};
    std::mutex publish_mutex;
    std::mutex wait_mutex;
    std::condition_variable wake;
    std::atomic<bool> running{false};
    std::atomic<std::uint64_t> dropped{0};
    std::atomic<std::uint64_t> market_dropped{0};
    std::atomic<std::uint64_t> strategy_risk_dropped{0};
    std::atomic<std::uint64_t> failures{0};
    std::thread worker;
    int lock_fd{-1};

    void Fail(Block* block, Failure failure) noexcept {
        block->quality = Quality::kFailed;
        block->error = failure;
        block->last_attempt_ms = NowMillis();
    }

    void DropMarket() noexcept {
        dropped.fetch_add(1);
        market_dropped.fetch_add(1);
    }

    void DropStrategyRisk() noexcept {
        dropped.fetch_add(1);
        strategy_risk_dropped.fetch_add(1);
    }
};

DashboardSnapshotWriter::DashboardSnapshotWriter() : impl_(std::make_unique<Impl>()) {}
DashboardSnapshotWriter::~DashboardSnapshotWriter() { Stop(); }

bool DashboardSnapshotWriter::Start(const std::string& output_file, const RuntimeIdentity& identity,
                                    std::string* error) noexcept {
    auto fail = [&](const char* reason) noexcept {
        try {
            if (error != nullptr) *error = reason;
        } catch (...) {
        }
        Stop();
        return false;
    };
    if (impl_->running.load() || impl_->worker.joinable()) return false;
    try {
        if (output_file.empty() ||
            (identity.environment != "sim" && identity.environment != "simnow" &&
             identity.environment != "prod") ||
            !SafeComponent(identity.broker_id) || !SafeComponent(identity.account_id) ||
            !SafeComponent(identity.instance)) {
            return fail("invalid dashboard output or runtime identity");
        }
        impl_->output = std::filesystem::absolute(output_file).lexically_normal().string();
        const auto parent = std::filesystem::path(impl_->output).parent_path();
        if (std::filesystem::create_directories(parent) && ::chmod(parent.c_str(), 0750) != 0) {
            return fail("dashboard private directory permissions failed");
        }
        impl_->lock_fd = ::open((impl_->output + ".lock").c_str(),
                                O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0600);
        if (impl_->lock_fd < 0 || ::flock(impl_->lock_fd, LOCK_EX | LOCK_NB) != 0) {
            return fail("dashboard output already owned or inaccessible");
        }
        impl_->identity = identity;
        const std::string manifest =
            "schema=1\nenvironment=" + identity.environment + "\nbroker=" + identity.broker_id +
            "\naccount=" + identity.account_id + "\ninstance=" + identity.instance + "\n";
        const auto manifest_path = impl_->output + ".identity";
        const auto manifest_status = std::filesystem::symlink_status(manifest_path);
        if (std::filesystem::exists(manifest_status)) {
            if (!std::filesystem::is_regular_file(manifest_status)) {
                return fail("dashboard identity is not a regular file");
            }
            std::ifstream input(manifest_path);
            const std::string contents((std::istreambuf_iterator<char>(input)), {});
            if ((!input.good() && !input.eof()) || contents != manifest) {
                return fail("dashboard output identity mismatch");
            }
        } else {
            if (std::filesystem::exists(std::filesystem::symlink_status(impl_->output))) {
                return fail("unbound dashboard output requires explicit migration");
            }
            if (!WriteAtomic(manifest_path, manifest, 0600)) {
                return fail("dashboard identity write failed");
            }
        }
        {
            std::lock_guard<std::mutex> lock(impl_->cache_mutex);
            impl_->account = CachedAccount{};
            impl_->positions = Block{};
            impl_->position_count = 0;
        }
        {
            std::lock_guard<std::mutex> lock(impl_->market_mutex);
            impl_->market_quotes = Block{};
            impl_->market_quote_count = 0;
            impl_->market_quote_index->fill(-1);
        }
        {
            std::lock_guard<std::mutex> lock(impl_->strategy_risk_mutex);
            impl_->strategy_risk = Block{};
            impl_->strategy_risk_count = 0;
        }
        impl_->started_at_ms = NowMillis();
        impl_->dropped.store(0);
        impl_->market_dropped.store(0);
        impl_->strategy_risk_dropped.store(0);
        impl_->failures.store(0);
        impl_->running.store(true);
        if (!PublishNow()) return fail("dashboard initial private snapshot write failed");
        impl_->worker = std::thread([this] {
            while (impl_->running.load()) {
                std::unique_lock<std::mutex> lock(impl_->wait_mutex);
                if (impl_->wake.wait_for(lock, std::chrono::seconds(1),
                                         [this] { return !impl_->running.load(); }))
                    break;
                lock.unlock();
                (void)PublishNow();
            }
        });
        return true;
    } catch (...) {
        return fail("dashboard snapshot initialization failed");
    }
}

void DashboardSnapshotWriter::Stop() noexcept {
    {
        // Coordinate with wait_for so shutdown cannot lose a notification between
        // its predicate check and sleeping. This mutex is never used by callbacks.
        std::lock_guard<std::mutex> lock(impl_->wait_mutex);
        impl_->running.store(false);
    }
    impl_->wake.notify_all();
    if (impl_->worker.joinable()) impl_->worker.join();
    if (impl_->lock_fd >= 0) {
        (void)::flock(impl_->lock_fd, LOCK_UN);
        (void)::close(impl_->lock_fd);
        impl_->lock_fd = -1;
    }
}

void DashboardSnapshotWriter::CaptureAccount(const TradingAccountSnapshot& snapshot) noexcept {
    if (!impl_->running.load()) return;
    try {
        std::unique_lock<std::mutex> lock(impl_->cache_mutex, std::try_to_lock);
        if (!lock.owns_lock()) {
            impl_->dropped.fetch_add(1);
            return;
        }
        auto& previous = impl_->account;
        if (snapshot.investor_id != impl_->identity.account_id || snapshot.account_id.empty()) {
            impl_->Fail(&previous.block, Failure::kIdentity);
            return;
        }
        CachedAccount next;
        next.values = {snapshot.balance,       snapshot.available,    snapshot.curr_margin,
                       snapshot.frozen_margin, snapshot.frozen_cash,  snapshot.frozen_commission,
                       snapshot.commission,    snapshot.close_profit, snapshot.position_profit};
        if (!ValidDay(snapshot.trading_day) || snapshot.ts_ns <= 0 || snapshot.source.empty() ||
            !CopyText(&next.block.source, snapshot.source) ||
            !CopyText(&next.block.trading_day, snapshot.trading_day) ||
            !std::all_of(next.values.begin(), next.values.end(),
                         [](double value) { return std::isfinite(value); })) {
            impl_->Fail(&previous.block, Failure::kInvalid);
            return;
        }
        next.block.as_of_ms = snapshot.ts_ns / 1000000;
        if (previous.block.has_data && (next.block.as_of_ms < previous.block.as_of_ms ||
                                        snapshot.trading_day < previous.block.trading_day.data())) {
            impl_->Fail(&previous.block, Failure::kOld);
            return;
        }
        next.block.quality = Quality::kOk;
        next.block.has_data = true;
        next.block.last_attempt_ms = NowMillis();
        next.block.generation = previous.block.generation + 1;
        previous = next;
    } catch (...) {
        impl_->dropped.fetch_add(1);
    }
}

void DashboardSnapshotWriter::CapturePositions(
    const QueryResult<InvestorPositionSnapshot>& result) noexcept {
    if (!impl_->running.load()) return;
    try {
        std::unique_lock<std::mutex> lock(impl_->cache_mutex, std::try_to_lock);
        if (!lock.owns_lock()) {
            impl_->dropped.fetch_add(1);
            return;
        }
        auto& previous = impl_->positions;
        const auto& meta = result.metadata;
        if (meta.account_id != impl_->identity.account_id) {
            impl_->Fail(&previous, Failure::kIdentity);
            return;
        }
        if (!meta.success || !meta.complete || !meta.full_account || !meta.instrument_id.empty()) {
            impl_->Fail(&previous, meta.success ? Failure::kIncomplete : Failure::kQueryFailed);
            return;
        }
        if (result.rows.size() > kMaxPositions) {
            impl_->Fail(&previous, Failure::kPositionCapacity);
            return;
        }
        Block next;
        if (!ValidDay(meta.trading_day) || meta.source.empty() ||
            !CopyText(&next.source, meta.source) ||
            !CopyText(&next.trading_day, meta.trading_day)) {
            impl_->Fail(&previous, Failure::kInvalid);
            return;
        }
        if (previous.has_data && (meta.generation < previous.generation ||
                                  meta.trading_day < previous.trading_day.data())) {
            impl_->Fail(&previous, Failure::kOld);
            return;
        }
        auto as_of_ms = NowMillis();
        for (std::size_t index = 0; index < result.rows.size(); ++index) {
            const auto& row = result.rows[index];
            if (row.account_id != impl_->identity.account_id ||
                row.investor_id != impl_->identity.account_id) {
                impl_->Fail(&previous, Failure::kIdentity);
                return;
            }
            auto& next_row = (*impl_->staging_rows)[index];
            next_row.counts = {row.position,    row.today_position, row.yd_position,
                               row.long_frozen, row.short_frozen,   row.open_volume,
                               row.close_volume};
            next_row.values = {row.position_cost,
                               row.open_cost,
                               row.position_profit,
                               row.close_profit,
                               row.margin_rate_by_money,
                               row.margin_rate_by_volume,
                               row.use_margin};
            if (row.ts_ns <= 0 || row.source != meta.source || row.instrument_id.empty() ||
                row.posi_direction.empty() || row.hedge_flag.empty() || row.position_date.empty() ||
                !CopyText(&next_row.instrument_id, row.instrument_id) ||
                !CopyText(&next_row.exchange_id, row.exchange_id) ||
                !CopyText(&next_row.posi_direction, row.posi_direction) ||
                !CopyText(&next_row.hedge_flag, row.hedge_flag) ||
                !CopyText(&next_row.position_date, row.position_date) ||
                !std::all_of(next_row.counts.begin(), next_row.counts.end(),
                             [](std::int32_t value) { return value >= 0; }) ||
                !std::all_of(next_row.values.begin(), next_row.values.end(),
                             [](double value) { return std::isfinite(value); })) {
                impl_->Fail(&previous, Failure::kInvalid);
                return;
            }
            as_of_ms = std::min(as_of_ms, row.ts_ns / 1000000);
        }
        if (previous.has_data && as_of_ms < previous.as_of_ms) {
            impl_->Fail(&previous, Failure::kOld);
            return;
        }
        next.quality = Quality::kOk;
        next.has_data = true;
        next.as_of_ms = as_of_ms;
        next.last_attempt_ms = NowMillis();
        next.generation = meta.generation;
        previous = next;
        impl_->position_count = result.rows.size();
        impl_->position_rows.swap(impl_->staging_rows);
    } catch (...) {
        impl_->dropped.fetch_add(1);
    }
}

void DashboardSnapshotWriter::MarkAccountQueryFailed() noexcept {
    if (!impl_->running.load()) return;
    try {
        std::unique_lock<std::mutex> lock(impl_->cache_mutex, std::try_to_lock);
        if (lock.owns_lock())
            impl_->Fail(&impl_->account.block, Failure::kQueryFailed);
        else
            impl_->dropped.fetch_add(1);
    } catch (...) {
        impl_->dropped.fetch_add(1);
    }
}

void DashboardSnapshotWriter::MarkPositionQueryFailed() noexcept {
    if (!impl_->running.load()) return;
    try {
        std::unique_lock<std::mutex> lock(impl_->cache_mutex, std::try_to_lock);
        if (lock.owns_lock())
            impl_->Fail(&impl_->positions, Failure::kQueryFailed);
        else
            impl_->dropped.fetch_add(1);
    } catch (...) {
        impl_->dropped.fetch_add(1);
    }
}

void DashboardSnapshotWriter::CaptureMarket(const MarketSnapshot& snapshot) noexcept {
    if (!impl_->running.load()) return;
    try {
        std::unique_lock<std::mutex> lock(impl_->market_mutex, std::try_to_lock);
        if (!lock.owns_lock()) {
            impl_->DropMarket();
            return;
        }
        auto& block = impl_->market_quotes;
        CachedMarketQuote next;
        if (snapshot.instrument_id.empty() || !ValidDay(snapshot.trading_day) ||
            snapshot.recv_ts_ns <= 0 || !ValidMarketPrice(snapshot.last_price) ||
            snapshot.bid_volume_1 < 0 ||
            snapshot.ask_volume_1 < 0 || snapshot.volume < 0 || snapshot.open_interest < 0 ||
            !CopyText(&next.instrument_id, snapshot.instrument_id) ||
            !CopyText(&next.exchange_id, snapshot.exchange_id) ||
            !CopyText(&next.trading_day, snapshot.trading_day)) {
            impl_->Fail(&block, Failure::kInvalid);
            impl_->DropMarket();
            return;
        }
        if (block.has_data && snapshot.trading_day < block.trading_day.data()) {
            impl_->DropMarket();
            return;
        }
        if (block.has_data && snapshot.trading_day > block.trading_day.data()) {
            block = Block{};
            impl_->market_quote_count = 0;
            impl_->market_quote_index->fill(-1);
        }

        std::size_t index = impl_->market_quote_count;
        std::size_t index_slot =
            static_cast<std::size_t>(HashInstrument(snapshot.instrument_id)) &
            (kMarketIndexSlots - 1);
        for (std::size_t probe = 0; probe < kMarketIndexSlots; ++probe) {
            const auto existing = (*impl_->market_quote_index)[index_slot];
            if (existing < 0) break;
            if (static_cast<std::size_t>(existing) < impl_->market_quote_count &&
                snapshot.instrument_id ==
                    (*impl_->market_quote_rows)[static_cast<std::size_t>(existing)]
                        .instrument_id.data()) {
                index = static_cast<std::size_t>(existing);
                break;
            }
            index_slot = (index_slot + 1) & (kMarketIndexSlots - 1);
        }
        if (index == impl_->market_quote_count &&
            impl_->market_quote_count == kMaxMarketQuotes) {
            impl_->Fail(&block, Failure::kMarketCapacity);
            impl_->DropMarket();
            return;
        }
        if (index < impl_->market_quote_count) {
            const auto& previous = (*impl_->market_quote_rows)[index];
            const EpochNanos incoming_event_ns = snapshot.exchange_ts_ns > 0
                                                     ? snapshot.exchange_ts_ns
                                                     : snapshot.recv_ts_ns;
            const EpochNanos previous_event_ns = previous.exchange_ts_ns > 0
                                                     ? previous.exchange_ts_ns
                                                     : previous.recv_ts_ns;
            if (incoming_event_ns < previous_event_ns ||
                (incoming_event_ns == previous_event_ns &&
                 snapshot.recv_ts_ns < previous.recv_ts_ns)) {
                impl_->DropMarket();
                return;
            }
        }

        next.last_price = snapshot.last_price;
        next.has_bid = ValidMarketPrice(snapshot.bid_price_1);
        next.has_ask = ValidMarketPrice(snapshot.ask_price_1);
        next.bid_price_1 = next.has_bid ? snapshot.bid_price_1 : 0.0;
        next.ask_price_1 = next.has_ask ? snapshot.ask_price_1 : 0.0;
        next.bid_volume_1 = snapshot.bid_volume_1;
        next.ask_volume_1 = snapshot.ask_volume_1;
        next.volume = snapshot.volume;
        next.open_interest = snapshot.open_interest;
        next.exchange_ts_ns = snapshot.exchange_ts_ns;
        next.recv_ts_ns = snapshot.recv_ts_ns;
        (*impl_->market_quote_rows)[index] = next;
        if (index == impl_->market_quote_count) {
            (*impl_->market_quote_index)[index_slot] = static_cast<std::int32_t>(index);
            ++impl_->market_quote_count;
        }

        block.quality = Quality::kOk;
        block.error = Failure::kNone;
        block.has_data = true;
        block.as_of_ms = snapshot.recv_ts_ns / 1'000'000;
        block.last_attempt_ms = NowMillis();
        ++block.generation;
        (void)CopyText(&block.source, "ctp_market_callback");
        (void)CopyText(&block.trading_day, snapshot.trading_day);
    } catch (...) {
        impl_->DropMarket();
    }
}

void DashboardSnapshotWriter::CaptureStrategyRisk(
    const std::vector<StrategyRiskSnapshot>& rows, EpochNanos as_of_ns,
    const std::string& trading_day) noexcept {
    if (!impl_->running.load()) return;
    try {
        std::unique_lock<std::mutex> lock(impl_->strategy_risk_mutex, std::try_to_lock);
        if (!lock.owns_lock()) {
            impl_->DropStrategyRisk();
            return;
        }
        auto& previous = impl_->strategy_risk;
        if (as_of_ns <= 0 || !ValidDay(trading_day)) {
            impl_->Fail(&previous, Failure::kInvalid);
            impl_->DropStrategyRisk();
            return;
        }
        if (rows.size() > kMaxStrategyRiskRows) {
            impl_->Fail(&previous, Failure::kRiskCapacity);
            impl_->DropStrategyRisk();
            return;
        }
        std::int64_t oldest_risk_ms = 0;
        bool missing_risk_time = false;
        for (std::size_t i = 0; i < rows.size(); ++i) {
            const auto& row = rows[i];
            auto& next = (*impl_->strategy_risk_staging_rows)[i];
            next = CachedStrategyRisk{};
            const auto valid_price = [](const std::optional<double>& price) {
                return !price.has_value() || (std::isfinite(*price) && *price > 0.0);
            };
            const bool valid_stop =
                (row.stop_kind == StrategyStopKind::kNone && !row.effective_stop.has_value()) ||
                (row.stop_kind == StrategyStopKind::kInitial && row.initial_stop.has_value() &&
                 row.effective_stop == row.initial_stop) ||
                (row.stop_kind == StrategyStopKind::kTrailing && row.trailing_stop.has_value() &&
                 row.effective_stop == row.trailing_stop);
            if (row.account_id != impl_->identity.account_id || row.strategy_id.empty() ||
                row.instrument_id.empty() || row.net == 0 || row.as_of_ns < 0 || !valid_stop ||
                !valid_price(row.avg_open) || !valid_price(row.initial_stop) ||
                !valid_price(row.trailing_stop) || !valid_price(row.effective_stop) ||
                !valid_price(row.take_profit) || !CopyText(&next.strategy_id, row.strategy_id) ||
                !CopyText(&next.owner_strategy_id, row.owner_strategy_id) ||
                !CopyText(&next.instrument_id, row.instrument_id)) {
                impl_->Fail(&previous, row.account_id != impl_->identity.account_id
                                           ? Failure::kIdentity
                                           : Failure::kInvalid);
                impl_->DropStrategyRisk();
                return;
            }
            next.net = row.net;
            const std::array<std::optional<double>, 5> prices = {
                row.avg_open, row.initial_stop, row.trailing_stop, row.effective_stop,
                row.take_profit};
            for (std::size_t n = 0; n < prices.size(); ++n) {
                next.has_price[n] = prices[n].has_value();
                next.prices[n] = prices[n].value_or(0.0);
            }
            next.stop_kind = row.stop_kind;
            next.as_of_ns = row.as_of_ns;
            if (row.as_of_ns > 0) {
                const auto row_ms = row.as_of_ns / 1'000'000;
                oldest_risk_ms = oldest_risk_ms == 0 ? row_ms : std::min(oldest_risk_ms, row_ms);
            } else {
                missing_risk_time = true;
            }
        }

        Block next_block;
        next_block.quality = Quality::kOk;
        next_block.error = Failure::kNone;
        next_block.has_data = true;
        next_block.as_of_ms =
            rows.empty() ? as_of_ns / 1'000'000 : (missing_risk_time ? 0 : oldest_risk_ms);
        next_block.last_attempt_ms = NowMillis();
        next_block.generation = previous.generation + 1;
        (void)CopyText(&next_block.source, "strategy_engine");
        (void)CopyText(&next_block.trading_day, trading_day);
        previous = next_block;
        impl_->strategy_risk_count = rows.size();
        impl_->strategy_risk_rows.swap(impl_->strategy_risk_staging_rows);
    } catch (...) {
        impl_->DropStrategyRisk();
    }
}

std::string DashboardSnapshotWriter::RenderSnapshot(std::int64_t now_ms) {
    CachedAccount account;
    Block positions;
    // Allocate outside the cache lock. Callback writers only copy bounded POD rows.
    std::vector<CachedPosition> rows(kMaxPositions);
    {
        std::lock_guard<std::mutex> lock(impl_->cache_mutex);
        account = impl_->account;
        positions = impl_->positions;
        std::copy_n(impl_->position_rows->begin(), impl_->position_count, rows.begin());
        rows.resize(impl_->position_count);
    }
    Block market_quotes;
    std::vector<CachedMarketQuote> market_rows(kMaxMarketQuotes);
    {
        std::lock_guard<std::mutex> lock(impl_->market_mutex);
        market_quotes = impl_->market_quotes;
        std::copy_n(impl_->market_quote_rows->begin(), impl_->market_quote_count,
                    market_rows.begin());
        market_rows.resize(impl_->market_quote_count);
    }
    Block strategy_risk;
    std::vector<CachedStrategyRisk> risk_rows(kMaxStrategyRiskRows);
    {
        std::lock_guard<std::mutex> lock(impl_->strategy_risk_mutex);
        strategy_risk = impl_->strategy_risk;
        std::copy_n(impl_->strategy_risk_rows->begin(), impl_->strategy_risk_count,
                    risk_rows.begin());
        risk_rows.resize(impl_->strategy_risk_count);
    }
    std::ostringstream out;
    out.imbue(std::locale::classic());
    out << std::setprecision(17) << "{\"schema_version\":2,\"identity\":{\"environment\":";
    Quote(out, impl_->identity.environment.c_str());
    out << ",\"broker_id\":";
    Quote(out, impl_->identity.broker_id.c_str());
    out << ",\"account_id\":";
    Quote(out, impl_->identity.account_id.c_str());
    out << ",\"instance_id\":";
    Quote(out, impl_->identity.instance.c_str());
    out << "},\"generated_at_ms\":" << now_ms
        << ",\"writer_started_at_ms\":" << impl_->started_at_ms
        << ",\"dropped_updates\":" << impl_->dropped.load()
        << ",\"write_failures\":" << impl_->failures.load() << ",\"account\":";
    RenderBlock(out, account.block);
    if (!account.block.has_data)
        out << "null";
    else {
        out << '{';
        for (std::size_t i = 0; i < account.values.size(); ++i) {
            if (i != 0) out << ',';
            Quote(out, kAccountFields[i]);
            out << ':' << account.values[i];
        }
        out << '}';
    }
    out << "},\"positions\":";
    RenderBlock(out, positions);
    if (!positions.has_data)
        out << "null";
    else {
        out << '[';
        for (std::size_t i = 0; i < rows.size(); ++i) {
            if (i != 0) out << ',';
            const auto& row = rows[i];
            out << "{\"instrument_id\":";
            Quote(out, row.instrument_id.data());
            out << ",\"exchange_id\":";
            Quote(out, row.exchange_id.data());
            out << ",\"posi_direction\":";
            Quote(out, row.posi_direction.data());
            out << ",\"hedge_flag\":";
            Quote(out, row.hedge_flag.data());
            out << ",\"position_date\":";
            Quote(out, row.position_date.data());
            for (std::size_t n = 0; n < row.counts.size(); ++n) {
                out << ',';
                Quote(out, kPositionCounts[n]);
                out << ':' << row.counts[n];
            }
            for (std::size_t n = 0; n < row.values.size(); ++n) {
                out << ',';
                Quote(out, kPositionValues[n]);
                out << ':' << row.values[n];
            }
            out << '}';
        }
        out << ']';
    }
    out << "},\"market_quotes\":";
    RenderBlock(out, market_quotes, impl_->market_dropped.load());
    if (!market_quotes.has_data) {
        out << "null";
    } else {
        out << '[';
        for (std::size_t i = 0; i < market_rows.size(); ++i) {
            if (i != 0) out << ',';
            const auto& row = market_rows[i];
            out << "{\"instrument_id\":";
            Quote(out, row.instrument_id.data());
            out << ",\"exchange_id\":";
            Quote(out, row.exchange_id.data());
            out << ",\"trading_day\":";
            Quote(out, row.trading_day.data());
            out << ",\"last_price\":" << row.last_price << ",\"bid_price_1\":";
            RenderNumberOrNull(out, row.has_bid, row.bid_price_1);
            out << ",\"ask_price_1\":";
            RenderNumberOrNull(out, row.has_ask, row.ask_price_1);
            out << ",\"bid_volume_1\":" << row.bid_volume_1
                << ",\"ask_volume_1\":" << row.ask_volume_1 << ",\"volume\":" << row.volume
                << ",\"open_interest\":" << row.open_interest
                << ",\"exchange_ts_ns\":" << row.exchange_ts_ns
                << ",\"recv_ts_ns\":" << row.recv_ts_ns << ",\"as_of_ms\":"
                << row.recv_ts_ns / 1'000'000 << '}';
        }
        out << ']';
    }
    out << "},\"strategy_risk\":";
    RenderBlock(out, strategy_risk, impl_->strategy_risk_dropped.load());
    if (!strategy_risk.has_data) {
        out << "null";
    } else {
        static constexpr const char* kRiskPriceNames[] = {
            "avg_open", "initial_stop", "trailing_stop", "effective_stop", "take_profit"};
        out << '[';
        for (std::size_t i = 0; i < risk_rows.size(); ++i) {
            if (i != 0) out << ',';
            const auto& row = risk_rows[i];
            out << "{\"strategy_id\":";
            Quote(out, row.strategy_id.data());
            out << ",\"owner_strategy_id\":";
            Quote(out, row.owner_strategy_id.data());
            out << ",\"instrument_id\":";
            Quote(out, row.instrument_id.data());
            out << ",\"net\":" << row.net;
            for (std::size_t n = 0; n < row.prices.size(); ++n) {
                out << ',';
                Quote(out, kRiskPriceNames[n]);
                out << ':';
                RenderNumberOrNull(out, row.has_price[n], row.prices[n]);
            }
            out << ",\"stop_kind\":";
            Quote(out, StopKindName(row.stop_kind));
            out << ",\"as_of_ms\":";
            if (row.as_of_ns > 0)
                out << row.as_of_ns / 1'000'000;
            else
                out << "null";
            out << '}';
        }
        out << ']';
    }
    out << "}}\n";
    return out.str();
}

bool DashboardSnapshotWriter::PublishNow() noexcept {
    if (!impl_->running.load()) return false;
    try {
        std::lock_guard<std::mutex> lock(impl_->publish_mutex);
        if (WriteAtomic(impl_->output, RenderSnapshot(NowMillis()), 0640)) return true;
    } catch (...) {
    }
    impl_->failures.fetch_add(1);
    return false;
}

std::uint64_t DashboardSnapshotWriter::dropped_updates() const noexcept {
    return impl_->dropped.load();
}

std::uint64_t DashboardSnapshotWriter::write_failures() const noexcept {
    return impl_->failures.load();
}

}  // namespace quant_hft
