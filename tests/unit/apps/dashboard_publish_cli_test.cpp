#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "quant_hft/apps/dashboard_publisher.h"
#include "quant_hft/core/simple_json.h"
#include "quant_hft/core/wal_format.h"

// Standalone CTest executable: these integration tests need no GoogleTest download.
namespace {
namespace fs = std::filesystem;
using J = quant_hft::simple_json::Value;
using Publisher = quant_hft::dashboard::Publisher;
constexpr std::int64_t kNow = 1788746400000LL;  // 2026-09-07 10:00 Asia/Shanghai.
constexpr const char* kIdentity =
    "{\"environment\":\"simnow\",\"broker_id\":\"b-test\",\"account_id\":\"SECRET_ACCOUNT_8877\","
    "\"instance_id\":\"main\"}";
void Check(bool ok, const std::string& label) {
    if (!ok) throw std::runtime_error(label);
}
void Write(const fs::path& path, const std::string& content, bool append = false) {
    fs::create_directories(path.parent_path());
    std::ofstream out(path, append ? std::ios::app : std::ios::trunc);
    out << content;
    Check(out.good(), "fixture write");
}
std::string Read(const fs::path& path) {
    std::ifstream in(path);
    return {std::istreambuf_iterator<char>(in), {}};
}
J Parse(const std::string& text) {
    J value;
    std::string error;
    Check(quant_hft::simple_json::ParseStrict(text, &value, &error), "valid complete JSON");
    return value;
}
const J& Field(const J& j, const std::string& key) {
    const J* found = j.Find(key);
    Check(found != nullptr, "field exists: " + key);
    return *found;
}
std::string Text(const J& j, const std::string& key) { return Field(j, key).string_value; }
const J& InstrumentRow(const std::vector<J>& rows, const std::string& instrument) {
    for (const auto& row : rows)
        if (Text(row, "instrument_id") == instrument) return row;
    throw std::runtime_error("instrument row missing: " + instrument);
}
struct Fixture {
    fs::path dir, root, out, state;
    quant_hft::dashboard::PublisherOptions options;
    explicit Fixture(const std::string& label) {
        std::string pattern = "/tmp/dashboard-publish-" + label + "-XXXXXX";
        std::vector<char> name(pattern.begin(), pattern.end());
        name.push_back('\0');
        Check(::mkdtemp(name.data()) != nullptr, "temporary workspace");
        dir = name.data();
        root = dir / "runtime";
        out = dir / "public";
        state = dir / "private";
        fs::create_directories(root / "wal");
        const auto descriptor = dir / "identity.json";
        Write(descriptor, "{\"schema_version\":1,\"identity\":" + std::string(kIdentity) +
                              ",\"account_alias\":\"个人账户\",\"instance_alias\":\"云端主实例\","
                              "\"recovery_root\":\"" +
                              root.string() + "\"}");
        options.source_dir = root.string();
        options.output_dir = out.string();
        options.state_dir = state.string();
        options.identity_file = descriptor.string();
        Snapshot();
        Health();
        Write(root / "wal/events.wal", "");
    }
    ~Fixture() {
        std::error_code ec;
        fs::remove_all(dir, ec);
    }
    void Snapshot(std::int64_t stamp = kNow, const std::string& quality = "ok",
                  const std::string& positions = "[]", const std::string& identity = kIdentity,
                  const std::string& day = "20260907", double balance = 100000) {
        const auto when = std::to_string(stamp);
        Write(
            root / "monitor/dashboard_private.json",
            "{\"schema_version\":1,\"identity\":" + identity + ",\"generated_at_ms\":" + when +
                ",\"account\":{\"quality\":\"" + quality +
                "\",\"source\":\"ctp\",\"as_of_ms\":" + when + ",\"trading_day\":\"" + day +
                "\",\"data\":{\"balance\":" + std::to_string(balance) +
                ",\"available\":80000,\"curr_margin\":20000,\"frozen_margin\":0,\"frozen_cash\":0,"
                "\"frozen_commission\":0,\"commission\":10,\"close_profit\":20,\"position_profit\":"
                "30}},"
                "\"positions\":{\"quality\":\"" +
                quality + "\",\"source\":\"ctp\",\"as_of_ms\":" + when + ",\"trading_day\":\"" +
                day + "\",\"data\":" + positions + "}}");
    }
    void SnapshotV2(const std::string& positions, const std::string& quotes,
                    const std::string& risks, std::int64_t stamp = kNow,
                    const std::string& quote_quality = "ok",
                    const std::string& risk_quality = "ok", const std::string& day = "20260907") {
        const auto when = std::to_string(stamp);
        Write(root / "monitor/dashboard_private.json",
              "{\"schema_version\":2,\"identity\":" + std::string(kIdentity) +
                  ",\"generated_at_ms\":" + when + ",\"writer_started_at_ms\":" +
                  std::to_string(stamp - 1000) +
                  ",\"account\":{\"quality\":\"ok\",\"source\":\"ctp\",\"as_of_ms\":" + when +
                  ",\"trading_day\":\"" + day +
                  "\",\"data\":{\"balance\":100000,\"available\":80000,\"curr_margin\":"
                  "20000,\"frozen_margin\":0,\"frozen_cash\":0,\"frozen_commission\":0,"
                  "\"commission\":10,\"close_profit\":20,\"position_profit\":30}},"
                  "\"positions\":{\"quality\":\"ok\",\"source\":\"ctp\",\"as_of_ms\":" + when +
                  ",\"trading_day\":\"" + day + "\",\"data\":" + positions + "},"
                  "\"market_quotes\":{\"quality\":\"" + quote_quality +
                  "\",\"source\":\"ctp_market_callback\",\"as_of_ms\":" + when +
                  ",\"trading_day\":\"" + day + "\",\"data\":" + quotes + "},"
                  "\"strategy_risk\":{\"quality\":\"" + risk_quality +
                  "\",\"source\":\"strategy_engine\",\"as_of_ms\":" + when +
                  ",\"trading_day\":\"" + day + "\",\"data\":" + risks + "}}");
    }
    void Health(std::int64_t stamp = kNow, int version = 3) {
        Write(root / "monitor/readiness.json",
              "{\"schema_version\":2,\"heartbeat_ts_ns\":" + std::to_string(stamp * 1000000) +
                  ",\"mode\":\"Ready\",\"recovery_complete\":true,"
                  "\"trader_ready\":true,\"gateway_healthy\":true,\"settlement_confirmed\":true,"
                  "\"reasons\":[]}");
        Write(root / "monitor/pipeline_health.json",
              "{\"schema_version\":" + std::to_string(version) +
                  ",\"generated_epoch\":" + std::to_string(stamp / 1000) +
                  ",\"overall_status\":\"healthy\",\"session\":\"day\",\"trading_day\":"
                  "\"20260907\",\"products\":[]}");
    }
    J Current() const { return Parse(Read(out / "current.json")); }
    void Publish(Publisher& p, std::int64_t stamp = kNow) {
        std::string error;
        Check(p.PublishOnce(stamp, &error), "publish cycle: " + error);
    }
    J Archive(const std::string& kind, const std::string& day = "20260907") const {
        return Parse(Read(out / "days" / day / (kind + ".json")));
    }
};
std::string Event(int seq, const std::string& kind = "trade", const std::string& id = "T001",
                  int status = 3, const std::string& day = "20260907",
                  const std::string& account = "SECRET_ACCOUNT_8877") {
    return "{\"schema_version\":2,\"kind\":\"" + kind + "\",\"event_type\":\"" +
           (kind == "trade" ? "trade_fill" : "order_update") + "\",\"seq\":" + std::to_string(seq) +
           ",\"run_id\":\"run1\",\"account_id\":\"" + account + "\",\"trading_day\":\"" + day +
           "\",\"client_order_id\":\"SECRET_ACCOUNT_8877_order1\",\"exchange_order_id\":\"ex1\","
           "\"instrument_id\":\"hc2610\",\"exchange_id\":\"SHFE\",\"strategy_id\":\"kama_hc\","
           "\"trade_id\":\"" +
           id +
           "\",\"side\":0,\"offset\":0,\"total_volume\":3,\"filled_volume\":1,"
           "\"last_trade_volume\":1,\"avg_fill_price\":3210,\"status\":" +
           std::to_string(status) + ",\"reason\":\"/private/path password=secret\",\"ts_ns\":" +
           std::to_string((kNow + seq) * 1000000) + "}\n";
}
std::string ModernEvent(int seq, const std::string& id, const std::string& broker = "b-test",
                        const std::string& stream = "stream-1") {
    auto record = Event(seq, "trade", id);
    record.pop_back();
    record.replace(record.find("schema_version\":2") + 16, 1, "4");
    record.insert(1, "\"stream_id\":\"" + stream +
                         "\",\"stream_first_sequence\":1,\"broker_id\":\"" + broker + "\",");
    return record.substr(0, record.size() - 1) + ",\"checksum\":\"" +
           quant_hft::WalChecksumHex(quant_hft::WalChecksum(record)) + "\"}\n";
}

void IdentityAndPublicWhitelist() {
    Fixture f("identity");
    Write(f.root / "wal/events.wal", Event(1, "order", "", 5) + Event(2));
    Publisher p(f.options);
    f.Publish(p);
    auto current = f.Current();
    Check(Text(Field(current, "account"), "quality") == "fresh", "account fresh");
    for (const auto& entry : fs::recursive_directory_iterator(f.out))
        if (entry.is_regular_file()) {
            const auto text = Read(entry.path());
            Check(text.find("SECRET_ACCOUNT_8877") == std::string::npos,
                  "no real account in public output");
            Check(text.find("/private/path") == std::string::npos &&
                      text.find("password=") == std::string::npos,
                  "no raw log or reason leak");
            Check(text.find(f.root.string()) == std::string::npos, "no source path leak");
        }
    f.Snapshot(kNow, "ok", "[]",
               "{\"environment\":\"simnow\",\"broker_id\":\"wrong\",\"account_id\":\"SECRET_"
               "ACCOUNT_8877\",\"instance_id\":\"main\"}");
    f.Publish(p);
    current = f.Current();
    Check(Text(Field(current, "account"), "quality") == "invalid",
          "identity mismatch fails closed");
    Check(Field(Field(current, "positions"), "data").array_value.empty(),
          "mismatched account data suppressed");
}
void EmptyStaleAndFailedAreDistinct() {
    Fixture f("quality");
    f.Snapshot(kNow, "ok", "[]", kIdentity, "20260907", 0);
    Publisher p(f.options);
    f.Publish(p);
    auto current = f.Current();
    Check(Field(Field(current, "positions"), "data").IsArray(),
          "complete zero-row query means flat");
    Check(Field(Field(Field(current, "account"), "data"), "balance").number_value == 0,
          "zero is valid account value");
    f.Publish(p, kNow + 16000);
    current = f.Current();
    Check(Text(Field(current, "account"), "quality") == "stale",
          "old data never refreshed by publishing");
    f.Snapshot(kNow, "failed");
    f.Publish(p, kNow + 17000);
    Check(Text(Field(f.Current(), "positions"), "quality") == "incomplete",
          "failed batch distinct from flat");
    fs::remove(f.root / "monitor/dashboard_private.json");
    f.Publish(p);
    Check(Text(Field(f.Current(), "positions"), "quality") == "missing", "missing source not zero");
}
void FirstQueryFailureWithoutPriorDataRemainsIncomplete() {
    Fixture f("first-failure");
    Write(f.root / "monitor/dashboard_private.json",
          "{\"schema_version\":1,\"identity\":" + std::string(kIdentity) +
              ",\"writer_started_at_ms\":" + std::to_string(kNow - 1000) +
              ",\"account\":{\"quality\":\"failed\",\"source\":\"\",\"as_of_ms\":0,"
              "\"trading_day\":\"\",\"error_code\":\"query_failed\",\"data\":null},"
              "\"positions\":{\"quality\":\"failed\",\"source\":\"\",\"as_of_ms\":0,"
              "\"trading_day\":\"\",\"error_code\":\"query_failed\",\"data\":null}}");
    Publisher p(f.options);
    f.Publish(p);
    const auto current = f.Current();
    Check(Text(Field(current, "account"), "quality") == "incomplete",
          "first account query failure remains visible");
    Check(Text(Field(current, "positions"), "quality") == "incomplete",
          "first position query failure remains visible");
}
void WalPartialReplayAndRestart() {
    Fixture f("wal");
    auto second = Event(2, "trade", "T002");
    Write(f.root / "wal/events.wal", Event(1) + second.substr(0, second.size() / 2));
    {
        Publisher p(f.options);
        f.Publish(p);
        Check(Field(f.Archive("trades"), "data").array_value.size() == 1,
              "partial line not consumed");
    }
    Write(f.root / "wal/events.wal", second.substr(second.size() / 2) + Event(3, "trade", "T001"),
          true);
    {
        Publisher p(f.options);
        f.Publish(p);
        Check(Field(f.Archive("trades"), "data").array_value.size() == 2,
              "restart cursor and replay dedup");
        f.Publish(p);
        Check(Field(f.Archive("trades"), "data").array_value.size() == 2,
              "repeat cycle idempotent");
    }
}
void WalRotationDrainAndTruncation() {
    Fixture f("rotation");
    Write(f.root / "wal/events.wal", Event(1));
    Publisher p(f.options);
    f.Publish(p);
    Write(f.root / "wal/events.wal", Event(2, "trade", "T002"), true);
    fs::rename(f.root / "wal/events.wal", f.root / "wal/events.wal.1");
    Write(f.root / "wal/events.wal", Event(3, "trade", "T003") + Event(4, "trade", "T001"));
    f.Publish(p);
    Check(Field(f.Archive("trades"), "data").array_value.size() == 3,
          "rotation drains old inode before new");
    Write(f.root / "wal/events.wal", Event(5, "trade", "T004"));
    f.Publish(p);
    Check(Field(f.Archive("trades"), "data").array_value.size() == 4,
          "truncation resets only cursor not history");
    Check(Text(Field(f.Current(), "trades"), "quality") == "incomplete",
          "possible source gap visible");
}
void OrderMergeDoesNotRegressTerminalState() {
    Fixture f("orders");
    Write(f.root / "wal/events.wal",
          Event(1, "order", "", 1) + Event(2, "order", "", 3) + Event(3, "order", "", 1));
    Publisher p(f.options);
    f.Publish(p);
    const auto rows = Field(f.Archive("orders"), "data").array_value;
    Check(rows.size() == 1 && Text(rows.front(), "status") == "filled",
          "one current order and terminal state monotonic");
}
void EquityUsesSourceMinuteAndRetention() {
    Fixture f("equity");
    {
        Publisher p(f.options);
        f.Publish(p);
        f.Publish(p, kNow + 1000);
    }
    {
        Publisher p(f.options);
        f.Publish(p, kNow + 5000);
        Check(Field(f.Archive("equity"), "data").array_value.size() == 1,
              "one sample per source minute after restart");
        f.Publish(p, kNow + 60000);
        Check(Field(f.Archive("equity"), "data").array_value.size() == 1,
              "stale balance creates no invented sample");
        f.Snapshot(kNow + 60000);
        f.Publish(p, kNow + 60000);
        Check(Field(f.Archive("equity"), "data").array_value.size() == 2,
              "next fresh source minute sampled");
        const auto later = kNow + 94LL * 86400000;
        f.Snapshot(later, "ok", "[]", kIdentity, "20261210");
        f.Publish(p, later);
        Check(!fs::exists(f.out / "days/20260907/equity.json"), "expired public archive removed");
        Check(!fs::exists(f.state / "days/20260907.json"), "expired private archive removed");
    }
}
void HealthDoesNotFallBackFromInvalidV3() {
    Fixture f("health");
    Publisher p(f.options);
    f.Health(kNow, 2);
    f.Publish(p);
    auto health = Field(Field(f.Current(), "health"), "data");
    Check(Text(Field(health, "pipeline"), "quality") == "invalid",
          "legacy pipeline not authoritative");
    Check(Text(Field(health, "readiness"), "quality") == "fresh",
          "independent readiness freshness");
    f.Health(kNow - 20000);
    f.Publish(p);
    health = Field(Field(f.Current(), "health"), "data");
    Check(Text(Field(health, "pipeline"), "quality") == "stale", "old pipeline marked stale");
}
void PrivateV1RealtimeBlocksAreMissingAndLegacyFilesIgnored() {
    Fixture f("legacy-realtime");
    Write(f.root / "state/strategy_state__a__kama.json",
          "{\"account_id\":\"SECRET_ACCOUNT_8877\",\"strategy_id\":\"kama_hc\","
          "\"saved_epoch_seconds\":" +
              std::to_string(kNow / 1000) +
              ",\"state\":{\"net_pos.hc2610\":\"-2\"}}");
    Write(f.root / "monitor/pipeline_health.json",
          "{\"schema_version\":3,\"generated_epoch\":" + std::to_string(kNow / 1000) +
              ",\"overall_status\":\"healthy\",\"session\":\"day\","
              "\"trading_day\":\"20260907\",\"products\":[{\"product_id\":\"hc\","
              "\"instrument_id\":\"hc2610\",\"exchange_id\":\"SHFE\"}]}");
    Write(f.root / "market/trading_day=20260907/varieties/hc/market/ticks.csv",
          "instrument_id,exchange_id,trading_day,last_price,recv_ts_ns\n"
          "hc2610,SHFE,20260907,3210," +
              std::to_string(kNow * 1000000) + "\n");
    Publisher p(f.options);
    f.Publish(p);
    const auto current = f.Current();
    Check(Text(Field(current, "strategy_positions"), "quality") == "missing" &&
              Field(Field(current, "strategy_positions"), "data").array_value.empty(),
          "private v1 has no strategy-state fallback");
    Check(Text(Field(current, "markets"), "quality") == "missing" &&
              Field(Field(current, "markets"), "data").array_value.empty(),
          "private v1 has no recorded-tick fallback");
}

void PrivateV2RealtimeBlocksAreFilteredFreshAndNullable() {
    Fixture f("private-v2-realtime");
    const auto stamp = std::to_string(kNow);
    f.SnapshotV2(
        "[{\"instrument_id\":\"hc2610\",\"exchange_id\":\"SHFE\","
        "\"posi_direction\":\"3\",\"hedge_flag\":\"1\",\"position_date\":\"1\","
        "\"position\":2},{\"instrument_id\":\"rb2610\",\"exchange_id\":\"SHFE\","
        "\"posi_direction\":\"2\",\"hedge_flag\":\"1\",\"position_date\":\"1\","
        "\"position\":0}]",
        "[{\"product_id\":\"hc\",\"instrument_id\":\"hc2610\","
        "\"exchange_id\":\"SHFE\",\"trading_day\":\"20260907\",\"last_price\":3212,"
        "\"bid_price_1\":3211,\"ask_price_1\":3213,\"volume\":20,\"open_interest\":100,"
        "\"as_of_ms\":" +
            stamp +
            "},{\"instrument_id\":\"au2612\",\"exchange_id\":\"SHFE\","
            "\"trading_day\":\"20260907\",\"last_price\":null,\"bid_price_1\":null,"
            "\"ask_price_1\":null,\"volume\":null,\"open_interest\":null,\"as_of_ms\":" +
            stamp +
            "},{\"instrument_id\":\"rb2610\",\"exchange_id\":\"SHFE\","
            "\"trading_day\":\"20260907\",\"last_price\":3500,\"as_of_ms\":" + stamp +
            "},{\"instrument_id\":\"cu2610\",\"exchange_id\":\"SHFE\","
            "\"trading_day\":\"20260907\",\"last_price\":70000,\"as_of_ms\":" + stamp +
            "}]",
        "[{\"strategy_id\":\"outer_a\",\"owner_strategy_id\":\"kama_hc\","
        "\"instrument_id\":\"hc2610\",\"net\":-2,\"avg_open\":3210,"
        "\"initial_stop\":3235,\"trailing_stop\":3225,\"effective_stop\":3225,"
        "\"stop_kind\":\"trailing\",\"take_profit\":3150,\"as_of_ms\":" +
            stamp +
            "},{\"strategy_id\":\"outer_b\",\"owner_strategy_id\":\"trend_hc\","
            "\"instrument_id\":\"hc2610\",\"net\":1,\"avg_open\":3200,"
            "\"initial_stop\":3180,\"trailing_stop\":null,\"effective_stop\":3180,"
            "\"stop_kind\":\"initial\",\"take_profit\":null,\"as_of_ms\":" + stamp +
            "},{\"strategy_id\":\"outer_au\",\"owner_strategy_id\":\"trend_au\","
            "\"instrument_id\":\"au2612\",\"net\":1,\"avg_open\":null,"
            "\"initial_stop\":null,\"trailing_stop\":null,\"effective_stop\":null,"
            "\"stop_kind\":\"\",\"take_profit\":null,\"as_of_ms\":" + stamp + "}]");
    Publisher p(f.options);
    f.Publish(p);
    const auto current = f.Current();
    const auto strategy_block = Field(current, "strategy_positions");
    const auto strategies = Field(strategy_block, "data").array_value;
    Check(Text(strategy_block, "quality") == "fresh" &&
              Field(strategy_block, "stale_after_ms").number_value == 5000,
          "v2 strategy risk uses five-second freshness");
    Check(strategies.size() == 3, "multiple nonzero strategy owners remain separate");
    Check(Text(strategies[0], "strategy_id") == "outer_a" &&
              Text(strategies[0], "owner_strategy_id") == "kama_hc" &&
              Field(strategies[0], "effective_stop").number_value == 3225 &&
              Text(strategies[0], "stop_kind") == "trailing",
          "outer and owner strategy identity plus explicit effective stop preserved");
    Check(Field(strategies[1], "take_profit").IsNull() &&
              Field(strategies[2], "effective_stop").IsNull() &&
              Field(strategies[2], "stop_kind").IsNull(),
          "unknown strategy prices and stop kind remain null");
    const auto market_block = Field(current, "markets");
    const auto markets = Field(market_block, "data").array_value;
    Check(Text(market_block, "quality") == "fresh" && markets.size() == 2 &&
              Field(market_block, "stale_after_ms").number_value == 5000,
          "only nonzero broker or strategy instruments receive quotes");
    Check(Text(markets[0], "instrument_id") == "au2612" &&
              Field(markets[0], "last_price").IsNull() &&
              Text(markets[1], "instrument_id") == "hc2610" &&
              Field(markets[1], "last_price").number_value == 3212,
          "quote values remain exact and nullable");
    Check(Read(f.out / "current.json").find("cu2610") == std::string::npos,
          "unheld quote is not published");

    f.Publish(p, kNow + 5001);
    const auto stale = f.Current();
    Check(Text(Field(stale, "strategy_positions"), "quality") == "stale",
          "strategy risk expires after five seconds");
    Check(Text(Field(stale, "markets"), "quality") == "stale" &&
              Text(Field(Field(stale, "markets"), "data").array_value.front(), "quality") ==
                  "stale",
          "quotes expire after five seconds");
}
void SymlinksAndConcurrentPublisherRejected() {
    Fixture f("paths");
    Publisher p(f.options);
    f.Publish(p);
    Publisher second(f.options);
    std::string error;
    Check(!second.PublishOnce(kNow, &error), "one writer per state directory");
    fs::rename(f.root / "wal/events.wal", f.dir / "outside.wal");
    fs::create_symlink(f.dir / "outside.wal", f.root / "wal/events.wal");
    Check(!p.PublishOnce(kNow, &error), "swapped source symlink rejected");
}
void CrossAccountWalAndDuplicateConflictVisible() {
    Fixture f("conflict");
    auto changed = Event(3);
    const auto at = changed.find("3210");
    changed.replace(at, 4, "3211");
    Write(f.root / "wal/events.wal",
          Event(1) + Event(2, "trade", "FOREIGN", 3, "20260907", "other_account") + changed);
    Publisher p(f.options);
    f.Publish(p);
    const auto archive = f.Archive("trades");
    Check(Field(archive, "data").array_value.size() == 1,
          "foreign account excluded and conflicts never double counted");
    Check(Text(archive, "quality") == "incomplete", "conflicting replay visible in history");
}
void ConcurrentReadersSeeOnlyWholeFiles() {
    Fixture f("atomic");
    Publisher p(f.options);
    f.Publish(p);
    std::atomic<bool> done{false}, invalid{false};
    std::thread reader([&]() {
        while (!done.load()) {
            J parsed;
            std::string error;
            if (!quant_hft::simple_json::ParseStrict(Read(f.out / "current.json"), &parsed, &error))
                invalid = true;
        }
    });
    for (int i = 0; i < 20; ++i) f.Publish(p, kNow + i);
    done = true;
    reader.join();
    Check(!invalid, "readers never see partial JSON replacement");
}
void CompleteBrokerLongAndShortRemainSeparate() {
    Fixture f("broker-positions");
    f.Snapshot(
        kNow, "ok",
        "[{\"instrument_id\":\"hc2610\",\"exchange_id\":\"SHFE\",\"posi_direction\":\"2\","
        "\"hedge_flag\":\"1\",\"position_date\":\"1\",\"position\":2,\"today_position\":1,\"yd_"
        "position\":1,"
        "\"long_frozen\":0,\"short_frozen\":1},{\"instrument_id\":\"hc2610\",\"exchange_id\":"
        "\"SHFE\","
        "\"posi_direction\":\"3\",\"hedge_flag\":\"1\",\"position_date\":\"2\",\"position\":2,"
        "\"today_position\":0,\"yd_position\":2,\"long_frozen\":1,\"short_frozen\":0}]");
    Publisher p(f.options);
    f.Publish(p);
    const auto rows = Field(Field(f.Current(), "positions"), "data").array_value;
    Check(rows.size() == 2 && Text(rows[0], "posi_direction") != Text(rows[1], "posi_direction"),
          "equal opposite broker positions are not netted away");
    Check(Field(rows[1], "yd_position").number_value == 2, "yesterday bucket preserved");
}
void InvalidV2RealtimeValuesFailClosed() {
    Fixture f("invalid-v2-realtime");
    const auto stamp = std::to_string(kNow);
    f.SnapshotV2(
        "[{\"instrument_id\":\"hc2610\",\"exchange_id\":\"SHFE\","
        "\"posi_direction\":\"2\",\"hedge_flag\":\"1\",\"position_date\":\"1\","
        "\"position\":1}]",
        "[{\"instrument_id\":\"hc2610\",\"exchange_id\":\"SHFE\","
        "\"trading_day\":\"20260906\",\"last_price\":9999,\"as_of_ms\":" +
            stamp + "}]",
        "[{\"strategy_id\":\"outer_hc\",\"owner_strategy_id\":\"kama_hc\","
        "\"instrument_id\":\"hc2610\",\"net\":1,\"avg_open\":3210,"
        "\"initial_stop\":3190,\"trailing_stop\":3200,\"effective_stop\":3200,"
        "\"stop_kind\":\"calculated_by_web\",\"take_profit\":3300,\"as_of_ms\":" +
            stamp + "}]");
    Publisher p(f.options);
    f.Publish(p);
    const auto current = f.Current();
    const auto strategy = Field(current, "strategy_positions");
    Check(Text(strategy, "quality") == "invalid" &&
              Field(strategy, "data").array_value.empty(),
          "unrecognized stop ownership rejects the entire risk row");
    const auto market = Field(current, "markets");
    const auto& market_row = Field(market, "data").array_value.front();
    Check(Text(market, "quality") == "invalid" && Text(market_row, "quality") == "missing" &&
              Field(market_row, "last_price").IsNull(),
          "wrong-day quote is rejected and represented only by a missing placeholder");
}
void StrategyRiskRejectsInvalidNetsPricesAndStopOwnership() {
    Fixture f("invalid-v2-risk-boundaries");
    const auto stamp = std::to_string(kNow);
    f.SnapshotV2(
        "[]", "[]",
        "[{\"strategy_id\":\"mismatch\",\"instrument_id\":\"hc2610\",\"net\":1,"
        "\"initial_stop\":3190,\"effective_stop\":3191,\"stop_kind\":\"initial\","
        "\"as_of_ms\":" +
            stamp +
            "},{\"strategy_id\":\"missing_level\",\"instrument_id\":\"rb2610\","
            "\"net\":1,\"initial_stop\":null,\"effective_stop\":3190,"
            "\"stop_kind\":\"initial\",\"as_of_ms\":" +
            stamp +
            "},{\"strategy_id\":\"fractional_net\",\"instrument_id\":\"au2612\","
            "\"net\":1.5,\"effective_stop\":null,\"stop_kind\":\"\",\"as_of_ms\":" +
            stamp +
            "},{\"strategy_id\":\"negative_price\",\"instrument_id\":\"ag2612\","
            "\"net\":1,\"initial_stop\":-1,\"effective_stop\":-1,"
            "\"stop_kind\":\"initial\",\"as_of_ms\":" +
            stamp +
            "},{\"strategy_id\":\"zero_price\",\"instrument_id\":\"cu2610\","
            "\"net\":1,\"initial_stop\":0,\"effective_stop\":0,"
            "\"stop_kind\":\"initial\",\"as_of_ms\":" +
            stamp +
            "},{\"strategy_id\":\"huge_price\",\"instrument_id\":\"zn2610\","
            "\"net\":1,\"initial_stop\":1e308,\"effective_stop\":1e308,"
            "\"stop_kind\":\"initial\",\"as_of_ms\":" +
            stamp +
            "},{\"strategy_id\":\"negative_time\",\"instrument_id\":\"ni2610\","
            "\"net\":1,\"effective_stop\":null,\"stop_kind\":\"\",\"as_of_ms\":-1},"
            "{\"strategy_id\":\"zero_time\",\"instrument_id\":\"sn2610\","
            "\"net\":1,\"effective_stop\":null,\"stop_kind\":\"\",\"as_of_ms\":0},"
            "{\"strategy_id\":\"fractional_time\",\"instrument_id\":\"pb2610\","
            "\"net\":1,\"effective_stop\":null,\"stop_kind\":\"\","
            "\"as_of_ms\":1788746400000.5},{\"strategy_id\":\"huge_time\","
            "\"instrument_id\":\"ss2610\",\"net\":1,\"effective_stop\":null,"
            "\"stop_kind\":\"\",\"as_of_ms\":1e20},{\"strategy_id\":\"future_time\","
            "\"instrument_id\":\"sp2610\",\"net\":1,\"effective_stop\":null,"
            "\"stop_kind\":\"\",\"as_of_ms\":" +
            std::to_string(kNow + 3000) + "}]");
    Publisher p(f.options);
    f.Publish(p);
    const auto current = f.Current();
    const auto risk = Field(current, "strategy_positions");
    Check(Text(risk, "quality") == "invalid" && Field(risk, "data").array_value.empty(),
          "mismatched stops, invalid nets, prices and timestamps reject whole rows");
    const auto public_text = Read(f.out / "current.json");
    Check(public_text.find("1e+308") == std::string::npos &&
              public_text.find("huge_price") == std::string::npos,
          "rejected risk values never enter public output");
}

void NullStrategyRiskTimestampRemainsUnknown() {
    Fixture f("null-v2-risk-time");
    f.SnapshotV2(
        "[]", "[]",
        "[{\"strategy_id\":\"outer_hc\",\"owner_strategy_id\":\"kama_hc\","
        "\"instrument_id\":\"hc2610\",\"net\":1,\"avg_open\":3210,"
        "\"initial_stop\":3190,\"effective_stop\":3190,\"stop_kind\":\"initial\","
        "\"take_profit\":3300,\"as_of_ms\":null}]");
    Publisher p(f.options);
    f.Publish(p);
    const auto risk = Field(f.Current(), "strategy_positions");
    const auto& row = Field(risk, "data").array_value.front();
    Check(Text(risk, "quality") == "incomplete" && Field(row, "as_of_ms").IsNull(),
          "null strategy time remains unknown and degrades block quality");
}

void InvalidNewMarketRowsNeverReplaceOlderValidQuotes() {
    Fixture f("market-row-boundaries");
    const std::vector<std::pair<std::string, double>> instruments = {
        {"ag2612", 101}, {"al2610", 102}, {"au2612", 103}, {"cu2610", 104},
        {"hc2610", 105}, {"rb2610", 106}, {"zn2610", 107}};
    std::string positions = "[";
    std::string quotes = "[";
    for (std::size_t i = 0; i < instruments.size(); ++i) {
        if (i != 0) {
            positions += ',';
            quotes += ',';
        }
        positions += "{\"instrument_id\":\"" + instruments[i].first +
                     "\",\"exchange_id\":\"SHFE\",\"position\":1}";
        quotes += "{\"instrument_id\":\"" + instruments[i].first +
                  "\",\"exchange_id\":\"SHFE\",\"trading_day\":\"20260907\","
                  "\"last_price\":" +
                  std::to_string(instruments[i].second) +
                  ",\"volume\":10,\"open_interest\":100,\"as_of_ms\":" +
                  std::to_string(kNow - 1000) + "}";
    }
    positions += ']';
    quotes +=
        ",{\"instrument_id\":\"hc2610\",\"exchange_id\":\"SHFE\","
        "\"trading_day\":\"20260907\",\"last_price\":0,\"as_of_ms\":" +
        std::to_string(kNow) +
        "},{\"instrument_id\":\"au2612\",\"exchange_id\":\"SHFE\","
        "\"trading_day\":\"20260907\",\"last_price\":200,\"bid_price_1\":-1,"
        "\"as_of_ms\":" +
        std::to_string(kNow) +
        "},{\"instrument_id\":\"ag2612\",\"exchange_id\":\"SHFE\","
        "\"trading_day\":\"20260907\",\"last_price\":200,\"ask_price_1\":1e308,"
        "\"as_of_ms\":" +
        std::to_string(kNow) +
        "},{\"instrument_id\":\"cu2610\",\"exchange_id\":\"SHFE\","
        "\"trading_day\":\"20260907\",\"last_price\":200,\"volume\":1.5,"
        "\"as_of_ms\":" +
        std::to_string(kNow) +
        "},{\"instrument_id\":\"zn2610\",\"exchange_id\":\"SHFE\","
        "\"trading_day\":\"20260907\",\"last_price\":200,\"open_interest\":1e20,"
        "\"as_of_ms\":" +
        std::to_string(kNow) +
        "},{\"instrument_id\":\"al2610\",\"exchange_id\":\"SHFE\","
        "\"trading_day\":\"20260907\",\"last_price\":200,\"as_of_ms\":1788746400000.5},"
        "{\"instrument_id\":\"rb2610\",\"exchange_id\":\"SHFE\","
        "\"trading_day\":\"20260907\",\"last_price\":200,\"as_of_ms\":" +
        std::to_string(kNow + 3000) + "}]";
    f.SnapshotV2(positions, quotes, "[]");
    Publisher p(f.options);
    f.Publish(p);
    const auto market = Field(f.Current(), "markets");
    const auto rows = Field(market, "data").array_value;
    Check(Text(market, "quality") == "invalid" && rows.size() == instruments.size(),
          "invalid newer market rows mark the block invalid without deleting valid coverage");
    for (const auto& [instrument, price] : instruments) {
        const auto& row = InstrumentRow(rows, instrument);
        Check(Field(row, "last_price").number_value == price &&
                  Text(row, "quality") == "fresh" &&
                  Field(row, "as_of_ms").number_value == kNow - 1000,
              "older valid quote survives invalid replacement: " + instrument);
    }
}
void OversizedWalLinesStayBoundedAndResume() {
    Fixture f("oversized");
    Write(f.root / "wal/events.wal", std::string(9 * 1024 * 1024, 'x') + '\n' + Event(1));
    {
        Publisher p(f.options);
        f.Publish(p);
        Check(Text(Field(f.Current(), "trades"), "quality") == "catching_up",
              "per-cycle WAL work bounded");
    }
    Publisher p(f.options);
    f.Publish(p);
    Check(Field(f.Archive("trades"), "data").array_value.size() == 1,
          "discard cursor resumes after oversized line");
}
void RegrownWalAndOutputScopeAreChecked() {
    Fixture f("regrown");
    {
        Publisher p(f.options);
        Write(f.root / "wal/events.wal", Event(1));
        f.Publish(p);
        Write(f.root / "wal/events.wal", Event(1, "trade", "T002"));
        f.Publish(p);
        Check(Field(f.Archive("trades"), "data").array_value.size() == 2,
              "same-size truncation detected by cursor anchor");
        const auto current_scope = Text(f.Current(), "data_scope_id");
        Check(Text(f.Archive("trades"), "data_scope_id") == current_scope,
              "archive matches current random scope");
    }
    auto other = f.options;
    other.state_dir = (f.dir / "other-state").string();
    Publisher p(other);
    std::string error;
    Check(!p.PublishOnce(kNow, &error), "different state cannot adopt existing public output");
}
void OrderIdentityBridgeMergesRows() {
    Fixture f("order-bridge");
    auto client = Event(1, "order", "", 1);
    const std::string exchange_field = "\"exchange_order_id\":\"ex1\",";
    client.erase(client.find(exchange_field), exchange_field.size());
    auto exchange = Event(2, "order", "", 1);
    const std::string client_field = "\"client_order_id\":\"SECRET_ACCOUNT_8877_order1\",";
    exchange.erase(exchange.find(client_field), client_field.size());
    Write(f.root / "wal/events.wal", client + exchange + Event(3, "order", "", 3));
    Publisher p(f.options);
    f.Publish(p);
    Check(Field(f.Archive("orders"), "data").array_value.size() == 1,
          "later client-to-exchange callback coalesces early identities");
}
void ModernWalChecksumsBrokerAndSequenceGate() {
    Fixture f("modern");
    Write(f.root / "wal/events.wal", ModernEvent(1, "T001"));
    {
        Publisher p(f.options);
        f.Publish(p);
        Check(Text(Field(f.Current(), "trades"), "quality") == "fresh",
              "valid modern chain accepted");
        Check(Field(f.Archive("trades"), "data").array_value.size() == 1,
              "modern first event visible");
    }
    auto corrupt = ModernEvent(2, "T002");
    corrupt.replace(corrupt.find("3210"), 4, "9210");
    Write(f.root / "wal/events.wal",
          corrupt + ModernEvent(3, "T003") + ModernEvent(4, "T004") +
              ModernEvent(5, "T005", "other-broker") + ModernEvent(6, "T006") +
              ModernEvent(7, "T007", "b-test", "wrong-stream") + ModernEvent(7, "T007"),
          true);
    Publisher p(f.options);
    f.Publish(p);
    const auto archive = f.Archive("trades");
    Check(Field(archive, "data").array_value.size() == 3,
          "only CRC identity and sequence validated records admitted");
    Check(Text(archive, "quality") == "incomplete",
          "damaged or missing WAL coverage remains incomplete in archive");
    for (const auto& row : Field(archive, "data").array_value)
        Check(Field(row, "price").number_value == 3210, "corrupt price is never published");
}
void NewTradingDayDoesNotRebucketOldEquity() {
    Fixture f("roll-day");
    f.Snapshot(kNow - 20000, "ok", "[]", kIdentity, "20260904");
    auto snapshot = Read(f.root / "monitor/dashboard_private.json");
    const auto positions_at = snapshot.find("\"positions\"");
    auto positions = snapshot.substr(positions_at);
    positions.replace(positions.find("20260904"), 8, "20260907");
    const auto old_time = std::to_string(kNow - 20000);
    positions.replace(positions.find(old_time), old_time.size(), std::to_string(kNow));
    snapshot.replace(positions_at, snapshot.size() - positions_at, positions);
    Write(f.root / "monitor/dashboard_private.json", snapshot);
    Write(f.root / "wal/events.wal", ModernEvent(1, "T001"));
    Publisher p(f.options);
    f.Publish(p);
    Check(Text(f.Current(), "trading_day") == "20260907",
          "new broker position day selects new current events");
    Check(Field(Field(f.Current(), "trades"), "data").array_value.size() == 1,
          "current day trade not hidden by old account query");
    Check(Field(f.Archive("equity"), "data").array_value.empty(),
          "old account equity not moved into new day");
}
void OldProcessHeartbeatCannotMakeRestartReady() {
    Fixture f("restart-heartbeat");
    auto snapshot = Read(f.root / "monitor/dashboard_private.json");
    snapshot.insert(1, "\"writer_started_at_ms\":" + std::to_string(kNow + 1000) + ",");
    Write(f.root / "monitor/dashboard_private.json", snapshot);
    Publisher p(f.options);
    f.Publish(p, kNow + 2000);
    const auto health = Field(Field(f.Current(), "health"), "data");
    Check(Text(Field(health, "readiness"), "quality") == "stale",
          "previous process Ready is not fresh after restart");
    Check(Text(Field(health, "pipeline"), "quality") == "stale",
          "previous process pipeline not fresh after restart");
}
void RejectionsUseOnlyFixedReasonCategories() {
    Fixture f("rejections");
    Publisher p(f.options);
    int sequence = 0;
    const std::vector<std::pair<std::string, std::string>> reasons = {
        {"insufficient_margin sensitive-token", "risk_limit"},
        {"close_only sensitive-token", "opening_blocked"},
        {"rate_limit sensitive-token", "rate_limited"},
        {"invalid_price sensitive-token", "invalid_price_or_volume"},
        {"CTP: rejected sensitive-token", "ctp_rejected"},
        {"unrecognized sensitive-token", "unknown_rejection"}};
    for (const auto& [reason, code] : reasons) {
        auto event = Event(++sequence, "order", "", 5);
        const std::string original = "/private/path password=secret";
        event.replace(event.find(original), original.size(), reason);
        Write(f.root / "wal/events.wal", event, true);
        f.Publish(p);
        const auto current = f.Current();
        const auto& row = Field(Field(current, "orders"), "data").array_value.front();
        Check(Text(row, "reject_code") == code, "known fixed rejection category");
        Check(Read(f.out / "current.json").find("sensitive-token") == std::string::npos,
              "classified rejection never copies raw source text");
    }
}
}  // namespace

int main() {
    const std::vector<std::pair<std::string, std::function<void()>>> tests = {
        {"identity_whitelist", IdentityAndPublicWhitelist},
        {"source_quality", EmptyStaleAndFailedAreDistinct},
        {"first_query_failure", FirstQueryFailureWithoutPriorDataRemainsIncomplete},
        {"wal_partial_restart", WalPartialReplayAndRestart},
        {"wal_rotation_truncation", WalRotationDrainAndTruncation},
        {"order_terminal_merge", OrderMergeDoesNotRegressTerminalState},
        {"equity_retention", EquityUsesSourceMinuteAndRetention},
        {"pipeline_v3", HealthDoesNotFallBackFromInvalidV3},
        {"private_v1_realtime_missing", PrivateV1RealtimeBlocksAreMissingAndLegacyFilesIgnored},
        {"private_v2_realtime", PrivateV2RealtimeBlocksAreFilteredFreshAndNullable},
        {"path_writer_lock", SymlinksAndConcurrentPublisherRejected},
        {"trade_conflict", CrossAccountWalAndDuplicateConflictVisible},
        {"atomic_readers", ConcurrentReadersSeeOnlyWholeFiles},
        {"broker_long_short", CompleteBrokerLongAndShortRemainSeparate},
        {"invalid_v2_realtime", InvalidV2RealtimeValuesFailClosed},
        {"invalid_v2_risk_boundaries", StrategyRiskRejectsInvalidNetsPricesAndStopOwnership},
        {"null_v2_risk_time", NullStrategyRiskTimestampRemainsUnknown},
        {"invalid_v2_market_replacement", InvalidNewMarketRowsNeverReplaceOlderValidQuotes},
        {"wal_bounded", OversizedWalLinesStayBoundedAndResume},
        {"scope_regrown", RegrownWalAndOutputScopeAreChecked},
        {"order_identity_bridge", OrderIdentityBridgeMergesRows},
        {"modern_wal_validation", ModernWalChecksumsBrokerAndSequenceGate},
        {"trading_day_boundary", NewTradingDayDoesNotRebucketOldEquity},
        {"restart_heartbeat", OldProcessHeartbeatCannotMakeRestartReady},
        {"fixed_rejection_categories", RejectionsUseOnlyFixedReasonCategories}};
    for (const auto& [name, run] : tests) {
        try {
            run();
            std::cout << "PASS " << name << '\n';
        } catch (const std::exception& e) {
            std::cerr << "FAIL " << name << ": " << e.what() << '\n';
            return 1;
        }
    }
    return 0;
}
