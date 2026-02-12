#include "quant_hft/core/market_bus_producer.h"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <utility>

namespace quant_hft {
namespace {

std::string EscapeJson(std::string value) {
    std::string out;
    out.reserve(value.size());
    for (const char ch : value) {
        switch (ch) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                out += ch;
                break;
        }
    }
    return out;
}

std::string SanitizeFileComponent(std::string value) {
    for (char& ch : value) {
        if (!(ch == '-' || ch == '_' || ch == '.' ||
              (ch >= '0' && ch <= '9') ||
              (ch >= 'a' && ch <= 'z') ||
              (ch >= 'A' && ch <= 'Z'))) {
            ch = '_';
        }
    }
    if (value.empty()) {
        return "market_ticks";
    }
    return value;
}

}  // namespace

MarketBusProducer::MarketBusProducer(std::string bootstrap_servers,
                                     std::string topic,
                                     std::string spool_dir)
    : bootstrap_servers_(std::move(bootstrap_servers)),
      topic_(std::move(topic)),
      spool_dir_(std::move(spool_dir)) {}

MarketBusProducer::PublishResult MarketBusProducer::PublishTick(const MarketSnapshot& snapshot) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!Enabled()) {
        return PublishResult{true, "disabled"};
    }

    const auto spool_path = SpoolPathForTopic();
    std::error_code ec;
    std::filesystem::create_directories(std::filesystem::path(spool_path).parent_path(), ec);
    if (ec) {
        ++failed_count_;
        return PublishResult{false, "create_directories_failed:" + ec.message()};
    }

    std::ofstream out(spool_path, std::ios::app);
    if (!out.is_open()) {
        ++failed_count_;
        return PublishResult{false, "open_spool_file_failed:" + spool_path};
    }

    std::ostringstream payload;
    payload << "{"
            << "\"topic\":\"" << EscapeJson(topic_) << "\",";
    payload << "\"instrument_id\":\"" << EscapeJson(snapshot.instrument_id) << "\",";
    payload << "\"exchange_id\":\"" << EscapeJson(snapshot.exchange_id) << "\",";
    payload << "\"trading_day\":\"" << EscapeJson(snapshot.trading_day) << "\",";
    payload << "\"action_day\":\"" << EscapeJson(snapshot.action_day) << "\",";
    payload << "\"update_time\":\"" << EscapeJson(snapshot.update_time) << "\",";
    payload << "\"update_millisec\":" << snapshot.update_millisec << ",";
    payload << "\"last_price\":" << snapshot.last_price << ",";
    payload << "\"bid_price_1\":" << snapshot.bid_price_1 << ",";
    payload << "\"ask_price_1\":" << snapshot.ask_price_1 << ",";
    payload << "\"bid_volume_1\":" << snapshot.bid_volume_1 << ",";
    payload << "\"ask_volume_1\":" << snapshot.ask_volume_1 << ",";
    payload << "\"volume\":" << snapshot.volume << ",";
    payload << "\"exchange_ts_ns\":" << snapshot.exchange_ts_ns << ",";
    payload << "\"recv_ts_ns\":" << snapshot.recv_ts_ns << ",";
    payload << "\"published_ts_ns\":" << NowEpochNanos();
    payload << "}";

    out << payload.str() << '\n';
    out.flush();
    if (!out.good()) {
        ++failed_count_;
        return PublishResult{false, "write_spool_file_failed:" + spool_path};
    }

    ++published_count_;
    return PublishResult{true, "ok"};
}

bool MarketBusProducer::Enabled() const {
    return !bootstrap_servers_.empty() && !topic_.empty() && !spool_dir_.empty();
}

std::uint64_t MarketBusProducer::PublishedCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return published_count_;
}

std::uint64_t MarketBusProducer::FailedCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return failed_count_;
}

std::string MarketBusProducer::SpoolPathForTopic() const {
    return spool_dir_ + "/" + SanitizeFileComponent(topic_) + ".jsonl";
}

}  // namespace quant_hft
