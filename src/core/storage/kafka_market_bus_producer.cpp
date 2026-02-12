#include "quant_hft/core/kafka_market_bus_producer.h"

#include <algorithm>
#include <cstdio>
#include <sstream>
#include <utility>

namespace quant_hft {

namespace {

std::string EscapeJsonString(const std::string& input) {
    std::string out;
    out.reserve(input.size());
    for (const char ch : input) {
        switch (ch) {
            case '\\':
                out.append("\\\\");
                break;
            case '"':
                out.append("\\\"");
                break;
            case '\n':
                out.append("\\n");
                break;
            case '\r':
                out.append("\\r");
                break;
            case '\t':
                out.append("\\t");
                break;
            default:
                out.push_back(ch);
                break;
        }
    }
    return out;
}

}  // namespace

KafkaMarketBusProducer::KafkaMarketBusProducer(KafkaConnectionConfig config)
    : config_(std::move(config)) {}

bool KafkaMarketBusProducer::PublishMarketSnapshot(const MarketSnapshot& snapshot,
                                                   std::string* error) {
    const std::string command = BuildProducerCommand(error);
    if (command.empty()) {
        return false;
    }

    std::lock_guard<std::mutex> lock(publish_mutex_);
    FILE* pipe = popen(command.c_str(), "w");
    if (pipe == nullptr) {
        if (error != nullptr) {
            *error = "failed to open kafka producer command";
        }
        return false;
    }

    const std::string payload = SerializeMarketSnapshotJson(snapshot);
    const auto bytes_written = fwrite(payload.data(), 1, payload.size(), pipe);
    (void)fwrite("\n", 1, 1, pipe);
    const int rc = pclose(pipe);
    if (bytes_written != payload.size() || rc != 0) {
        if (error != nullptr) {
            *error = "kafka publish command failed";
        }
        return false;
    }
    return true;
}

bool KafkaMarketBusProducer::Flush(std::string* error) {
    (void)error;
    return true;
}

std::string KafkaMarketBusProducer::SerializeMarketSnapshotJson(const MarketSnapshot& snapshot) {
    std::ostringstream oss;
    oss << "{"
        << "\"instrument_id\":\"" << EscapeJsonString(snapshot.instrument_id) << "\","
        << "\"exchange_id\":\"" << EscapeJsonString(snapshot.exchange_id) << "\","
        << "\"trading_day\":\"" << EscapeJsonString(snapshot.trading_day) << "\","
        << "\"action_day\":\"" << EscapeJsonString(snapshot.action_day) << "\","
        << "\"update_time\":\"" << EscapeJsonString(snapshot.update_time) << "\","
        << "\"update_millisec\":" << snapshot.update_millisec << ","
        << "\"last_price\":" << snapshot.last_price << ","
        << "\"bid_price_1\":" << snapshot.bid_price_1 << ","
        << "\"ask_price_1\":" << snapshot.ask_price_1 << ","
        << "\"bid_volume_1\":" << snapshot.bid_volume_1 << ","
        << "\"ask_volume_1\":" << snapshot.ask_volume_1 << ","
        << "\"volume\":" << snapshot.volume << ","
        << "\"settlement_price\":" << snapshot.settlement_price << ","
        << "\"average_price_raw\":" << snapshot.average_price_raw << ","
        << "\"average_price_norm\":" << snapshot.average_price_norm << ","
        << "\"is_valid_settlement\":" << (snapshot.is_valid_settlement ? "true" : "false") << ","
        << "\"exchange_ts_ns\":" << snapshot.exchange_ts_ns << ","
        << "\"recv_ts_ns\":" << snapshot.recv_ts_ns
        << "}";
    return oss.str();
}

bool KafkaMarketBusProducer::IsSafeKafkaName(const std::string& text) {
    if (text.empty()) {
        return false;
    }
    return std::all_of(text.begin(), text.end(), [](unsigned char ch) {
        return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
               (ch >= '0' && ch <= '9') || ch == '.' || ch == '_' || ch == '-' || ch == ':' ||
               ch == ',';
    });
}

std::string KafkaMarketBusProducer::ReplaceAll(std::string text,
                                               const std::string& pattern,
                                               const std::string& replacement) {
    if (pattern.empty()) {
        return text;
    }
    std::size_t pos = 0;
    while ((pos = text.find(pattern, pos)) != std::string::npos) {
        text.replace(pos, pattern.size(), replacement);
        pos += replacement.size();
    }
    return text;
}

std::string KafkaMarketBusProducer::BuildProducerCommand(std::string* error) const {
    if (!IsSafeKafkaName(config_.brokers)) {
        if (error != nullptr) {
            *error = "invalid kafka brokers";
        }
        return "";
    }
    if (!IsSafeKafkaName(config_.market_topic)) {
        if (error != nullptr) {
            *error = "invalid kafka market topic";
        }
        return "";
    }
    if (config_.producer_command_template.empty()) {
        if (error != nullptr) {
            *error = "kafka producer command template is empty";
        }
        return "";
    }
    auto command = ReplaceAll(config_.producer_command_template, "{brokers}", config_.brokers);
    command = ReplaceAll(command, "{topic}", config_.market_topic);
    return command;
}

}  // namespace quant_hft
