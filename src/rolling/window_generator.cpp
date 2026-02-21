#include "quant_hft/rolling/window_generator.h"

#include <algorithm>
#include <cctype>
#include <limits>
#include <set>
#include <string>
#include <vector>

#include "quant_hft/backtest/parquet_data_feed.h"

namespace quant_hft::rolling {
namespace {

std::string NormalizeTradingDay(const std::string& raw) {
    std::string digits;
    digits.reserve(raw.size());
    for (char ch : raw) {
        if (std::isdigit(static_cast<unsigned char>(ch)) != 0) {
            digits.push_back(ch);
        }
    }
    if (digits.size() != 8) {
        return "";
    }
    return digits;
}

}  // namespace

bool BuildTradingDayCalendar(const RollingConfig& config,
                             std::vector<std::string>* trading_days,
                             std::string* error) {
    if (trading_days == nullptr) {
        if (error != nullptr) {
            *error = "trading day output is null";
        }
        return false;
    }
    trading_days->clear();

    ParquetDataFeed feed(config.backtest_base.dataset_root);
    if (!feed.LoadManifestJsonl(config.backtest_base.dataset_manifest, error)) {
        return false;
    }

    const auto partitions = feed.QueryPartitions(0,
                                                 std::numeric_limits<EpochNanos>::max(),
                                                 std::vector<std::string>{},
                                                 "");
    std::set<std::string> day_set;
    for (const auto& partition : partitions) {
        const std::string day = NormalizeTradingDay(partition.trading_day);
        if (!day.empty()) {
            day_set.insert(day);
        }
    }

    for (const std::string& day : day_set) {
        if (day < config.window.start_date || day > config.window.end_date) {
            continue;
        }
        trading_days->push_back(day);
    }

    if (trading_days->empty()) {
        if (error != nullptr) {
            *error = "no trading days found in requested range";
        }
        return false;
    }

    return true;
}

bool GenerateWindows(const RollingConfig& config,
                     const std::vector<std::string>& trading_days,
                     std::vector<Window>* windows,
                     std::string* error) {
    if (windows == nullptr) {
        if (error != nullptr) {
            *error = "window output is null";
        }
        return false;
    }
    windows->clear();

    if (trading_days.empty()) {
        if (error != nullptr) {
            *error = "trading day list is empty";
        }
        return false;
    }

    const int train_len = config.window.train_length_days;
    const int test_len = config.window.test_length_days;
    const int step = config.window.step_days;

    if (config.window.type == "rolling") {
        std::size_t index = 0;
        int window_index = 0;
        while (index < trading_days.size()) {
            const std::size_t train_begin = index;
            const std::size_t train_end_exclusive = train_begin + static_cast<std::size_t>(train_len);
            const std::size_t test_begin = train_end_exclusive;
            const std::size_t test_end_exclusive = test_begin + static_cast<std::size_t>(test_len);
            if (test_end_exclusive > trading_days.size()) {
                break;
            }

            Window window;
            window.index = window_index++;
            window.train_start = trading_days[train_begin];
            window.train_end = trading_days[train_end_exclusive - 1];
            window.test_start = trading_days[test_begin];
            window.test_end = trading_days[test_end_exclusive - 1];
            windows->push_back(std::move(window));

            index += static_cast<std::size_t>(step);
        }
    } else if (config.window.type == "expanding") {
        const int min_train = std::max(config.window.min_train_days, 1);
        int window_index = 0;
        for (std::size_t split = static_cast<std::size_t>(min_train); split < trading_days.size();
             split += static_cast<std::size_t>(step)) {
            const std::size_t test_begin = split;
            const std::size_t test_end_exclusive = test_begin + static_cast<std::size_t>(test_len);
            if (test_end_exclusive > trading_days.size()) {
                break;
            }
            Window window;
            window.index = window_index++;
            window.train_start = trading_days.front();
            window.train_end = trading_days[test_begin - 1];
            window.test_start = trading_days[test_begin];
            window.test_end = trading_days[test_end_exclusive - 1];
            windows->push_back(std::move(window));
        }
    } else {
        if (error != nullptr) {
            *error = "unsupported window type: " + config.window.type;
        }
        return false;
    }

    if (windows->empty()) {
        if (error != nullptr) {
            *error = "no complete windows generated";
        }
        return false;
    }

    return true;
}

}  // namespace quant_hft::rolling
