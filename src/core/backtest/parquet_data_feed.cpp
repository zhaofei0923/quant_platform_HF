#include "quant_hft/backtest/parquet_data_feed.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <utility>

#if QUANT_HFT_ENABLE_ARROW_PARQUET
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#endif

namespace quant_hft {
namespace {

std::vector<std::string> SplitCsvLine(const std::string& line) {
    std::vector<std::string> cells;
    std::string current;
    bool in_quotes = false;
    for (char ch : line) {
        if (ch == '"') {
            in_quotes = !in_quotes;
            continue;
        }
        if (ch == ',' && !in_quotes) {
            cells.push_back(current);
            current.clear();
            continue;
        }
        current.push_back(ch);
    }
    cells.push_back(current);
    return cells;
}

std::string ParsePartitionValue(const std::filesystem::path& path_segment,
                                const std::string& key_prefix) {
    const std::string text = path_segment.string();
    if (text.rfind(key_prefix, 0) != 0) {
        return "";
    }
    return text.substr(key_prefix.size());
}

void LoadMetaFile(const std::filesystem::path& meta_path, ParquetPartitionMeta* out) {
    std::ifstream input(meta_path);
    if (!input.is_open()) {
        return;
    }

    std::string line;
    while (std::getline(input, line)) {
        const std::size_t split_pos = line.find('=');
        if (split_pos == std::string::npos) {
            continue;
        }
        const std::string key = line.substr(0, split_pos);
        const std::string value = line.substr(split_pos + 1);
        try {
            if (key == "min_ts_ns") {
                out->min_ts_ns = static_cast<EpochNanos>(std::stoll(value));
            } else if (key == "max_ts_ns") {
                out->max_ts_ns = static_cast<EpochNanos>(std::stoll(value));
            } else if (key == "row_count") {
                out->row_count = static_cast<std::size_t>(std::stoull(value));
            }
        } catch (...) {
            continue;
        }
    }
}

Tick BuildTickFromValues(const std::vector<std::string>& headers,
                         const std::vector<std::string>& values,
                         const std::string& default_symbol) {
    std::unordered_map<std::string, std::string> row;
    for (std::size_t index = 0; index < headers.size() && index < values.size(); ++index) {
        row.emplace(headers[index], values[index]);
    }

    Tick tick;
    const auto symbol_it = row.find("symbol");
    tick.symbol = symbol_it != row.end() && !symbol_it->second.empty() ? symbol_it->second : default_symbol;

    const auto exchange_it = row.find("exchange");
    tick.exchange = exchange_it != row.end() ? exchange_it->second : "";

    const auto ts_it = row.find("ts_ns");
    tick.ts_ns = ts_it != row.end() ? static_cast<EpochNanos>(std::stoll(ts_it->second)) : 0;

    const auto last_price_it = row.find("last_price");
    tick.last_price = last_price_it != row.end() ? std::stod(last_price_it->second) : 0.0;

    const auto last_volume_it = row.find("last_volume");
    tick.last_volume = last_volume_it != row.end() ? std::stoi(last_volume_it->second) : 0;

    const auto bid_price1_it = row.find("bid_price1");
    tick.bid_price1 = bid_price1_it != row.end() ? std::stod(bid_price1_it->second) : 0.0;

    const auto bid_volume1_it = row.find("bid_volume1");
    tick.bid_volume1 = bid_volume1_it != row.end() ? std::stoi(bid_volume1_it->second) : 0;

    const auto ask_price1_it = row.find("ask_price1");
    tick.ask_price1 = ask_price1_it != row.end() ? std::stod(ask_price1_it->second) : 0.0;

    const auto ask_volume1_it = row.find("ask_volume1");
    tick.ask_volume1 = ask_volume1_it != row.end() ? std::stoi(ask_volume1_it->second) : 0;

    const auto volume_it = row.find("volume");
    tick.volume = volume_it != row.end() ? static_cast<std::int64_t>(std::stoll(volume_it->second)) : 0;

    const auto turnover_it = row.find("turnover");
    tick.turnover = turnover_it != row.end() ? std::stod(turnover_it->second) : 0.0;

    const auto open_interest_it = row.find("open_interest");
    tick.open_interest =
        open_interest_it != row.end() ? static_cast<std::int64_t>(std::stoll(open_interest_it->second)) : 0;

    return tick;
}

#if QUANT_HFT_ENABLE_ARROW_PARQUET
std::string ReadStringScalar(const std::shared_ptr<arrow::Scalar>& scalar) {
    if (scalar == nullptr || !scalar->is_valid) {
        return "";
    }
    if (scalar->type->id() == arrow::Type::STRING) {
        return std::static_pointer_cast<arrow::StringScalar>(scalar)->value->ToString();
    }
    return scalar->ToString();
}

double ReadDoubleScalar(const std::shared_ptr<arrow::Scalar>& scalar) {
    if (scalar == nullptr || !scalar->is_valid) {
        return 0.0;
    }
    switch (scalar->type->id()) {
        case arrow::Type::DOUBLE:
            return std::static_pointer_cast<arrow::DoubleScalar>(scalar)->value;
        case arrow::Type::FLOAT:
            return std::static_pointer_cast<arrow::FloatScalar>(scalar)->value;
        case arrow::Type::INT64:
            return static_cast<double>(std::static_pointer_cast<arrow::Int64Scalar>(scalar)->value);
        case arrow::Type::INT32:
            return static_cast<double>(std::static_pointer_cast<arrow::Int32Scalar>(scalar)->value);
        default:
            return 0.0;
    }
}

std::int64_t ReadInt64Scalar(const std::shared_ptr<arrow::Scalar>& scalar) {
    if (scalar == nullptr || !scalar->is_valid) {
        return 0;
    }
    switch (scalar->type->id()) {
        case arrow::Type::INT64:
            return std::static_pointer_cast<arrow::Int64Scalar>(scalar)->value;
        case arrow::Type::INT32:
            return std::static_pointer_cast<arrow::Int32Scalar>(scalar)->value;
        case arrow::Type::DOUBLE:
            return static_cast<std::int64_t>(std::static_pointer_cast<arrow::DoubleScalar>(scalar)->value);
        default:
            return 0;
    }
}

bool AppendTicksFromParquet(const std::filesystem::path& parquet_path,
                            const std::string& default_symbol,
                            const Timestamp& start,
                            const Timestamp& end,
                            std::vector<Tick>* out) {
    auto input_res = arrow::io::ReadableFile::Open(parquet_path.string());
    if (!input_res.ok()) {
        return false;
    }

    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto reader_status = parquet::arrow::OpenFile(*input_res.ValueOrDie(), arrow::default_memory_pool(), &reader);
    if (!reader_status.ok() || reader == nullptr) {
        return false;
    }

    std::shared_ptr<arrow::Table> table;
    auto table_status = reader->ReadTable(&table);
    if (!table_status.ok() || table == nullptr) {
        return false;
    }

    const auto* schema = table->schema().get();
    const int symbol_index = schema->GetFieldIndex("symbol");
    const int exchange_index = schema->GetFieldIndex("exchange");
    const int ts_index = schema->GetFieldIndex("ts_ns");
    const int last_price_index = schema->GetFieldIndex("last_price");
    const int last_volume_index = schema->GetFieldIndex("last_volume");
    const int bid_price_index = schema->GetFieldIndex("bid_price1");
    const int bid_volume_index = schema->GetFieldIndex("bid_volume1");
    const int ask_price_index = schema->GetFieldIndex("ask_price1");
    const int ask_volume_index = schema->GetFieldIndex("ask_volume1");
    const int volume_index = schema->GetFieldIndex("volume");
    const int turnover_index = schema->GetFieldIndex("turnover");
    const int open_interest_index = schema->GetFieldIndex("open_interest");

    if (ts_index < 0) {
        return false;
    }

    for (std::int64_t row = 0; row < table->num_rows(); ++row) {
        Tick tick;
        tick.symbol = default_symbol;

        auto get_scalar = [&](int index) -> std::shared_ptr<arrow::Scalar> {
            if (index < 0) {
                return nullptr;
            }
            auto scalar_result = table->column(index)->GetScalar(row);
            if (!scalar_result.ok()) {
                return nullptr;
            }
            return scalar_result.ValueOrDie();
        };

        if (symbol_index >= 0) {
            const std::string parsed_symbol = ReadStringScalar(get_scalar(symbol_index));
            if (!parsed_symbol.empty()) {
                tick.symbol = parsed_symbol;
            }
        }
        tick.exchange = ReadStringScalar(get_scalar(exchange_index));
        tick.ts_ns = ReadInt64Scalar(get_scalar(ts_index));
        tick.last_price = ReadDoubleScalar(get_scalar(last_price_index));
        tick.last_volume = static_cast<std::int32_t>(ReadInt64Scalar(get_scalar(last_volume_index)));
        tick.bid_price1 = ReadDoubleScalar(get_scalar(bid_price_index));
        tick.bid_volume1 = static_cast<std::int32_t>(ReadInt64Scalar(get_scalar(bid_volume_index)));
        tick.ask_price1 = ReadDoubleScalar(get_scalar(ask_price_index));
        tick.ask_volume1 = static_cast<std::int32_t>(ReadInt64Scalar(get_scalar(ask_volume_index)));
        tick.volume = ReadInt64Scalar(get_scalar(volume_index));
        tick.turnover = ReadDoubleScalar(get_scalar(turnover_index));
        tick.open_interest = ReadInt64Scalar(get_scalar(open_interest_index));

        if (tick.ts_ns < start.ToEpochNanos() || tick.ts_ns > end.ToEpochNanos()) {
            continue;
        }
        out->push_back(tick);
    }

    return true;
}
#endif

}  // namespace

ParquetDataFeed::ParquetDataFeed(std::string parquet_root) : parquet_root_(std::move(parquet_root)) {}

void ParquetDataFeed::SetParquetRoot(const std::string& parquet_root) {
    parquet_root_ = parquet_root;
}

bool ParquetDataFeed::RegisterPartition(const ParquetPartitionMeta& partition) {
    if (partition.file_path.empty()) {
        return false;
    }
    if (partition.min_ts_ns > 0 && partition.max_ts_ns > 0 && partition.min_ts_ns > partition.max_ts_ns) {
        return false;
    }
    partitions_.push_back(partition);
    return true;
}

std::vector<ParquetPartitionMeta> ParquetDataFeed::DiscoverFromDirectory(const std::string& root_path) const {
    std::vector<ParquetPartitionMeta> discovered;
    const std::filesystem::path root(root_path);
    if (!std::filesystem::exists(root)) {
        return discovered;
    }

    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        if (entry.path().extension() != ".parquet") {
            continue;
        }

        ParquetPartitionMeta meta;
        meta.file_path = entry.path().string();

        for (const auto& segment : entry.path()) {
            const std::string trading_day = ParsePartitionValue(segment, "trading_day=");
            if (!trading_day.empty()) {
                meta.trading_day = trading_day;
            }
            const std::string instrument = ParsePartitionValue(segment, "instrument_id=");
            if (!instrument.empty()) {
                meta.instrument_id = instrument;
            }
        }

        std::filesystem::path meta_file = entry.path();
        meta_file += ".meta";
        LoadMetaFile(meta_file, &meta);

        discovered.push_back(meta);
    }

    std::sort(discovered.begin(), discovered.end(), [](const ParquetPartitionMeta& left, const ParquetPartitionMeta& right) {
        if (left.min_ts_ns != right.min_ts_ns) {
            return left.min_ts_ns < right.min_ts_ns;
        }
        return left.file_path < right.file_path;
    });
    return discovered;
}

std::vector<ParquetPartitionMeta> ParquetDataFeed::QueryPartitions(EpochNanos start_ts_ns,
                                                                   EpochNanos end_ts_ns,
                                                                   const std::string& instrument_id) const {
    std::vector<ParquetPartitionMeta> filtered;
    if (start_ts_ns > end_ts_ns) {
        return filtered;
    }

    for (const auto& partition : partitions_) {
        if (!instrument_id.empty() && partition.instrument_id != instrument_id) {
            continue;
        }

        if (partition.min_ts_ns == 0 && partition.max_ts_ns == 0) {
            filtered.push_back(partition);
            continue;
        }

        const bool no_overlap = partition.max_ts_ns < start_ts_ns || partition.min_ts_ns > end_ts_ns;
        if (no_overlap) {
            continue;
        }
        filtered.push_back(partition);
    }

    std::sort(filtered.begin(), filtered.end(), [](const ParquetPartitionMeta& left, const ParquetPartitionMeta& right) {
        if (left.min_ts_ns != right.min_ts_ns) {
            return left.min_ts_ns < right.min_ts_ns;
        }
        return left.file_path < right.file_path;
    });
    return filtered;
}

std::vector<Tick> ParquetDataFeed::LoadTicks(const std::string& symbol,
                                             const Timestamp& start,
                                             const Timestamp& end) const {
    std::vector<Tick> ticks;
    if (start > end) {
        return ticks;
    }

    std::vector<ParquetPartitionMeta> source_partitions;
    if (partitions_.empty()) {
        source_partitions = DiscoverFromDirectory(parquet_root_);
    } else {
        source_partitions = partitions_;
    }

    const auto selected = [&]() {
        std::vector<ParquetPartitionMeta> result;
        for (const auto& partition : source_partitions) {
            if (!symbol.empty() && partition.instrument_id != symbol) {
                continue;
            }
            if (partition.min_ts_ns > 0 && partition.max_ts_ns > 0) {
                if (partition.max_ts_ns < start.ToEpochNanos() || partition.min_ts_ns > end.ToEpochNanos()) {
                    continue;
                }
            }
            result.push_back(partition);
        }
        return result;
    }();

    for (const auto& partition : selected) {
#if QUANT_HFT_ENABLE_ARROW_PARQUET
        if (AppendTicksFromParquet(partition.file_path, partition.instrument_id, start, end, &ticks)) {
            continue;
        }
#endif
        const std::filesystem::path ticks_sidecar = partition.file_path + ".ticks.csv";
        if (!std::filesystem::exists(ticks_sidecar)) {
            continue;
        }

        std::ifstream input(ticks_sidecar);
        if (!input.is_open()) {
            continue;
        }

        std::string line;
        if (!std::getline(input, line)) {
            continue;
        }
        const auto headers = SplitCsvLine(line);
        while (std::getline(input, line)) {
            if (line.empty()) {
                continue;
            }
            try {
                const auto values = SplitCsvLine(line);
                Tick tick = BuildTickFromValues(headers, values, partition.instrument_id);
                if (tick.ts_ns < start.ToEpochNanos() || tick.ts_ns > end.ToEpochNanos()) {
                    continue;
                }
                ticks.push_back(tick);
            } catch (...) {
                continue;
            }
        }
    }

    std::sort(ticks.begin(), ticks.end(), [](const Tick& left, const Tick& right) {
        return left.ts_ns < right.ts_ns;
    });
    return ticks;
}

std::size_t ParquetDataFeed::PartitionCount() const noexcept {
    return partitions_.size();
}

}  // namespace quant_hft
