#include "quant_hft/backtest/parquet_data_feed.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

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

std::string Trim(std::string text) {
    const auto is_space = [](unsigned char ch) { return std::isspace(ch) != 0; };
    while (!text.empty() && is_space(static_cast<unsigned char>(text.front()))) {
        text.erase(text.begin());
    }
    while (!text.empty() && is_space(static_cast<unsigned char>(text.back()))) {
        text.pop_back();
    }
    return text;
}

std::string ParsePartitionValue(const std::filesystem::path& path_segment,
                                const std::string& key_prefix) {
    const std::string text = path_segment.string();
    if (text.rfind(key_prefix, 0) != 0) {
        return "";
    }
    return text.substr(key_prefix.size());
}

std::int64_t SafeFileSize(const std::filesystem::path& path) {
    std::error_code ec;
    const auto bytes = std::filesystem::file_size(path, ec);
    if (ec) {
        return 0;
    }
    return static_cast<std::int64_t>(bytes);
}

bool ParseInt64(const std::string& raw, std::int64_t* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string value = Trim(raw);
    if (value.empty()) {
        return false;
    }
    try {
        std::size_t parsed = 0;
        const std::int64_t number = std::stoll(value, &parsed);
        if (parsed != value.size()) {
            return false;
        }
        *out = number;
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseSize(const std::string& raw, std::size_t* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string value = Trim(raw);
    if (value.empty()) {
        return false;
    }
    try {
        std::size_t parsed = 0;
        const auto number = std::stoull(value, &parsed);
        if (parsed != value.size()) {
            return false;
        }
        *out = static_cast<std::size_t>(number);
        return true;
    } catch (...) {
        return false;
    }
}

bool ExtractJsonString(const std::string& json, const std::string& key, std::string* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string quoted_key = "\"" + key + "\"";
    const std::size_t key_pos = json.find(quoted_key);
    if (key_pos == std::string::npos) {
        return false;
    }
    const std::size_t colon_pos = json.find(':', key_pos + quoted_key.size());
    if (colon_pos == std::string::npos) {
        return false;
    }

    std::size_t pos = colon_pos + 1;
    while (pos < json.size() && std::isspace(static_cast<unsigned char>(json[pos])) != 0) {
        ++pos;
    }
    if (pos >= json.size() || json[pos] != '"') {
        return false;
    }
    ++pos;

    std::string value;
    bool escaped = false;
    while (pos < json.size()) {
        const char ch = json[pos++];
        if (escaped) {
            switch (ch) {
                case '"':
                    value.push_back('"');
                    break;
                case '\\':
                    value.push_back('\\');
                    break;
                case 'n':
                    value.push_back('\n');
                    break;
                case 'r':
                    value.push_back('\r');
                    break;
                case 't':
                    value.push_back('\t');
                    break;
                default:
                    value.push_back(ch);
                    break;
            }
            escaped = false;
            continue;
        }
        if (ch == '\\') {
            escaped = true;
            continue;
        }
        if (ch == '"') {
            *out = value;
            return true;
        }
        value.push_back(ch);
    }
    return false;
}

bool ExtractJsonInt64(const std::string& json, const std::string& key, std::int64_t* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string quoted_key = "\"" + key + "\"";
    const std::size_t key_pos = json.find(quoted_key);
    if (key_pos == std::string::npos) {
        return false;
    }
    const std::size_t colon_pos = json.find(':', key_pos + quoted_key.size());
    if (colon_pos == std::string::npos) {
        return false;
    }
    std::size_t start = colon_pos + 1;
    while (start < json.size() && std::isspace(static_cast<unsigned char>(json[start])) != 0) {
        ++start;
    }
    std::size_t end = start;
    while (end < json.size() &&
           (std::isdigit(static_cast<unsigned char>(json[end])) != 0 || json[end] == '-')) {
        ++end;
    }
    if (end <= start) {
        return false;
    }
    return ParseInt64(json.substr(start, end - start), out);
}

void LoadMetaFile(const std::filesystem::path& meta_path, ParquetPartitionMeta* out) {
    if (out == nullptr) {
        return;
    }

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
        const std::string key = Trim(line.substr(0, split_pos));
        const std::string value = Trim(line.substr(split_pos + 1));
        if (key == "min_ts_ns") {
            std::int64_t parsed = 0;
            if (ParseInt64(value, &parsed)) {
                out->min_ts_ns = static_cast<EpochNanos>(parsed);
            }
            continue;
        }
        if (key == "max_ts_ns") {
            std::int64_t parsed = 0;
            if (ParseInt64(value, &parsed)) {
                out->max_ts_ns = static_cast<EpochNanos>(parsed);
            }
            continue;
        }
        if (key == "row_count") {
            std::size_t parsed = 0;
            if (ParseSize(value, &parsed)) {
                out->row_count = parsed;
            }
            continue;
        }
        if (key == "schema_version") {
            out->schema_version = value;
            continue;
        }
        if (key == "source_csv_fingerprint") {
            out->source_csv_fingerprint = value;
            continue;
        }
        if (key == "source") {
            out->source = value;
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
    tick.symbol =
        symbol_it != row.end() && !symbol_it->second.empty() ? symbol_it->second : default_symbol;

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
    tick.volume =
        volume_it != row.end() ? static_cast<std::int64_t>(std::stoll(volume_it->second)) : 0;

    const auto turnover_it = row.find("turnover");
    tick.turnover = turnover_it != row.end() ? std::stod(turnover_it->second) : 0.0;

    const auto open_interest_it = row.find("open_interest");
    tick.open_interest = open_interest_it != row.end()
                             ? static_cast<std::int64_t>(std::stoll(open_interest_it->second))
                             : 0;

    return tick;
}

#if QUANT_HFT_ENABLE_ARROW_PARQUET
std::string ReadStringArrayValue(const std::shared_ptr<arrow::Array>& values, std::int64_t row) {
    if (values == nullptr || row < 0 || row >= values->length() || values->IsNull(row)) {
        return "";
    }
    if (values->type_id() == arrow::Type::STRING) {
        const auto& array = static_cast<const arrow::StringArray&>(*values);
        return array.GetString(row);
    }
    auto scalar_result = values->GetScalar(row);
    if (!scalar_result.ok()) {
        return "";
    }
    return scalar_result.ValueOrDie()->ToString();
}

double ReadDoubleArrayValue(const std::shared_ptr<arrow::Array>& values, std::int64_t row) {
    if (values == nullptr || row < 0 || row >= values->length() || values->IsNull(row)) {
        return 0.0;
    }
    switch (values->type_id()) {
        case arrow::Type::DOUBLE:
            return static_cast<const arrow::DoubleArray&>(*values).Value(row);
        case arrow::Type::FLOAT:
            return static_cast<const arrow::FloatArray&>(*values).Value(row);
        case arrow::Type::INT64:
            return static_cast<double>(static_cast<const arrow::Int64Array&>(*values).Value(row));
        case arrow::Type::INT32:
            return static_cast<double>(static_cast<const arrow::Int32Array&>(*values).Value(row));
        default:
            return 0.0;
    }
}

std::int64_t ReadInt64ArrayValue(const std::shared_ptr<arrow::Array>& values, std::int64_t row) {
    if (values == nullptr || row < 0 || row >= values->length() || values->IsNull(row)) {
        return 0;
    }
    switch (values->type_id()) {
        case arrow::Type::INT64:
            return static_cast<const arrow::Int64Array&>(*values).Value(row);
        case arrow::Type::INT32:
            return static_cast<const arrow::Int32Array&>(*values).Value(row);
        case arrow::Type::DOUBLE:
            return static_cast<std::int64_t>(
                static_cast<const arrow::DoubleArray&>(*values).Value(row));
        case arrow::Type::FLOAT:
            return static_cast<std::int64_t>(
                static_cast<const arrow::FloatArray&>(*values).Value(row));
        default:
            return 0;
    }
}

bool AppendTicksFromParquet(const std::filesystem::path& parquet_path,
                            const std::string& default_symbol, const Timestamp& start,
                            const Timestamp& end, std::vector<Tick>* out,
                            ParquetScanMetrics* metrics, std::int64_t max_ticks,
                            std::string* error) {
    if (out == nullptr) {
        return false;
    }
    if (max_ticks == 0) {
        if (metrics != nullptr) {
            metrics->early_stop_hit = true;
        }
        return true;
    }

    auto input_res = arrow::io::ReadableFile::Open(parquet_path.string());
    if (!input_res.ok()) {
        return false;
    }

    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto reader_status =
        parquet::arrow::OpenFile(input_res.ValueOrDie(), arrow::default_memory_pool(), &reader);
    if (!reader_status.ok() || reader == nullptr) {
        return false;
    }

    std::shared_ptr<arrow::Table> table;
    auto table_status = reader->ReadTable(&table);
    if (!table_status.ok() || table == nullptr) {
        return false;
    }

    if (metrics != nullptr) {
        metrics->io_bytes += SafeFileSize(parquet_path);
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
        if (error != nullptr) {
            *error = "parquet missing required column: ts_ns";
        }
        return false;
    }

    arrow::TableBatchReader batch_reader(*table);
    std::shared_ptr<arrow::RecordBatch> batch;
    while (true) {
        auto batch_status = batch_reader.ReadNext(&batch);
        if (!batch_status.ok()) {
            if (error != nullptr) {
                *error = batch_status.ToString();
            }
            return false;
        }
        if (batch == nullptr) {
            break;
        }

        if (metrics != nullptr) {
            metrics->scan_row_groups += 1;
        }

        const auto get_column = [&](int index) -> std::shared_ptr<arrow::Array> {
            if (index < 0 || index >= batch->num_columns()) {
                return nullptr;
            }
            return batch->column(index);
        };

        const std::shared_ptr<arrow::Array> symbol_column = get_column(symbol_index);
        const std::shared_ptr<arrow::Array> exchange_column = get_column(exchange_index);
        const std::shared_ptr<arrow::Array> ts_column = get_column(ts_index);
        const std::shared_ptr<arrow::Array> last_price_column = get_column(last_price_index);
        const std::shared_ptr<arrow::Array> last_volume_column = get_column(last_volume_index);
        const std::shared_ptr<arrow::Array> bid_price_column = get_column(bid_price_index);
        const std::shared_ptr<arrow::Array> bid_volume_column = get_column(bid_volume_index);
        const std::shared_ptr<arrow::Array> ask_price_column = get_column(ask_price_index);
        const std::shared_ptr<arrow::Array> ask_volume_column = get_column(ask_volume_index);
        const std::shared_ptr<arrow::Array> volume_column = get_column(volume_index);
        const std::shared_ptr<arrow::Array> turnover_column = get_column(turnover_index);
        const std::shared_ptr<arrow::Array> open_interest_column = get_column(open_interest_index);

        for (std::int64_t row = 0; row < batch->num_rows(); ++row) {
            if (metrics != nullptr) {
                metrics->scan_rows += 1;
            }

            Tick tick;
            tick.symbol = default_symbol;

            if (symbol_column != nullptr) {
                const std::string parsed_symbol = ReadStringArrayValue(symbol_column, row);
                if (!parsed_symbol.empty()) {
                    tick.symbol = parsed_symbol;
                }
            }

            tick.exchange = ReadStringArrayValue(exchange_column, row);
            tick.ts_ns = ReadInt64ArrayValue(ts_column, row);
            tick.last_price = ReadDoubleArrayValue(last_price_column, row);
            tick.last_volume =
                static_cast<std::int32_t>(ReadInt64ArrayValue(last_volume_column, row));
            tick.bid_price1 = ReadDoubleArrayValue(bid_price_column, row);
            tick.bid_volume1 =
                static_cast<std::int32_t>(ReadInt64ArrayValue(bid_volume_column, row));
            tick.ask_price1 = ReadDoubleArrayValue(ask_price_column, row);
            tick.ask_volume1 =
                static_cast<std::int32_t>(ReadInt64ArrayValue(ask_volume_column, row));
            tick.volume = ReadInt64ArrayValue(volume_column, row);
            tick.turnover = ReadDoubleArrayValue(turnover_column, row);
            tick.open_interest = ReadInt64ArrayValue(open_interest_column, row);

            if (tick.ts_ns < start.ToEpochNanos() || tick.ts_ns > end.ToEpochNanos()) {
                continue;
            }
            out->push_back(tick);
            if (max_ticks > 0 && static_cast<std::int64_t>(out->size()) >= max_ticks) {
                if (metrics != nullptr) {
                    metrics->early_stop_hit = true;
                }
                return true;
            }
        }
    }

    return true;
}
#endif

bool LoadTicksFromSidecar(const ParquetPartitionMeta& partition, const Timestamp& start,
                          const Timestamp& end, std::vector<Tick>* out, ParquetScanMetrics* metrics,
                          std::int64_t max_ticks, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "tick output is null";
        }
        return false;
    }
    if (max_ticks == 0) {
        if (metrics != nullptr) {
            metrics->early_stop_hit = true;
        }
        return true;
    }

    const std::filesystem::path ticks_sidecar = partition.file_path + ".ticks.csv";
    if (!std::filesystem::exists(ticks_sidecar)) {
        if (error != nullptr) {
            *error = "ticks sidecar missing: " + ticks_sidecar.string();
        }
        return false;
    }

    std::ifstream input(ticks_sidecar);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open ticks sidecar: " + ticks_sidecar.string();
        }
        return false;
    }

    if (metrics != nullptr) {
        metrics->io_bytes += SafeFileSize(ticks_sidecar);
        metrics->scan_row_groups += 1;
    }

    std::string line;
    if (!std::getline(input, line)) {
        if (error != nullptr) {
            *error = "ticks sidecar is empty: " + ticks_sidecar.string();
        }
        return false;
    }
    const auto headers = SplitCsvLine(line);

    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }
        try {
            const auto values = SplitCsvLine(line);
            Tick tick = BuildTickFromValues(headers, values, partition.instrument_id);
            if (metrics != nullptr) {
                metrics->scan_rows += 1;
            }
            if (tick.ts_ns < start.ToEpochNanos() || tick.ts_ns > end.ToEpochNanos()) {
                continue;
            }
            out->push_back(tick);
            if (max_ticks > 0 && static_cast<std::int64_t>(out->size()) >= max_ticks) {
                if (metrics != nullptr) {
                    metrics->early_stop_hit = true;
                }
                return true;
            }
        } catch (...) {
            continue;
        }
    }

    return true;
}

}  // namespace

ParquetDataFeed::ParquetDataFeed(std::string parquet_root)
    : parquet_root_(std::move(parquet_root)) {}

void ParquetDataFeed::SetParquetRoot(const std::string& parquet_root) {
    parquet_root_ = parquet_root;
}

bool ParquetDataFeed::RegisterPartition(const ParquetPartitionMeta& partition) {
    if (partition.file_path.empty()) {
        return false;
    }
    if (partition.min_ts_ns > 0 && partition.max_ts_ns > 0 &&
        partition.min_ts_ns > partition.max_ts_ns) {
        return false;
    }
    partitions_.push_back(partition);
    return true;
}

bool ParquetDataFeed::LoadManifestJsonl(const std::string& manifest_path, std::string* error) {
    std::ifstream input(manifest_path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open manifest: " + manifest_path;
        }
        return false;
    }

    partitions_.clear();
    std::string line;
    const std::filesystem::path manifest_dir = std::filesystem::path(manifest_path).parent_path();
    const std::filesystem::path root = manifest_dir.parent_path();

    while (std::getline(input, line)) {
        line = Trim(line);
        if (line.empty()) {
            continue;
        }

        ParquetPartitionMeta meta;
        std::string file_path;
        if (!ExtractJsonString(line, "file_path", &file_path)) {
            if (error != nullptr) {
                *error = "manifest line missing file_path";
            }
            return false;
        }

        std::filesystem::path parsed(file_path);
        if (parsed.is_relative()) {
            parsed = root / parsed;
        }
        meta.file_path = parsed.lexically_normal().string();
        ExtractJsonString(line, "source", &meta.source);
        ExtractJsonString(line, "trading_day", &meta.trading_day);
        ExtractJsonString(line, "instrument_id", &meta.instrument_id);
        ExtractJsonString(line, "schema_version", &meta.schema_version);
        ExtractJsonString(line, "source_csv_fingerprint", &meta.source_csv_fingerprint);

        std::int64_t parsed_int = 0;
        if (ExtractJsonInt64(line, "min_ts_ns", &parsed_int)) {
            meta.min_ts_ns = static_cast<EpochNanos>(parsed_int);
        }
        if (ExtractJsonInt64(line, "max_ts_ns", &parsed_int)) {
            meta.max_ts_ns = static_cast<EpochNanos>(parsed_int);
        }
        if (ExtractJsonInt64(line, "row_count", &parsed_int) && parsed_int >= 0) {
            meta.row_count = static_cast<std::size_t>(parsed_int);
        }

        if (meta.source.empty()) {
            for (const auto& segment : parsed) {
                const std::string value = ParsePartitionValue(segment, "source=");
                if (!value.empty()) {
                    meta.source = value;
                }
            }
        }

        if (!RegisterPartition(meta)) {
            if (error != nullptr) {
                *error = "invalid partition in manifest: " + meta.file_path;
            }
            return false;
        }
    }

    std::sort(partitions_.begin(), partitions_.end(),
              [](const ParquetPartitionMeta& left, const ParquetPartitionMeta& right) {
                  if (left.min_ts_ns != right.min_ts_ns) {
                      return left.min_ts_ns < right.min_ts_ns;
                  }
                  return left.file_path < right.file_path;
              });
    return true;
}

std::vector<ParquetPartitionMeta> ParquetDataFeed::DiscoverFromDirectory(
    const std::string& root_path) const {
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
            const std::string source = ParsePartitionValue(segment, "source=");
            if (!source.empty()) {
                meta.source = source;
            }
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

    std::sort(discovered.begin(), discovered.end(),
              [](const ParquetPartitionMeta& left, const ParquetPartitionMeta& right) {
                  if (left.min_ts_ns != right.min_ts_ns) {
                      return left.min_ts_ns < right.min_ts_ns;
                  }
                  return left.file_path < right.file_path;
              });
    return discovered;
}

std::vector<ParquetPartitionMeta> ParquetDataFeed::QueryPartitions(
    EpochNanos start_ts_ns, EpochNanos end_ts_ns, const std::string& instrument_id) const {
    std::vector<std::string> instruments;
    if (!instrument_id.empty()) {
        instruments.push_back(instrument_id);
    }
    return QueryPartitions(start_ts_ns, end_ts_ns, instruments, "");
}

std::vector<ParquetPartitionMeta> ParquetDataFeed::QueryPartitions(
    EpochNanos start_ts_ns, EpochNanos end_ts_ns, const std::vector<std::string>& instrument_ids,
    const std::string& source) const {
    std::vector<ParquetPartitionMeta> filtered;
    if (start_ts_ns > end_ts_ns) {
        return filtered;
    }

    const std::vector<ParquetPartitionMeta> source_partitions =
        partitions_.empty() ? DiscoverFromDirectory(parquet_root_) : partitions_;

    std::unordered_set<std::string> instrument_set;
    for (const std::string& instrument_id : instrument_ids) {
        if (!instrument_id.empty()) {
            instrument_set.insert(instrument_id);
        }
    }

    for (const auto& partition : source_partitions) {
        if (!source.empty() && partition.source != source) {
            continue;
        }
        if (!instrument_set.empty() &&
            instrument_set.find(partition.instrument_id) == instrument_set.end()) {
            continue;
        }

        if (partition.min_ts_ns == 0 && partition.max_ts_ns == 0) {
            filtered.push_back(partition);
            continue;
        }

        const bool no_overlap =
            partition.max_ts_ns < start_ts_ns || partition.min_ts_ns > end_ts_ns;
        if (no_overlap) {
            continue;
        }
        filtered.push_back(partition);
    }

    std::sort(filtered.begin(), filtered.end(),
              [](const ParquetPartitionMeta& left, const ParquetPartitionMeta& right) {
                  if (left.min_ts_ns != right.min_ts_ns) {
                      return left.min_ts_ns < right.min_ts_ns;
                  }
                  return left.file_path < right.file_path;
              });
    return filtered;
}

bool ParquetDataFeed::LoadPartitionTicks(const ParquetPartitionMeta& partition,
                                         const Timestamp& start, const Timestamp& end,
                                         const std::vector<std::string>& /*projected_columns*/,
                                         std::vector<Tick>* out, ParquetScanMetrics* metrics,
                                         std::int64_t max_ticks, std::string* error) const {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "partition tick output is null";
        }
        return false;
    }
    out->clear();

    if (partition.min_ts_ns > 0 && partition.max_ts_ns > 0) {
        if (partition.max_ts_ns < start.ToEpochNanos() ||
            partition.min_ts_ns > end.ToEpochNanos()) {
            return true;
        }
    }

#if QUANT_HFT_ENABLE_ARROW_PARQUET
    if (AppendTicksFromParquet(partition.file_path, partition.instrument_id, start, end, out,
                               metrics, max_ticks, nullptr)) {
        std::sort(out->begin(), out->end(), [](const Tick& left, const Tick& right) {
            if (left.ts_ns != right.ts_ns) {
                return left.ts_ns < right.ts_ns;
            }
            return left.symbol < right.symbol;
        });
        return true;
    }
#endif

    if (!LoadTicksFromSidecar(partition, start, end, out, metrics, max_ticks, error)) {
        return false;
    }

    std::sort(out->begin(), out->end(), [](const Tick& left, const Tick& right) {
        if (left.ts_ns != right.ts_ns) {
            return left.ts_ns < right.ts_ns;
        }
        return left.symbol < right.symbol;
    });
    return true;
}

std::vector<Tick> ParquetDataFeed::LoadTicks(const std::string& symbol, const Timestamp& start,
                                             const Timestamp& end) const {
    std::vector<Tick> ticks;
    if (start > end) {
        return ticks;
    }

    std::vector<std::string> symbols;
    if (!symbol.empty()) {
        symbols.push_back(symbol);
    }

    const auto selected = QueryPartitions(start.ToEpochNanos(), end.ToEpochNanos(), symbols, "");
    for (const auto& partition : selected) {
        std::vector<Tick> partition_ticks;
        if (!LoadPartitionTicks(partition, start, end, {}, &partition_ticks, nullptr, -1,
                                nullptr)) {
            continue;
        }
        ticks.insert(ticks.end(), partition_ticks.begin(), partition_ticks.end());
    }

    std::sort(ticks.begin(), ticks.end(), [](const Tick& left, const Tick& right) {
        if (left.ts_ns != right.ts_ns) {
            return left.ts_ns < right.ts_ns;
        }
        return left.symbol < right.symbol;
    });
    return ticks;
}

std::size_t ParquetDataFeed::PartitionCount() const noexcept { return partitions_.size(); }

}  // namespace quant_hft
