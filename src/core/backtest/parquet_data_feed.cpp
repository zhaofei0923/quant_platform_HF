#include "quant_hft/backtest/parquet_data_feed.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <limits>
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

#if !QUANT_HFT_ENABLE_ARROW_PARQUET
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
#endif

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

#if !QUANT_HFT_ENABLE_ARROW_PARQUET
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
#endif

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
    if (!out) return false;
    ParquetTickCursor cursor;
    ParquetPartitionMeta partition;
    partition.file_path = parquet_path.string();
    partition.instrument_id = default_symbol;
    if (!cursor.Open(partition, start, end, {}, 4096, error)) return false;
    while (max_ticks < 0 || static_cast<std::int64_t>(out->size()) < max_ticks) {
        Tick tick;
        bool has_tick = false;
        if (!cursor.Next(&tick, &has_tick, error)) return false;
        if (!has_tick) break;
        out->push_back(std::move(tick));
    }
    if (metrics) {
        *metrics = cursor.metrics();
        metrics->early_stop_hit =
            max_ticks >= 0 && static_cast<std::int64_t>(out->size()) >= max_ticks;
    }
    return true;
}

#endif

#if !QUANT_HFT_ENABLE_ARROW_PARQUET
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
#endif

}  // namespace

struct ParquetTickCursor::Impl {
    ParquetPartitionMeta partition;
    EpochNanos start{0};
    EpochNanos end{0};
    EpochNanos previous_ts{std::numeric_limits<EpochNanos>::min()};
    ParquetScanMetrics metrics;
#if QUANT_HFT_ENABLE_ARROW_PARQUET
    std::unique_ptr<parquet::arrow::FileReader> reader;
    std::unique_ptr<arrow::RecordBatchReader> batches;
    std::shared_ptr<arrow::RecordBatch> batch;
    std::array<std::shared_ptr<arrow::Array>, 12> columns;
    std::int64_t row{0};
    std::vector<std::int64_t> row_group_ends;
#else
    std::ifstream input;
    std::vector<std::string> headers;
#endif
};

ParquetTickCursor::ParquetTickCursor() : impl_(std::make_unique<Impl>()) {}
ParquetTickCursor::~ParquetTickCursor() = default;
ParquetTickCursor::ParquetTickCursor(ParquetTickCursor&&) noexcept = default;
ParquetTickCursor& ParquetTickCursor::operator=(ParquetTickCursor&&) noexcept = default;
const ParquetScanMetrics& ParquetTickCursor::metrics() const { return impl_->metrics; }

bool ParquetTickCursor::Open(const ParquetPartitionMeta& partition, const Timestamp& start,
                             const Timestamp& end,
                             const std::vector<std::string>& projected_columns,
                             std::size_t batch_size, std::string* error) {
    impl_ = std::make_unique<Impl>();
    auto& state = *impl_;
    state.partition = partition;
    state.start = start.ToEpochNanos();
    state.end = end.ToEpochNanos();
    if (batch_size == 0 || batch_size > 1'048'576 || state.start > state.end) {
        if (error) *error = "invalid parquet cursor batch size or interval";
        return false;
    }
#if QUANT_HFT_ENABLE_ARROW_PARQUET
    auto input = arrow::io::ReadableFile::Open(partition.file_path);
    if (!input.ok()) {
        if (error) *error = input.status().ToString();
        return false;
    }
    parquet::ReaderProperties parquet_properties;
    parquet_properties.enable_buffered_stream();
    parquet_properties.set_buffer_size(64 * 1024);
    parquet::ArrowReaderProperties arrow_properties;
    arrow_properties.set_pre_buffer(false);
    parquet::arrow::FileReaderBuilder builder;
    builder.properties(arrow_properties);
    auto status = builder.Open(input.ValueOrDie(), parquet_properties);
    if (status.ok()) status = builder.Build(&state.reader);
    if (!status.ok() || !state.reader) {
        if (error) *error = "unable to open parquet reader: " + status.ToString();
        return false;
    }
    state.reader->set_batch_size(static_cast<std::int64_t>(batch_size));
    std::shared_ptr<arrow::Schema> schema;
    status = state.reader->GetSchema(&schema);
    if (!status.ok() || !schema || schema->GetFieldIndex("ts_ns") < 0) {
        if (error) *error = "parquet cursor requires ts_ns column";
        return false;
    }
    std::vector<int> columns;
    if (projected_columns.empty()) {
        for (int column = 0; column < schema->num_fields(); ++column) columns.push_back(column);
    } else {
        std::set<int> unique;
        unique.insert(schema->GetFieldIndex("ts_ns"));
        for (const auto& name : projected_columns) {
            const int index = schema->GetFieldIndex(name);
            if (index >= 0) unique.insert(index);
        }
        columns.assign(unique.begin(), unique.end());
    }
    std::vector<int> row_groups;
    std::int64_t rows = 0;
    const auto metadata = state.reader->parquet_reader()->metadata();
    for (int group = 0; group < metadata->num_row_groups(); ++group) {
        row_groups.push_back(group);
        rows += metadata->RowGroup(group)->num_rows();
        state.row_group_ends.push_back(rows);
    }
    status = state.reader->GetRecordBatchReader(row_groups, columns, &state.batches);
    if (!status.ok() || !state.batches) {
        if (error) *error = "unable to open parquet batch reader: " + status.ToString();
        return false;
    }
    state.metrics.io_bytes = SafeFileSize(partition.file_path);
#else
    (void)projected_columns;
    state.input.open(partition.file_path + ".ticks.csv");
    std::string header;
    if (!state.input || !std::getline(state.input, header)) {
        if (error) *error = "unable to open parquet cursor sidecar";
        return false;
    }
    state.headers = SplitCsvLine(header);
    state.metrics.io_bytes = SafeFileSize(partition.file_path + ".ticks.csv");
#endif
    return true;
}

bool ParquetTickCursor::Next(Tick* tick, bool* has_tick, std::string* error) {
    if (!tick || !has_tick) {
        if (error) *error = "parquet cursor output is null";
        return false;
    }
    *has_tick = false;
    auto& state = *impl_;
    for (;;) {
        Tick row;
#if QUANT_HFT_ENABLE_ARROW_PARQUET
        if (!state.batches) {
            if (error) *error = "parquet cursor is not open";
            return false;
        }
        if (!state.batch || state.row >= state.batch->num_rows()) {
            const auto status = state.batches->ReadNext(&state.batch);
            if (!status.ok()) {
                if (error) *error = status.ToString();
                return false;
            }
            if (!state.batch) return true;
            static constexpr std::array<const char*, 12> names = {
                "symbol",      "exchange",   "ts_ns",       "last_price",
                "last_volume", "bid_price1", "bid_volume1", "ask_price1",
                "ask_volume1", "volume",     "turnover",    "open_interest"};
            for (std::size_t column = 0; column < names.size(); ++column) {
                state.columns[column] = state.batch->GetColumnByName(names[column]);
            }
            state.row = 0;
            ++state.metrics.batches_read;
            state.metrics.buffered_rows_high_water =
                std::max(state.metrics.buffered_rows_high_water, state.batch->num_rows());
        }
        const auto index = state.row++;
        row.symbol = ReadStringArrayValue(state.columns[0], index);
        if (row.symbol.empty()) row.symbol = state.partition.instrument_id;
        row.exchange = ReadStringArrayValue(state.columns[1], index);
        row.ts_ns = ReadInt64ArrayValue(state.columns[2], index);
        row.last_price = ReadDoubleArrayValue(state.columns[3], index);
        row.last_volume = static_cast<std::int32_t>(ReadInt64ArrayValue(state.columns[4], index));
        row.bid_price1 = ReadDoubleArrayValue(state.columns[5], index);
        row.bid_volume1 = static_cast<std::int32_t>(ReadInt64ArrayValue(state.columns[6], index));
        row.ask_price1 = ReadDoubleArrayValue(state.columns[7], index);
        row.ask_volume1 = static_cast<std::int32_t>(ReadInt64ArrayValue(state.columns[8], index));
        row.volume = ReadInt64ArrayValue(state.columns[9], index);
        row.turnover = ReadDoubleArrayValue(state.columns[10], index);
        row.open_interest = ReadInt64ArrayValue(state.columns[11], index);
#else
        std::string line;
        if (!std::getline(state.input, line)) {
            if (state.input.bad()) {
                if (error) *error = "parquet cursor sidecar read failed";
                return false;
            }
            return true;
        }
        if (line.empty()) continue;
        try {
            row = BuildTickFromValues(state.headers, SplitCsvLine(line),
                                      state.partition.instrument_id);
        } catch (const std::exception&) {
            if (error) *error = "invalid parquet cursor sidecar row";
            return false;
        }
        ++state.metrics.batches_read;
        state.metrics.buffered_rows_high_water = 1;
#endif
        ++state.metrics.scan_rows;
#if QUANT_HFT_ENABLE_ARROW_PARQUET
        state.metrics.scan_row_groups =
            static_cast<std::int64_t>(std::lower_bound(state.row_group_ends.begin(),
                                                       state.row_group_ends.end(),
                                                       state.metrics.scan_rows) -
                                      state.row_group_ends.begin()) +
            1;
#else
        state.metrics.scan_row_groups = 1;
#endif
        if (row.ts_ns < state.previous_ts) {
            if (error) *error = "non-monotonic parquet partition: " + state.partition.file_path;
            return false;
        }
        state.previous_ts = row.ts_ns;
        if (row.ts_ns < state.start) continue;
        if (row.ts_ns > state.end) return true;
        *tick = std::move(row);
        *has_tick = true;
        return true;
    }
}

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
    std::string parquet_error;
    if (!AppendTicksFromParquet(partition.file_path, partition.instrument_id, start, end, out,
                                metrics, max_ticks, &parquet_error)) {
        if (error != nullptr) {
            if (!parquet_error.empty()) {
                *error = parquet_error;
            } else {
                *error = "failed to read parquet partition: " + partition.file_path;
            }
        }
        return false;
    }

#else
    if (!LoadTicksFromSidecar(partition, start, end, out, metrics, max_ticks, error)) {
        return false;
    }
#endif

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
