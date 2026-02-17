#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <queue>
#include <sstream>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

#include "quant_hft/apps/backtest_replay_support.h"

#if QUANT_HFT_ENABLE_ARROW_PARQUET
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#endif

namespace {

namespace qapps = quant_hft::apps;

struct CsvToParquetSpec {
    std::string input_csv;
    std::string output_root;
    std::string source_filter;
    std::string start_date;
    std::string end_date;
    std::int64_t batch_rows{500000};
    std::int64_t memory_budget_mb{1024};
    std::int64_t row_group_mb{128};
    std::string compression{"snappy"};
    bool resume{true};
    bool overwrite{false};
    bool require_arrow_writer{false};
    std::string manifest_path;
};

struct PartitionState {
    std::string source;
    std::string trading_day;
    std::string instrument_id;
    std::filesystem::path raw_path;
    std::int64_t min_ts_ns{0};
    std::int64_t max_ts_ns{0};
    std::int64_t row_count{0};
};

struct ManifestEntry {
    std::string relative_file_path;
    std::string source;
    std::string trading_day;
    std::string instrument_id;
    std::int64_t min_ts_ns{0};
    std::int64_t max_ts_ns{0};
    std::int64_t row_count{0};
    std::string schema_version{"v2"};
    std::string source_csv_fingerprint;
};

struct SortRow {
    quant_hft::EpochNanos ts_ns{0};
    std::string symbol;
    std::string line;
};

struct MergeNode {
    quant_hft::EpochNanos ts_ns{0};
    std::string symbol;
    std::string line;
    std::size_t run_index{0};
};

struct MergeNodeCompare {
    bool operator()(const MergeNode& left, const MergeNode& right) const {
        if (left.ts_ns != right.ts_ns) {
            return left.ts_ns > right.ts_ns;
        }
        if (left.symbol != right.symbol) {
            return left.symbol > right.symbol;
        }
        return left.run_index > right.run_index;
    }
};

constexpr const char* kTickSidecarHeader =
    "symbol,exchange,ts_ns,last_price,last_volume,bid_price1,bid_volume1,ask_price1,"
    "ask_volume1,volume,turnover,open_interest";

bool ParsePositiveInt64(const std::string& raw, std::int64_t fallback, std::int64_t* out,
                        std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "internal parse output is null";
        }
        return false;
    }
    if (raw.empty()) {
        *out = fallback;
        return true;
    }

    std::int64_t parsed = 0;
    if (!qapps::detail::ParseInt64(raw, &parsed) || parsed <= 0) {
        if (error != nullptr) {
            *error = "invalid positive integer: " + raw;
        }
        return false;
    }
    *out = parsed;
    return true;
}

bool ParseBoolWithDefault(const std::string& raw, bool fallback, bool* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "internal bool output is null";
        }
        return false;
    }
    if (raw.empty()) {
        *out = fallback;
        return true;
    }
    bool parsed = fallback;
    if (!qapps::detail::ParseBool(raw, &parsed)) {
        if (error != nullptr) {
            *error = "invalid bool value: " + raw;
        }
        return false;
    }
    *out = parsed;
    return true;
}

bool ParseCliSpec(const qapps::ArgMap& args, CsvToParquetSpec* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "spec output is null";
        }
        return false;
    }

    CsvToParquetSpec spec;
    spec.input_csv = qapps::detail::GetArgAny(args, {"input_csv", "input-csv"});
    spec.output_root =
        qapps::detail::GetArgAny(args, {"output_root", "output-root", "output_dir", "output-dir"});
    spec.source_filter =
        qapps::detail::ToLower(qapps::detail::Trim(qapps::detail::GetArgAny(args, {"source"})));
    spec.start_date = qapps::detail::NormalizeTradingDay(
        qapps::detail::GetArgAny(args, {"start_date", "start-date"}));
    spec.end_date = qapps::detail::NormalizeTradingDay(
        qapps::detail::GetArgAny(args, {"end_date", "end-date"}));
    spec.compression =
        qapps::detail::ToLower(qapps::detail::GetArgAny(args, {"compression"}, "snappy"));
    spec.manifest_path = qapps::detail::GetArgAny(args, {"manifest_path", "manifest-path"});

    if (!ParsePositiveInt64(qapps::detail::GetArgAny(args, {"batch_rows", "batch-rows"}),
                            spec.batch_rows, &spec.batch_rows, error)) {
        return false;
    }
    if (!ParsePositiveInt64(
            qapps::detail::GetArgAny(args, {"memory_budget_mb", "memory-budget-mb"}),
            spec.memory_budget_mb, &spec.memory_budget_mb, error)) {
        return false;
    }
    if (!ParsePositiveInt64(qapps::detail::GetArgAny(args, {"row_group_mb", "row-group-mb"}),
                            spec.row_group_mb, &spec.row_group_mb, error)) {
        return false;
    }
    if (!ParseBoolWithDefault(qapps::detail::GetArgAny(args, {"resume"}), true, &spec.resume,
                              error)) {
        return false;
    }
    if (!ParseBoolWithDefault(qapps::detail::GetArgAny(args, {"overwrite"}), false, &spec.overwrite,
                              error)) {
        return false;
    }
    if (!ParseBoolWithDefault(
            qapps::detail::GetArgAny(args, {"require_arrow_writer", "require-arrow-writer"}), false,
            &spec.require_arrow_writer, error)) {
        return false;
    }

    if (spec.input_csv.empty()) {
        if (error != nullptr) {
            *error = "--input_csv is required";
        }
        return false;
    }
    if (spec.output_root.empty()) {
        if (error != nullptr) {
            *error = "--output_root is required";
        }
        return false;
    }
    if (spec.compression.empty()) {
        spec.compression = "snappy";
    }

    if (!spec.start_date.empty() && !spec.end_date.empty() && spec.start_date > spec.end_date) {
        if (error != nullptr) {
            *error = "start_date must be <= end_date";
        }
        return false;
    }

    if (spec.manifest_path.empty()) {
        spec.manifest_path =
            (std::filesystem::path(spec.output_root) / "_manifest" / "partitions.jsonl").string();
    }

    *out = std::move(spec);
    return true;
}

std::string BuildPartitionKey(const std::string& source, const std::string& trading_day,
                              const std::string& instrument_id) {
    return source + "|" + trading_day + "|" + instrument_id;
}

std::string BuildNormalizedTickLine(const qapps::ReplayTick& tick, const std::string& exchange,
                                    std::int32_t last_volume, double turnover,
                                    std::int64_t open_interest) {
    std::ostringstream oss;
    oss << tick.instrument_id << ',' << exchange << ',' << tick.ts_ns << ','
        << qapps::detail::FormatDouble(tick.last_price) << ',' << last_volume << ','
        << qapps::detail::FormatDouble(tick.bid_price_1) << ',' << tick.bid_volume_1 << ','
        << qapps::detail::FormatDouble(tick.ask_price_1) << ',' << tick.ask_volume_1 << ','
        << tick.volume << ',' << qapps::detail::FormatDouble(turnover) << ',' << open_interest;
    return oss.str();
}

bool ParseTickWithExtras(const std::map<std::string, std::size_t>& header_index,
                         const std::vector<std::string>& cells, qapps::ReplayTick* out_tick,
                         std::string* out_exchange, std::int32_t* out_last_volume,
                         double* out_turnover, std::int64_t* out_open_interest) {
    if (out_tick == nullptr || out_exchange == nullptr || out_last_volume == nullptr ||
        out_turnover == nullptr || out_open_interest == nullptr) {
        return false;
    }

    qapps::ReplayTick tick;
    if (!qapps::ParseCsvTick(header_index, cells, &tick)) {
        return false;
    }

    *out_exchange = qapps::detail::FindCell(
        header_index, cells, {"ExchangeID", "exchange", "Exchange", "exchange_id", "exchangeID"});

    std::int64_t parsed_last_volume = 0;
    const std::string last_volume_raw =
        qapps::detail::FindCell(header_index, cells, {"LastVolume", "last_volume"});
    if (!last_volume_raw.empty()) {
        qapps::detail::ParseInt64(last_volume_raw, &parsed_last_volume);
    }
    if (parsed_last_volume <= 0) {
        parsed_last_volume = tick.volume;
    }
    *out_last_volume = static_cast<std::int32_t>(std::max<std::int64_t>(0, parsed_last_volume));

    const std::string turnover_raw =
        qapps::detail::FindCell(header_index, cells, {"Turnover", "turnover"});
    double turnover = 0.0;
    qapps::detail::ParseDouble(turnover_raw, &turnover);
    *out_turnover = turnover;

    const std::string open_interest_raw =
        qapps::detail::FindCell(header_index, cells, {"OpenInterest", "open_interest"});
    std::int64_t open_interest = 0;
    qapps::detail::ParseInt64(open_interest_raw, &open_interest);
    *out_open_interest = open_interest;

    *out_tick = std::move(tick);
    return true;
}

bool ParseLineOrdering(const std::string& line, quant_hft::EpochNanos* out_ts,
                       std::string* out_symbol) {
    if (out_ts == nullptr || out_symbol == nullptr) {
        return false;
    }

    const std::size_t first = line.find(',');
    if (first == std::string::npos) {
        return false;
    }
    const std::size_t second = line.find(',', first + 1);
    if (second == std::string::npos) {
        return false;
    }
    const std::size_t third = line.find(',', second + 1);
    if (third == std::string::npos) {
        return false;
    }

    *out_symbol = line.substr(0, first);
    std::int64_t parsed_ts = 0;
    if (!qapps::detail::ParseInt64(line.substr(second + 1, third - second - 1), &parsed_ts)) {
        return false;
    }
    *out_ts = parsed_ts;
    return true;
}

bool WriteTextAtomic(const std::filesystem::path& path, const std::string& content,
                     std::string* error) {
    try {
        std::filesystem::create_directories(path.parent_path());
        const std::filesystem::path tmp_path = path.string() + ".tmp";

        {
            std::ofstream out(tmp_path, std::ios::out | std::ios::trunc);
            if (!out.is_open()) {
                if (error != nullptr) {
                    *error = "unable to open temporary file: " + tmp_path.string();
                }
                return false;
            }
            out << content;
            if (!out.good()) {
                if (error != nullptr) {
                    *error = "failed writing temporary file: " + tmp_path.string();
                }
                return false;
            }
        }

        std::error_code ec;
        std::filesystem::remove(path, ec);
        std::filesystem::rename(tmp_path, path);
    } catch (const std::exception& ex) {
        if (error != nullptr) {
            *error = ex.what();
        }
        return false;
    }
    return true;
}

bool WriteBinaryAtomic(const std::filesystem::path& path, const std::string& bytes,
                       std::string* error) {
    try {
        std::filesystem::create_directories(path.parent_path());
        const std::filesystem::path tmp_path = path.string() + ".tmp";

        {
            std::ofstream out(tmp_path, std::ios::out | std::ios::binary | std::ios::trunc);
            if (!out.is_open()) {
                if (error != nullptr) {
                    *error = "unable to open temporary binary file: " + tmp_path.string();
                }
                return false;
            }
            out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
            if (!out.good()) {
                if (error != nullptr) {
                    *error = "failed writing temporary binary file: " + tmp_path.string();
                }
                return false;
            }
        }

        std::error_code ec;
        std::filesystem::remove(path, ec);
        std::filesystem::rename(tmp_path, path);
    } catch (const std::exception& ex) {
        if (error != nullptr) {
            *error = ex.what();
        }
        return false;
    }
    return true;
}

bool MetaFingerprintMatches(const std::filesystem::path& meta_path, const std::string& fingerprint,
                            bool* out_matches, std::string* error) {
    if (out_matches == nullptr) {
        if (error != nullptr) {
            *error = "meta match output is null";
        }
        return false;
    }

    std::ifstream input(meta_path);
    if (!input.is_open()) {
        *out_matches = false;
        return true;
    }

    std::string line;
    while (std::getline(input, line)) {
        const std::size_t split = line.find('=');
        if (split == std::string::npos) {
            continue;
        }
        const std::string key = qapps::detail::Trim(line.substr(0, split));
        const std::string value = qapps::detail::Trim(line.substr(split + 1));
        if (key == "source_csv_fingerprint") {
            *out_matches = (value == fingerprint);
            return true;
        }
    }

    *out_matches = false;
    return true;
}

bool LoadMetaAsManifestEntry(const std::filesystem::path& parquet_path,
                             const std::string& output_root, ManifestEntry* out_entry,
                             std::string* error) {
    if (out_entry == nullptr) {
        if (error != nullptr) {
            *error = "meta entry output is null";
        }
        return false;
    }

    const std::filesystem::path meta_path = parquet_path.string() + ".meta";
    std::ifstream input(meta_path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open meta file: " + meta_path.string();
        }
        return false;
    }

    ManifestEntry entry;
    entry.relative_file_path =
        std::filesystem::relative(parquet_path, std::filesystem::path(output_root))
            .generic_string();

    for (const auto& segment : parquet_path) {
        const std::string text = segment.string();
        if (text.rfind("source=", 0) == 0) {
            entry.source = text.substr(std::string("source=").size());
        } else if (text.rfind("trading_day=", 0) == 0) {
            entry.trading_day = text.substr(std::string("trading_day=").size());
        } else if (text.rfind("instrument_id=", 0) == 0) {
            entry.instrument_id = text.substr(std::string("instrument_id=").size());
        }
    }

    std::string line;
    while (std::getline(input, line)) {
        const std::size_t split = line.find('=');
        if (split == std::string::npos) {
            continue;
        }
        const std::string key = qapps::detail::Trim(line.substr(0, split));
        const std::string value = qapps::detail::Trim(line.substr(split + 1));
        if (key == "min_ts_ns") {
            qapps::detail::ParseInt64(value, &entry.min_ts_ns);
        } else if (key == "max_ts_ns") {
            qapps::detail::ParseInt64(value, &entry.max_ts_ns);
        } else if (key == "row_count") {
            qapps::detail::ParseInt64(value, &entry.row_count);
        } else if (key == "schema_version") {
            entry.schema_version = value;
        } else if (key == "source_csv_fingerprint") {
            entry.source_csv_fingerprint = value;
        } else if (key == "source") {
            entry.source = value;
        }
    }

    if (entry.schema_version.empty()) {
        entry.schema_version = "v2";
    }
    *out_entry = std::move(entry);
    return true;
}

bool WriteManifestEntryLine(const ManifestEntry& entry, std::string* out_line) {
    if (out_line == nullptr) {
        return false;
    }

    std::ostringstream oss;
    oss << "{" << "\"file_path\":\"" << qapps::JsonEscape(entry.relative_file_path) << "\","
        << "\"source\":\"" << qapps::JsonEscape(entry.source) << "\"," << "\"trading_day\":\""
        << qapps::JsonEscape(entry.trading_day) << "\"," << "\"instrument_id\":\""
        << qapps::JsonEscape(entry.instrument_id) << "\"," << "\"min_ts_ns\":" << entry.min_ts_ns
        << ',' << "\"max_ts_ns\":" << entry.max_ts_ns << ',' << "\"row_count\":" << entry.row_count
        << ',' << "\"schema_version\":\"" << qapps::JsonEscape(entry.schema_version) << "\","
        << "\"source_csv_fingerprint\":\"" << qapps::JsonEscape(entry.source_csv_fingerprint)
        << "\"" << "}";
    *out_line = oss.str();
    return true;
}

bool LoadExistingManifest(const std::filesystem::path& manifest_path,
                          std::map<std::string, ManifestEntry>* out_entries, std::string* error) {
    if (out_entries == nullptr) {
        if (error != nullptr) {
            *error = "manifest map output is null";
        }
        return false;
    }

    if (!std::filesystem::exists(manifest_path)) {
        return true;
    }

    std::ifstream input(manifest_path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open existing manifest: " + manifest_path.string();
        }
        return false;
    }

    std::string line;
    while (std::getline(input, line)) {
        line = qapps::detail::Trim(line);
        if (line.empty()) {
            continue;
        }

        ManifestEntry entry;
        if (!qapps::detail::ExtractJsonString(line, "file_path", &entry.relative_file_path)) {
            if (error != nullptr) {
                *error = "invalid manifest line: missing file_path";
            }
            return false;
        }
        qapps::detail::ExtractJsonString(line, "source", &entry.source);
        qapps::detail::ExtractJsonString(line, "trading_day", &entry.trading_day);
        qapps::detail::ExtractJsonString(line, "instrument_id", &entry.instrument_id);
        qapps::detail::ExtractJsonString(line, "schema_version", &entry.schema_version);
        qapps::detail::ExtractJsonString(line, "source_csv_fingerprint",
                                         &entry.source_csv_fingerprint);
        double number = 0.0;
        if (qapps::detail::ExtractJsonNumber(line, "min_ts_ns", &number)) {
            entry.min_ts_ns = static_cast<std::int64_t>(number);
        }
        if (qapps::detail::ExtractJsonNumber(line, "max_ts_ns", &number)) {
            entry.max_ts_ns = static_cast<std::int64_t>(number);
        }
        if (qapps::detail::ExtractJsonNumber(line, "row_count", &number)) {
            entry.row_count = static_cast<std::int64_t>(number);
        }
        if (entry.schema_version.empty()) {
            entry.schema_version = "v2";
        }
        (*out_entries)[entry.relative_file_path] = entry;
    }

    return true;
}

bool FlushChunkToRun(const std::vector<SortRow>& chunk, std::size_t run_index,
                     const std::filesystem::path& run_dir, std::filesystem::path* out_run_path,
                     std::string* error) {
    if (out_run_path == nullptr) {
        if (error != nullptr) {
            *error = "run path output is null";
        }
        return false;
    }

    std::vector<SortRow> sorted = chunk;
    std::sort(sorted.begin(), sorted.end(), [](const SortRow& left, const SortRow& right) {
        if (left.ts_ns != right.ts_ns) {
            return left.ts_ns < right.ts_ns;
        }
        if (left.symbol != right.symbol) {
            return left.symbol < right.symbol;
        }
        return left.line < right.line;
    });

    std::filesystem::create_directories(run_dir);
    const std::filesystem::path run_path = run_dir / ("run-" + std::to_string(run_index) + ".csv");
    std::ofstream out(run_path, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        if (error != nullptr) {
            *error = "unable to write run file: " + run_path.string();
        }
        return false;
    }
    out << kTickSidecarHeader << '\n';
    for (const SortRow& row : sorted) {
        out << row.line << '\n';
    }
    if (!out.good()) {
        if (error != nullptr) {
            *error = "failed writing run file: " + run_path.string();
        }
        return false;
    }

    *out_run_path = run_path;
    return true;
}

bool BuildRunFiles(const std::filesystem::path& raw_path, const std::filesystem::path& run_dir,
                   std::int64_t batch_rows, std::int64_t memory_budget_mb,
                   std::vector<std::filesystem::path>* out_runs, std::string* error) {
    if (out_runs == nullptr) {
        if (error != nullptr) {
            *error = "run files output is null";
        }
        return false;
    }

    std::ifstream input(raw_path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open raw partition data: " + raw_path.string();
        }
        return false;
    }

    const std::size_t row_limit = static_cast<std::size_t>(std::max<std::int64_t>(1, batch_rows));
    const std::size_t byte_limit =
        static_cast<std::size_t>(std::max<std::int64_t>(1, memory_budget_mb) * 1024LL * 1024LL);

    std::vector<SortRow> chunk;
    chunk.reserve(row_limit);
    std::size_t chunk_bytes = 0;
    std::size_t run_index = 0;

    std::string line;
    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }

        quant_hft::EpochNanos ts_ns = 0;
        std::string symbol;
        if (!ParseLineOrdering(line, &ts_ns, &symbol)) {
            continue;
        }

        chunk.push_back(SortRow{ts_ns, std::move(symbol), line});
        chunk_bytes += line.size() + 32;

        if (chunk.size() >= row_limit || chunk_bytes >= byte_limit) {
            std::filesystem::path run_path;
            if (!FlushChunkToRun(chunk, run_index++, run_dir, &run_path, error)) {
                return false;
            }
            out_runs->push_back(std::move(run_path));
            chunk.clear();
            chunk_bytes = 0;
        }
    }

    if (!chunk.empty()) {
        std::filesystem::path run_path;
        if (!FlushChunkToRun(chunk, run_index++, run_dir, &run_path, error)) {
            return false;
        }
        out_runs->push_back(std::move(run_path));
    }

    return true;
}

bool MergeRunFiles(const std::vector<std::filesystem::path>& runs,
                   const std::filesystem::path& sidecar_path, std::int64_t* out_rows,
                   std::int64_t* out_min_ts_ns, std::int64_t* out_max_ts_ns, std::string* error) {
    if (out_rows == nullptr || out_min_ts_ns == nullptr || out_max_ts_ns == nullptr) {
        if (error != nullptr) {
            *error = "merge outputs are null";
        }
        return false;
    }

    std::filesystem::create_directories(sidecar_path.parent_path());
    const std::filesystem::path tmp_path = sidecar_path.string() + ".tmp";
    std::ofstream out(tmp_path, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        if (error != nullptr) {
            *error = "unable to open sidecar temp file: " + tmp_path.string();
        }
        return false;
    }
    out << kTickSidecarHeader << '\n';

    std::vector<std::unique_ptr<std::ifstream>> readers;
    readers.reserve(runs.size());

    std::priority_queue<MergeNode, std::vector<MergeNode>, MergeNodeCompare> heap;
    for (std::size_t index = 0; index < runs.size(); ++index) {
        auto reader = std::make_unique<std::ifstream>(runs[index]);
        if (!reader->is_open()) {
            if (error != nullptr) {
                *error = "unable to open run file: " + runs[index].string();
            }
            return false;
        }

        std::string header;
        std::getline(*reader, header);

        std::string line;
        if (std::getline(*reader, line) && !line.empty()) {
            quant_hft::EpochNanos ts_ns = 0;
            std::string symbol;
            if (ParseLineOrdering(line, &ts_ns, &symbol)) {
                heap.push(MergeNode{ts_ns, std::move(symbol), line, index});
            }
        }

        readers.push_back(std::move(reader));
    }

    std::int64_t rows = 0;
    std::int64_t min_ts = 0;
    std::int64_t max_ts = 0;
    while (!heap.empty()) {
        MergeNode node = heap.top();
        heap.pop();

        out << node.line << '\n';
        if (!out.good()) {
            if (error != nullptr) {
                *error = "failed writing sidecar file: " + tmp_path.string();
            }
            return false;
        }

        if (rows == 0) {
            min_ts = node.ts_ns;
            max_ts = node.ts_ns;
        } else {
            min_ts = std::min(min_ts, static_cast<std::int64_t>(node.ts_ns));
            max_ts = std::max(max_ts, static_cast<std::int64_t>(node.ts_ns));
        }
        ++rows;

        std::string next_line;
        if (std::getline(*readers[node.run_index], next_line) && !next_line.empty()) {
            quant_hft::EpochNanos next_ts = 0;
            std::string next_symbol;
            if (ParseLineOrdering(next_line, &next_ts, &next_symbol)) {
                heap.push(MergeNode{next_ts, std::move(next_symbol), next_line, node.run_index});
            }
        }
    }

    out.close();
    std::error_code ec;
    std::filesystem::remove(sidecar_path, ec);
    std::filesystem::rename(tmp_path, sidecar_path);

    *out_rows = rows;
    *out_min_ts_ns = min_ts;
    *out_max_ts_ns = max_ts;
    return true;
}

#if QUANT_HFT_ENABLE_ARROW_PARQUET
parquet::Compression::type ParseCompressionCodec(const std::string& compression) {
    const std::string normalized = qapps::detail::ToLower(qapps::detail::Trim(compression));
    if (normalized == "zstd") {
        return parquet::Compression::ZSTD;
    }
    if (normalized == "gzip") {
        return parquet::Compression::GZIP;
    }
    if (normalized == "brotli") {
        return parquet::Compression::BROTLI;
    }
    if (normalized == "lz4") {
        return parquet::Compression::LZ4;
    }
    if (normalized == "none" || normalized == "uncompressed") {
        return parquet::Compression::UNCOMPRESSED;
    }
    return parquet::Compression::SNAPPY;
}

bool WriteArrowParquetFromSidecar(const std::filesystem::path& sidecar_path,
                                  const std::filesystem::path& parquet_path,
                                  const CsvToParquetSpec& spec, std::string* error) {
    std::ifstream input(sidecar_path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open sidecar for arrow parquet write: " + sidecar_path.string();
        }
        return false;
    }

    std::string header;
    if (!std::getline(input, header)) {
        if (error != nullptr) {
            *error = "sidecar is empty: " + sidecar_path.string();
        }
        return false;
    }

    arrow::StringBuilder symbol_builder;
    arrow::StringBuilder exchange_builder;
    arrow::Int64Builder ts_builder;
    arrow::DoubleBuilder last_price_builder;
    arrow::Int32Builder last_volume_builder;
    arrow::DoubleBuilder bid_price1_builder;
    arrow::Int32Builder bid_volume1_builder;
    arrow::DoubleBuilder ask_price1_builder;
    arrow::Int32Builder ask_volume1_builder;
    arrow::Int64Builder volume_builder;
    arrow::DoubleBuilder turnover_builder;
    arrow::Int64Builder open_interest_builder;

    std::string line;
    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }
        const auto cells = qapps::detail::SplitCsvLine(line);
        if (cells.size() < 12U) {
            continue;
        }

        std::int64_t ts_ns = 0;
        double last_price = 0.0;
        std::int64_t last_volume = 0;
        double bid_price1 = 0.0;
        std::int64_t bid_volume1 = 0;
        double ask_price1 = 0.0;
        std::int64_t ask_volume1 = 0;
        std::int64_t volume = 0;
        double turnover = 0.0;
        std::int64_t open_interest = 0;

        if (!qapps::detail::ParseInt64(cells[2], &ts_ns)) {
            continue;
        }
        qapps::detail::ParseDouble(cells[3], &last_price);
        qapps::detail::ParseInt64(cells[4], &last_volume);
        qapps::detail::ParseDouble(cells[5], &bid_price1);
        qapps::detail::ParseInt64(cells[6], &bid_volume1);
        qapps::detail::ParseDouble(cells[7], &ask_price1);
        qapps::detail::ParseInt64(cells[8], &ask_volume1);
        qapps::detail::ParseInt64(cells[9], &volume);
        qapps::detail::ParseDouble(cells[10], &turnover);
        qapps::detail::ParseInt64(cells[11], &open_interest);

        symbol_builder.Append(cells[0]);
        exchange_builder.Append(cells[1]);
        ts_builder.Append(ts_ns);
        last_price_builder.Append(last_price);
        last_volume_builder.Append(static_cast<std::int32_t>(std::max<std::int64_t>(
            std::numeric_limits<std::int32_t>::min(),
            std::min<std::int64_t>(std::numeric_limits<std::int32_t>::max(), last_volume))));
        bid_price1_builder.Append(bid_price1);
        bid_volume1_builder.Append(static_cast<std::int32_t>(std::max<std::int64_t>(
            std::numeric_limits<std::int32_t>::min(),
            std::min<std::int64_t>(std::numeric_limits<std::int32_t>::max(), bid_volume1))));
        ask_price1_builder.Append(ask_price1);
        ask_volume1_builder.Append(static_cast<std::int32_t>(std::max<std::int64_t>(
            std::numeric_limits<std::int32_t>::min(),
            std::min<std::int64_t>(std::numeric_limits<std::int32_t>::max(), ask_volume1))));
        volume_builder.Append(volume);
        turnover_builder.Append(turnover);
        open_interest_builder.Append(open_interest);
    }

    auto finish_array = [&](auto* builder, std::shared_ptr<arrow::Array>* out_array,
                            const char* field_name) -> bool {
        if (builder == nullptr || out_array == nullptr) {
            if (error != nullptr) {
                *error = std::string("invalid arrow builder for field: ") + field_name;
            }
            return false;
        }
        auto status = builder->Finish(out_array);
        if (!status.ok()) {
            if (error != nullptr) {
                *error = std::string("failed to finish arrow array for field: ") + field_name +
                         " (" + status.ToString() + ")";
            }
            return false;
        }
        return true;
    };

    std::shared_ptr<arrow::Array> symbol_array;
    std::shared_ptr<arrow::Array> exchange_array;
    std::shared_ptr<arrow::Array> ts_array;
    std::shared_ptr<arrow::Array> last_price_array;
    std::shared_ptr<arrow::Array> last_volume_array;
    std::shared_ptr<arrow::Array> bid_price1_array;
    std::shared_ptr<arrow::Array> bid_volume1_array;
    std::shared_ptr<arrow::Array> ask_price1_array;
    std::shared_ptr<arrow::Array> ask_volume1_array;
    std::shared_ptr<arrow::Array> volume_array;
    std::shared_ptr<arrow::Array> turnover_array;
    std::shared_ptr<arrow::Array> open_interest_array;

    if (!finish_array(&symbol_builder, &symbol_array, "symbol") ||
        !finish_array(&exchange_builder, &exchange_array, "exchange") ||
        !finish_array(&ts_builder, &ts_array, "ts_ns") ||
        !finish_array(&last_price_builder, &last_price_array, "last_price") ||
        !finish_array(&last_volume_builder, &last_volume_array, "last_volume") ||
        !finish_array(&bid_price1_builder, &bid_price1_array, "bid_price1") ||
        !finish_array(&bid_volume1_builder, &bid_volume1_array, "bid_volume1") ||
        !finish_array(&ask_price1_builder, &ask_price1_array, "ask_price1") ||
        !finish_array(&ask_volume1_builder, &ask_volume1_array, "ask_volume1") ||
        !finish_array(&volume_builder, &volume_array, "volume") ||
        !finish_array(&turnover_builder, &turnover_array, "turnover") ||
        !finish_array(&open_interest_builder, &open_interest_array, "open_interest")) {
        return false;
    }

    const std::vector<std::shared_ptr<arrow::Field>> fields = {
        arrow::field("symbol", arrow::utf8()),       arrow::field("exchange", arrow::utf8()),
        arrow::field("ts_ns", arrow::int64()),       arrow::field("last_price", arrow::float64()),
        arrow::field("last_volume", arrow::int32()), arrow::field("bid_price1", arrow::float64()),
        arrow::field("bid_volume1", arrow::int32()), arrow::field("ask_price1", arrow::float64()),
        arrow::field("ask_volume1", arrow::int32()), arrow::field("volume", arrow::int64()),
        arrow::field("turnover", arrow::float64()),  arrow::field("open_interest", arrow::int64()),
    };
    const auto schema = std::make_shared<arrow::Schema>(fields);
    const auto table = arrow::Table::Make(
        schema, {symbol_array, exchange_array, ts_array, last_price_array, last_volume_array,
                 bid_price1_array, bid_volume1_array, ask_price1_array, ask_volume1_array,
                 volume_array, turnover_array, open_interest_array});

    const std::filesystem::path tmp_path = parquet_path.string() + ".tmp";
    std::error_code ec;
    std::filesystem::remove(tmp_path, ec);

    auto output_result = arrow::io::FileOutputStream::Open(tmp_path.string());
    if (!output_result.ok()) {
        if (error != nullptr) {
            *error =
                "failed to open arrow parquet output file: " + output_result.status().ToString();
        }
        return false;
    }
    std::shared_ptr<arrow::io::FileOutputStream> output = output_result.ValueOrDie();

    parquet::WriterProperties::Builder writer_props_builder;
    writer_props_builder.compression(ParseCompressionCodec(spec.compression));
    const std::shared_ptr<parquet::WriterProperties> writer_props = writer_props_builder.build();

    parquet::ArrowWriterProperties::Builder arrow_props_builder;
    const std::shared_ptr<parquet::ArrowWriterProperties> arrow_props =
        arrow_props_builder.store_schema()->build();

    const std::int64_t row_group_rows =
        std::max<std::int64_t>(1024, (spec.row_group_mb * 1024LL * 1024LL) / 128LL);
    const auto write_status = parquet::arrow::WriteTable(
        *table, arrow::default_memory_pool(), output, row_group_rows, writer_props, arrow_props);
    if (!write_status.ok()) {
        if (error != nullptr) {
            *error = "failed to write parquet with arrow: " + write_status.ToString();
        }
        output->Close();
        return false;
    }
    auto close_status = output->Close();
    if (!close_status.ok()) {
        if (error != nullptr) {
            *error = "failed to close parquet file: " + close_status.ToString();
        }
        return false;
    }

    std::filesystem::remove(parquet_path, ec);
    std::filesystem::rename(tmp_path, parquet_path);
    return true;
}
#endif

bool WriteParquetStubFile(const std::filesystem::path& parquet_path, const CsvToParquetSpec& spec,
                          std::string* error) {
    std::ostringstream metadata;
    metadata << "schema_version=v2\n";
    metadata << "compression=" << spec.compression << "\n";
    metadata << "row_group_mb=" << spec.row_group_mb << "\n";
    const std::string bytes = std::string("PAR1") + metadata.str() + std::string("PAR1");
    return WriteBinaryAtomic(parquet_path, bytes, error);
}

bool WritePartitionParquetFile(const std::filesystem::path& sidecar_path,
                               const std::filesystem::path& parquet_path,
                               const CsvToParquetSpec& spec, bool* out_used_arrow,
                               std::string* error) {
    if (out_used_arrow != nullptr) {
        *out_used_arrow = false;
    }

#if QUANT_HFT_ENABLE_ARROW_PARQUET
    if (WriteArrowParquetFromSidecar(sidecar_path, parquet_path, spec, error)) {
        if (out_used_arrow != nullptr) {
            *out_used_arrow = true;
        }
        return true;
    }
    if (spec.require_arrow_writer) {
        return false;
    }
#else
    (void)sidecar_path;
    if (spec.require_arrow_writer) {
        if (error != nullptr) {
            *error =
                "arrow parquet writer is not enabled in this build, configure with "
                "-DQUANT_HFT_ENABLE_ARROW_PARQUET=ON";
        }
        return false;
    }
#endif

    return WriteParquetStubFile(parquet_path, spec, error);
}

bool WriteMetaFile(const std::filesystem::path& meta_path, const ManifestEntry& entry,
                   std::string* error) {
    std::ostringstream meta;
    meta << "min_ts_ns=" << entry.min_ts_ns << '\n';
    meta << "max_ts_ns=" << entry.max_ts_ns << '\n';
    meta << "row_count=" << entry.row_count << '\n';
    meta << "schema_version=" << entry.schema_version << '\n';
    meta << "source_csv_fingerprint=" << entry.source_csv_fingerprint << '\n';
    meta << "source=" << entry.source << '\n';
    return WriteTextAtomic(meta_path, meta.str(), error);
}

}  // namespace

int main(int argc, char** argv) {
    using namespace qapps;

    const ArgMap args = ParseArgs(argc, argv);

    CsvToParquetSpec spec;
    std::string error;
    if (!ParseCliSpec(args, &spec, &error)) {
        std::cerr << "csv_to_parquet_cli: " << error << '\n';
        return 2;
    }

    if (!std::filesystem::exists(spec.input_csv)) {
        std::cerr << "csv_to_parquet_cli: input csv does not exist: " << spec.input_csv << '\n';
        return 2;
    }

    const std::filesystem::path output_root(spec.output_root);
    const std::filesystem::path tmp_root = output_root / "_tmp" / "csv_to_parquet_runs";
    std::filesystem::create_directories(tmp_root);

    std::string fingerprint = ComputeFileDigest(spec.input_csv, &error);
    if (fingerprint.empty()) {
        std::cerr << "csv_to_parquet_cli: " << error << '\n';
        return 1;
    }

    std::ifstream input(spec.input_csv);
    if (!input.is_open()) {
        std::cerr << "csv_to_parquet_cli: unable to open input csv: " << spec.input_csv << '\n';
        return 1;
    }

    std::string header_line;
    if (!std::getline(input, header_line)) {
        std::cerr << "csv_to_parquet_cli: csv file is empty: " << spec.input_csv << '\n';
        return 1;
    }

    const auto headers = detail::SplitCsvLine(header_line);
    std::map<std::string, std::size_t> header_index;
    for (std::size_t index = 0; index < headers.size(); ++index) {
        header_index[headers[index]] = index;
    }

    std::map<std::string, PartitionState> partition_state;
    std::unordered_map<std::string, std::unique_ptr<std::ofstream>> raw_streams;

    std::string line;
    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }

        const auto cells = detail::SplitCsvLine(line);

        ReplayTick tick;
        std::string exchange;
        std::int32_t last_volume = 0;
        double turnover = 0.0;
        std::int64_t open_interest = 0;
        if (!ParseTickWithExtras(header_index, cells, &tick, &exchange, &last_volume, &turnover,
                                 &open_interest)) {
            continue;
        }

        if (!spec.start_date.empty()) {
            const std::string day = detail::NormalizeTradingDay(tick.trading_day);
            if (!day.empty() && day < spec.start_date) {
                continue;
            }
        }
        if (!spec.end_date.empty()) {
            const std::string day = detail::NormalizeTradingDay(tick.trading_day);
            if (!day.empty() && day > spec.end_date) {
                continue;
            }
        }

        const std::string source = detail::InstrumentSymbolPrefix(tick.instrument_id);
        if (source.empty()) {
            continue;
        }
        if (!spec.source_filter.empty() && source != spec.source_filter) {
            continue;
        }

        const std::string trading_day = detail::NormalizeTradingDay(tick.trading_day);
        if (trading_day.empty()) {
            continue;
        }

        const std::string partition_key =
            BuildPartitionKey(source, trading_day, tick.instrument_id);
        auto [state_it, inserted] = partition_state.try_emplace(partition_key);
        PartitionState& state = state_it->second;
        if (inserted) {
            state.source = source;
            state.trading_day = trading_day;
            state.instrument_id = tick.instrument_id;
            state.raw_path = tmp_root / ("source=" + source) / ("trading_day=" + trading_day) /
                             ("instrument_id=" + tick.instrument_id + ".raw.csv");
            std::filesystem::create_directories(state.raw_path.parent_path());
            state.min_ts_ns = tick.ts_ns;
            state.max_ts_ns = tick.ts_ns;
        } else {
            state.min_ts_ns = std::min(state.min_ts_ns, static_cast<std::int64_t>(tick.ts_ns));
            state.max_ts_ns = std::max(state.max_ts_ns, static_cast<std::int64_t>(tick.ts_ns));
        }
        ++state.row_count;

        auto stream_it = raw_streams.find(partition_key);
        if (stream_it == raw_streams.end()) {
            auto stream =
                std::make_unique<std::ofstream>(state.raw_path, std::ios::out | std::ios::trunc);
            if (!stream->is_open()) {
                std::cerr << "csv_to_parquet_cli: unable to open raw partition file: "
                          << state.raw_path << '\n';
                return 1;
            }
            stream_it = raw_streams.emplace(partition_key, std::move(stream)).first;
        }

        *stream_it->second << BuildNormalizedTickLine(tick, exchange, last_volume, turnover,
                                                      open_interest)
                           << '\n';
        if (!stream_it->second->good()) {
            std::cerr << "csv_to_parquet_cli: failed writing raw partition file: " << state.raw_path
                      << '\n';
            return 1;
        }
    }

    for (auto& [_, stream] : raw_streams) {
        if (stream != nullptr) {
            stream->close();
        }
    }

    std::map<std::string, ManifestEntry> manifest_entries;
    const std::filesystem::path manifest_path(spec.manifest_path);
    if (spec.resume && !spec.overwrite) {
        if (!LoadExistingManifest(manifest_path, &manifest_entries, &error)) {
            std::cerr << "csv_to_parquet_cli: " << error << '\n';
            return 1;
        }
    }

    std::int64_t partitions_converted = 0;
    std::int64_t partitions_skipped = 0;
    std::int64_t partitions_written_with_arrow = 0;

    for (const auto& [partition_key, state] : partition_state) {
        (void)partition_key;

        const std::filesystem::path partition_dir = output_root / ("source=" + state.source) /
                                                    ("trading_day=" + state.trading_day) /
                                                    ("instrument_id=" + state.instrument_id);
        const std::filesystem::path parquet_path = partition_dir / "part-0000.parquet";
        const std::filesystem::path meta_path = parquet_path.string() + ".meta";
        const std::filesystem::path sidecar_path = parquet_path.string() + ".ticks.csv";

        ManifestEntry entry;
        entry.relative_file_path =
            std::filesystem::relative(parquet_path, output_root).generic_string();
        entry.source = state.source;
        entry.trading_day = state.trading_day;
        entry.instrument_id = state.instrument_id;
        entry.min_ts_ns = state.min_ts_ns;
        entry.max_ts_ns = state.max_ts_ns;
        entry.row_count = state.row_count;
        entry.schema_version = "v2";
        entry.source_csv_fingerprint = fingerprint;

        bool should_skip = false;
        if (spec.resume && !spec.overwrite && std::filesystem::exists(parquet_path) &&
            std::filesystem::exists(meta_path)) {
            bool fingerprint_matches = false;
            if (!MetaFingerprintMatches(meta_path, fingerprint, &fingerprint_matches, &error)) {
                std::cerr << "csv_to_parquet_cli: " << error << '\n';
                return 1;
            }
            if (fingerprint_matches) {
                should_skip = true;
            }
        }

        if (should_skip) {
            ManifestEntry loaded;
            if (!LoadMetaAsManifestEntry(parquet_path, output_root.string(), &loaded, &error)) {
                std::cerr << "csv_to_parquet_cli: " << error << '\n';
                return 1;
            }
            manifest_entries[loaded.relative_file_path] = loaded;
            ++partitions_skipped;
            continue;
        }

        std::filesystem::create_directories(partition_dir);
        if (spec.overwrite) {
            std::error_code ec;
            std::filesystem::remove(parquet_path, ec);
            std::filesystem::remove(meta_path, ec);
            std::filesystem::remove(sidecar_path, ec);
        }

        const std::filesystem::path run_dir =
            tmp_root / "runs" /
            (state.source + "_" + state.trading_day + "_" + state.instrument_id);
        std::filesystem::remove_all(run_dir);

        std::vector<std::filesystem::path> runs;
        if (!BuildRunFiles(state.raw_path, run_dir, spec.batch_rows, spec.memory_budget_mb, &runs,
                           &error)) {
            std::cerr << "csv_to_parquet_cli: " << error << '\n';
            return 1;
        }

        std::int64_t merged_rows = 0;
        std::int64_t merged_min_ts = 0;
        std::int64_t merged_max_ts = 0;
        if (!MergeRunFiles(runs, sidecar_path, &merged_rows, &merged_min_ts, &merged_max_ts,
                           &error)) {
            std::cerr << "csv_to_parquet_cli: " << error << '\n';
            return 1;
        }

        bool used_arrow_writer = false;
        if (!WritePartitionParquetFile(sidecar_path, parquet_path, spec, &used_arrow_writer,
                                       &error)) {
            std::cerr << "csv_to_parquet_cli: " << error << '\n';
            return 1;
        }
        if (used_arrow_writer) {
            ++partitions_written_with_arrow;
        }

        entry.row_count = merged_rows;
        entry.min_ts_ns = merged_rows > 0 ? merged_min_ts : 0;
        entry.max_ts_ns = merged_rows > 0 ? merged_max_ts : 0;

        if (!WriteMetaFile(meta_path, entry, &error)) {
            std::cerr << "csv_to_parquet_cli: " << error << '\n';
            return 1;
        }

        manifest_entries[entry.relative_file_path] = entry;
        ++partitions_converted;

        std::error_code ec;
        std::filesystem::remove_all(run_dir, ec);
    }

    {
        std::ostringstream manifest_text;
        for (const auto& [_, entry] : manifest_entries) {
            std::string line_json;
            WriteManifestEntryLine(entry, &line_json);
            manifest_text << line_json << '\n';
        }
        if (!WriteTextAtomic(manifest_path, manifest_text.str(), &error)) {
            std::cerr << "csv_to_parquet_cli: " << error << '\n';
            return 1;
        }
    }

    std::error_code ec;
    std::filesystem::remove_all(tmp_root, ec);

    std::ostringstream out;
    out << "{\n"
        << "  \"status\": \"ok\",\n"
        << "  \"input_csv\": \"" << JsonEscape(spec.input_csv) << "\",\n"
        << "  \"output_root\": \"" << JsonEscape(spec.output_root) << "\",\n"
        << "  \"manifest_path\": \"" << JsonEscape(manifest_path.string()) << "\",\n"
        << "  \"source_filter\": \"" << JsonEscape(spec.source_filter) << "\",\n"
        << "  \"require_arrow_writer\": " << (spec.require_arrow_writer ? "true" : "false") << ",\n"
        << "  \"partitions_written_with_arrow\": " << partitions_written_with_arrow << ",\n"
        << "  \"partitions_converted\": " << partitions_converted << ",\n"
        << "  \"partitions_skipped\": " << partitions_skipped << "\n"
        << "}\n";

    std::cout << out.str();
    return 0;
}
