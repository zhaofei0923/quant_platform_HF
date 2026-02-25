#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
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

constexpr const char* kTickSidecarHeader =
    "symbol,exchange,ts_ns,last_price,last_volume,bid_price1,bid_volume1,ask_price1,"
    "ask_volume1,volume,turnover,open_interest";
constexpr const char* kSchemaVersion = "v3";
constexpr std::int64_t kDefaultMaxOpenSidecarStreams = 64;
constexpr std::int64_t kMinMaxOpenSidecarStreams = 8;
constexpr std::int64_t kMaxMaxOpenSidecarStreams = 512;
constexpr std::int64_t kMinArrowBatchRows = 4096;
constexpr std::int64_t kMaxArrowBatchRows = 100000;
constexpr std::int64_t kApproxArrowBytesPerRow = 320;

struct CsvToParquetSpec {
    std::string input_csv;
    std::string output_root;
    std::string source_filter;
    std::string start_date;
    std::string end_date;
    std::int64_t batch_rows{500000};
    std::int64_t memory_budget_mb{1024};
    std::int64_t row_group_mb{128};
    std::int64_t max_open_sidecar_streams{kDefaultMaxOpenSidecarStreams};
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
    std::filesystem::path sidecar_tmp_path;
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
    std::string schema_version{kSchemaVersion};
    std::string source_csv_fingerprint;
};

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

std::int64_t ClampInt64(std::int64_t value, std::int64_t low, std::int64_t high) {
    if (value < low) {
        return low;
    }
    if (value > high) {
        return high;
    }
    return value;
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
    if (!ParsePositiveInt64(
            qapps::detail::GetArgAny(args, {"max_open_sidecar_streams",
                                            "max-open-sidecar-streams"}),
            spec.max_open_sidecar_streams, &spec.max_open_sidecar_streams, error)) {
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

    spec.max_open_sidecar_streams = ClampInt64(spec.max_open_sidecar_streams,
                                               kMinMaxOpenSidecarStreams,
                                               kMaxMaxOpenSidecarStreams);

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

std::string AbbreviateLine(const std::string& line, std::size_t limit = 240) {
    if (line.size() <= limit) {
        return line;
    }
    return line.substr(0, limit) + "...";
}

bool ParseTickWithExtras(const std::map<std::string, std::size_t>& header_index,
                         const std::vector<std::string>& cells, qapps::ReplayTick* out_tick,
                         std::string* out_exchange, std::int32_t* out_last_volume,
                         double* out_turnover, std::int64_t* out_open_interest,
                         std::string* out_error) {
    if (out_tick == nullptr || out_exchange == nullptr || out_last_volume == nullptr ||
        out_turnover == nullptr || out_open_interest == nullptr) {
        if (out_error != nullptr) {
            *out_error = "internal parse output is null";
        }
        return false;
    }

    qapps::ReplayTick tick;
    if (!qapps::ParseCsvTick(header_index, cells, &tick)) {
        if (out_error != nullptr) {
            *out_error = "failed to parse tick fields";
        }
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

bool MoveFileAtomic(const std::filesystem::path& src, const std::filesystem::path& dst,
                    std::string* error) {
    try {
        if (!std::filesystem::exists(src)) {
            if (error != nullptr) {
                *error = "source file does not exist: " + src.string();
            }
            return false;
        }

        std::filesystem::create_directories(dst.parent_path());

        std::error_code ec;
        std::filesystem::remove(dst, ec);
        std::filesystem::rename(src, dst, ec);
        if (!ec) {
            return true;
        }

        const std::filesystem::path tmp_dst = dst.string() + ".tmp";
        std::filesystem::remove(tmp_dst, ec);

        std::ifstream input(src, std::ios::binary);
        if (!input.is_open()) {
            if (error != nullptr) {
                *error = "unable to open source file for copy: " + src.string();
            }
            return false;
        }
        std::ofstream output(tmp_dst, std::ios::binary | std::ios::trunc);
        if (!output.is_open()) {
            if (error != nullptr) {
                *error = "unable to open destination temp file for copy: " + tmp_dst.string();
            }
            return false;
        }

        output << input.rdbuf();
        if (!input.good() && !input.eof()) {
            if (error != nullptr) {
                *error = "failed reading source file during copy: " + src.string();
            }
            return false;
        }
        if (!output.good()) {
            if (error != nullptr) {
                *error = "failed writing destination temp file during copy: " + tmp_dst.string();
            }
            return false;
        }
        output.close();

        std::filesystem::remove(dst, ec);
        std::filesystem::rename(tmp_dst, dst);
        std::filesystem::remove(src, ec);
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
        std::filesystem::relative(parquet_path, std::filesystem::path(output_root)).generic_string();

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
        entry.schema_version = kSchemaVersion;
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
            entry.schema_version = kSchemaVersion;
        }
        (*out_entries)[entry.relative_file_path] = entry;
    }

    return true;
}

#if QUANT_HFT_ENABLE_ARROW_PARQUET
std::int64_t EffectiveArrowBatchRows(const CsvToParquetSpec& spec) {
    const std::int64_t memory_bytes = std::max<std::int64_t>(1, spec.memory_budget_mb) * 1024LL * 1024LL;
    const std::int64_t estimated_rows = std::max<std::int64_t>(
        kMinArrowBatchRows, memory_bytes / (kApproxArrowBytesPerRow * 2LL));
    const std::int64_t requested_rows = std::max<std::int64_t>(kMinArrowBatchRows, spec.batch_rows);
    return ClampInt64(std::min(requested_rows, estimated_rows), kMinArrowBatchRows,
                      kMaxArrowBatchRows);
}

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

    const std::vector<std::shared_ptr<arrow::Field>> fields = {
        arrow::field("symbol", arrow::utf8()),       arrow::field("exchange", arrow::utf8()),
        arrow::field("ts_ns", arrow::int64()),       arrow::field("last_price", arrow::float64()),
        arrow::field("last_volume", arrow::int32()), arrow::field("bid_price1", arrow::float64()),
        arrow::field("bid_volume1", arrow::int32()), arrow::field("ask_price1", arrow::float64()),
        arrow::field("ask_volume1", arrow::int32()), arrow::field("volume", arrow::int64()),
        arrow::field("turnover", arrow::float64()),  arrow::field("open_interest", arrow::int64()),
    };
    const auto schema = std::make_shared<arrow::Schema>(fields);

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
    const std::int64_t row_group_rows =
        std::max<std::int64_t>(1024, (spec.row_group_mb * 1024LL * 1024LL) / 128LL);
    writer_props_builder.max_row_group_length(row_group_rows);
    const std::shared_ptr<parquet::WriterProperties> writer_props = writer_props_builder.build();

    parquet::ArrowWriterProperties::Builder arrow_props_builder;
    const std::shared_ptr<parquet::ArrowWriterProperties> arrow_props =
        arrow_props_builder.store_schema()->build();

    auto writer_result = parquet::arrow::FileWriter::Open(*schema, arrow::default_memory_pool(),
                                                          output, writer_props, arrow_props);
    if (!writer_result.ok()) {
        if (error != nullptr) {
            *error = "failed to open parquet file writer: " + writer_result.status().ToString();
        }
        const arrow::Status ignored_close_status = output->Close();
        (void)ignored_close_status;
        return false;
    }
    std::unique_ptr<parquet::arrow::FileWriter> writer = std::move(writer_result).ValueOrDie();

    const std::int64_t flush_rows = EffectiveArrowBatchRows(spec);
    std::int64_t pending_rows = 0;

    auto append_or_fail = [&](const arrow::Status& status, const char* field_name) -> bool {
        if (status.ok()) {
            return true;
        }
        if (error != nullptr) {
            *error = std::string("failed to append field: ") + field_name + " (" + status.ToString() +
                     ")";
        }
        return false;
    };

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

    auto flush_batch = [&]() -> bool {
        if (pending_rows <= 0) {
            return true;
        }

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

        const auto batch = arrow::RecordBatch::Make(
            schema, pending_rows,
            {symbol_array, exchange_array, ts_array, last_price_array, last_volume_array,
             bid_price1_array, bid_volume1_array, ask_price1_array, ask_volume1_array, volume_array,
             turnover_array, open_interest_array});
        const auto write_status = writer->WriteRecordBatch(*batch);
        if (!write_status.ok()) {
            if (error != nullptr) {
                *error = "failed to write parquet record batch: " + write_status.ToString();
            }
            return false;
        }
        pending_rows = 0;
        return true;
    };

    std::string line;
    std::int64_t line_no = 1;
    while (std::getline(input, line)) {
        ++line_no;
        if (line.empty()) {
            continue;
        }

        const auto cells = qapps::detail::SplitCsvLine(line);
        if (cells.size() < 12U) {
            if (error != nullptr) {
                *error = "invalid sidecar row at line " + std::to_string(line_no) +
                         ": expected 12 columns";
            }
            return false;
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

        if (!qapps::detail::ParseInt64(cells[2], &ts_ns) ||
            !qapps::detail::ParseInt64(cells[4], &last_volume) ||
            !qapps::detail::ParseInt64(cells[6], &bid_volume1) ||
            !qapps::detail::ParseInt64(cells[8], &ask_volume1) ||
            !qapps::detail::ParseInt64(cells[9], &volume) ||
            !qapps::detail::ParseInt64(cells[11], &open_interest)) {
            if (error != nullptr) {
                *error = "invalid integer value in sidecar at line " + std::to_string(line_no);
            }
            return false;
        }

        qapps::detail::ParseDouble(cells[3], &last_price);
        qapps::detail::ParseDouble(cells[5], &bid_price1);
        qapps::detail::ParseDouble(cells[7], &ask_price1);
        qapps::detail::ParseDouble(cells[10], &turnover);

        if (!append_or_fail(symbol_builder.Append(cells[0]), "symbol") ||
            !append_or_fail(exchange_builder.Append(cells[1]), "exchange") ||
            !append_or_fail(ts_builder.Append(ts_ns), "ts_ns") ||
            !append_or_fail(last_price_builder.Append(last_price), "last_price") ||
            !append_or_fail(
                last_volume_builder.Append(static_cast<std::int32_t>(std::max<std::int64_t>(
                    std::numeric_limits<std::int32_t>::min(),
                    std::min<std::int64_t>(std::numeric_limits<std::int32_t>::max(), last_volume)))),
                "last_volume") ||
            !append_or_fail(bid_price1_builder.Append(bid_price1), "bid_price1") ||
            !append_or_fail(
                bid_volume1_builder.Append(static_cast<std::int32_t>(std::max<std::int64_t>(
                    std::numeric_limits<std::int32_t>::min(),
                    std::min<std::int64_t>(std::numeric_limits<std::int32_t>::max(), bid_volume1)))),
                "bid_volume1") ||
            !append_or_fail(ask_price1_builder.Append(ask_price1), "ask_price1") ||
            !append_or_fail(
                ask_volume1_builder.Append(static_cast<std::int32_t>(std::max<std::int64_t>(
                    std::numeric_limits<std::int32_t>::min(),
                    std::min<std::int64_t>(std::numeric_limits<std::int32_t>::max(), ask_volume1)))),
                "ask_volume1") ||
            !append_or_fail(volume_builder.Append(volume), "volume") ||
            !append_or_fail(turnover_builder.Append(turnover), "turnover") ||
            !append_or_fail(open_interest_builder.Append(open_interest), "open_interest")) {
            return false;
        }

        ++pending_rows;
        if (pending_rows >= flush_rows && !flush_batch()) {
            return false;
        }
    }

    if (!flush_batch()) {
        return false;
    }

    auto writer_close_status = writer->Close();
    if (!writer_close_status.ok()) {
        if (error != nullptr) {
            *error = "failed to close parquet writer: " + writer_close_status.ToString();
        }
        const arrow::Status ignored_close_status = output->Close();
        (void)ignored_close_status;
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
    metadata << "schema_version=" << kSchemaVersion << "\n";
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
    struct SidecarStreamState {
        std::unique_ptr<std::ofstream> stream;
        std::uint64_t last_used_seq{0};
    };
    std::unordered_map<std::string, SidecarStreamState> sidecar_streams;
    std::uint64_t stream_use_seq = 0;
    std::size_t open_sidecar_streams = 0;

    auto close_stream = [&](SidecarStreamState* stream_state) {
        if (stream_state == nullptr || stream_state->stream == nullptr) {
            return;
        }
        stream_state->stream->close();
        stream_state->stream.reset();
        if (open_sidecar_streams > 0) {
            --open_sidecar_streams;
        }
    };

    auto evict_lru_stream = [&]() {
        std::string victim_key;
        std::uint64_t victim_seq = std::numeric_limits<std::uint64_t>::max();
        for (auto& [key, stream_state] : sidecar_streams) {
            if (stream_state.stream != nullptr && stream_state.last_used_seq < victim_seq) {
                victim_key = key;
                victim_seq = stream_state.last_used_seq;
            }
        }
        if (!victim_key.empty()) {
            close_stream(&sidecar_streams[victim_key]);
        }
    };

    auto ensure_sidecar_stream = [&](const std::string& partition_key, const PartitionState& state,
                                     std::ofstream** out_stream) -> bool {
        if (out_stream == nullptr) {
            std::cerr << "csv_to_parquet_cli: internal sidecar stream output is null\n";
            return false;
        }
        auto [stream_it, inserted] = sidecar_streams.try_emplace(partition_key);
        (void)inserted;
        SidecarStreamState& stream_state = stream_it->second;
        if (stream_state.stream == nullptr) {
            while (open_sidecar_streams >= static_cast<std::size_t>(spec.max_open_sidecar_streams)) {
                evict_lru_stream();
            }
            std::filesystem::create_directories(state.sidecar_tmp_path.parent_path());
            const bool sidecar_exists = std::filesystem::exists(state.sidecar_tmp_path);
            const std::ios::openmode mode = std::ios::out | (sidecar_exists ? std::ios::app : std::ios::trunc);
            auto stream = std::make_unique<std::ofstream>(state.sidecar_tmp_path, mode);
            if (!stream->is_open()) {
                std::cerr << "csv_to_parquet_cli: unable to open partition sidecar temp file: "
                          << state.sidecar_tmp_path << '\n';
                return false;
            }
            if (!sidecar_exists) {
                *stream << kTickSidecarHeader << '\n';
                if (!stream->good()) {
                    std::cerr << "csv_to_parquet_cli: failed writing sidecar header: "
                              << state.sidecar_tmp_path << '\n';
                    return false;
                }
            }
            stream_state.stream = std::move(stream);
            ++open_sidecar_streams;
        }

        stream_state.last_used_seq = ++stream_use_seq;
        *out_stream = stream_state.stream.get();
        return true;
    };

    std::int64_t input_rows_total = 0;
    std::int64_t input_rows_selected = 0;
    std::int64_t input_rows_filtered_source = 0;
    std::int64_t input_rows_filtered_start_date = 0;
    std::int64_t input_rows_filtered_end_date = 0;
    std::int64_t input_rows_parse_failed = 0;

    std::string line;
    std::int64_t line_no = 1;
    while (std::getline(input, line)) {
        ++line_no;
        if (line.empty()) {
            continue;
        }

        ++input_rows_total;
        const auto cells = detail::SplitCsvLine(line);

        ReplayTick tick;
        std::string exchange;
        std::int32_t last_volume = 0;
        double turnover = 0.0;
        std::int64_t open_interest = 0;
        std::string parse_error;
        if (!ParseTickWithExtras(header_index, cells, &tick, &exchange, &last_volume, &turnover,
                                 &open_interest, &parse_error)) {
            ++input_rows_parse_failed;
            std::cerr << "csv_to_parquet_cli: parse error at line " << line_no << ": " << parse_error
                      << "; row=\"" << AbbreviateLine(line) << "\"\n";
            return 1;
        }

        const std::string source = detail::InstrumentSymbolPrefix(tick.instrument_id);
        if (source.empty()) {
            ++input_rows_parse_failed;
            std::cerr << "csv_to_parquet_cli: parse error at line " << line_no
                      << ": empty source prefix for instrument_id=\"" << tick.instrument_id << "\"\n";
            return 1;
        }
        if (!spec.source_filter.empty() && source != spec.source_filter) {
            ++input_rows_filtered_source;
            continue;
        }

        const std::string trading_day = detail::NormalizeTradingDay(tick.trading_day);
        if (trading_day.empty()) {
            ++input_rows_parse_failed;
            std::cerr << "csv_to_parquet_cli: parse error at line " << line_no
                      << ": trading_day is empty after normalization\n";
            return 1;
        }

        if (!spec.start_date.empty() && trading_day < spec.start_date) {
            ++input_rows_filtered_start_date;
            continue;
        }
        if (!spec.end_date.empty() && trading_day > spec.end_date) {
            ++input_rows_filtered_end_date;
            continue;
        }

        const std::string partition_key = BuildPartitionKey(source, trading_day, tick.instrument_id);
        auto [state_it, inserted] = partition_state.try_emplace(partition_key);
        PartitionState& state = state_it->second;
        if (inserted) {
            state.source = source;
            state.trading_day = trading_day;
            state.instrument_id = tick.instrument_id;
            state.sidecar_tmp_path =
                tmp_root / ("source=" + source) / ("trading_day=" + trading_day) /
                ("instrument_id=" + tick.instrument_id) / "part-0000.parquet.ticks.csv";
            state.min_ts_ns = tick.ts_ns;
            state.max_ts_ns = tick.ts_ns;
        } else {
            state.min_ts_ns = std::min(state.min_ts_ns, static_cast<std::int64_t>(tick.ts_ns));
            state.max_ts_ns = std::max(state.max_ts_ns, static_cast<std::int64_t>(tick.ts_ns));
        }
        ++state.row_count;
        ++input_rows_selected;

        std::ofstream* stream = nullptr;
        if (!ensure_sidecar_stream(partition_key, state, &stream) || stream == nullptr) {
            return 1;
        }

        *stream << BuildNormalizedTickLine(tick, exchange, last_volume, turnover, open_interest)
                << '\n';
        if (!stream->good()) {
            std::cerr << "csv_to_parquet_cli: failed writing sidecar row: " << state.sidecar_tmp_path
                      << '\n';
            return 1;
        }
    }

    for (auto& [_, stream_state] : sidecar_streams) {
        close_stream(&stream_state);
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
        entry.min_ts_ns = state.row_count > 0 ? state.min_ts_ns : 0;
        entry.max_ts_ns = state.row_count > 0 ? state.max_ts_ns : 0;
        entry.row_count = state.row_count;
        entry.schema_version = kSchemaVersion;
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

        if (!MoveFileAtomic(state.sidecar_tmp_path, sidecar_path, &error)) {
            std::cerr << "csv_to_parquet_cli: " << error << '\n';
            return 1;
        }

        bool used_arrow_writer = false;
        if (!WritePartitionParquetFile(sidecar_path, parquet_path, spec, &used_arrow_writer, &error)) {
            std::cerr << "csv_to_parquet_cli: " << error << '\n';
            return 1;
        }
        if (used_arrow_writer) {
            ++partitions_written_with_arrow;
        }

        if (!WriteMetaFile(meta_path, entry, &error)) {
            std::cerr << "csv_to_parquet_cli: " << error << '\n';
            return 1;
        }

        manifest_entries[entry.relative_file_path] = entry;
        ++partitions_converted;
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
    const std::int64_t effective_arrow_batch_rows =
#if QUANT_HFT_ENABLE_ARROW_PARQUET
        EffectiveArrowBatchRows(spec);
#else
        0;
#endif
    out << "{\n"
        << "  \"status\": \"ok\",\n"
        << "  \"input_csv\": \"" << JsonEscape(spec.input_csv) << "\",\n"
        << "  \"output_root\": \"" << JsonEscape(spec.output_root) << "\",\n"
        << "  \"manifest_path\": \"" << JsonEscape(manifest_path.string()) << "\",\n"
        << "  \"schema_version\": \"" << kSchemaVersion << "\",\n"
        << "  \"source_filter\": \"" << JsonEscape(spec.source_filter) << "\",\n"
        << "  \"require_arrow_writer\": " << (spec.require_arrow_writer ? "true" : "false") << ",\n"
        << "  \"batch_rows\": " << spec.batch_rows << ",\n"
        << "  \"effective_arrow_batch_rows\": " << effective_arrow_batch_rows << ",\n"
        << "  \"memory_budget_mb\": " << spec.memory_budget_mb << ",\n"
        << "  \"row_group_mb\": " << spec.row_group_mb << ",\n"
        << "  \"max_open_sidecar_streams\": " << spec.max_open_sidecar_streams << ",\n"
        << "  \"partitions_written_with_arrow\": " << partitions_written_with_arrow << ",\n"
        << "  \"partitions_converted\": " << partitions_converted << ",\n"
        << "  \"partitions_skipped\": " << partitions_skipped << ",\n"
        << "  \"input_rows_total\": " << input_rows_total << ",\n"
        << "  \"input_rows_selected\": " << input_rows_selected << ",\n"
        << "  \"input_rows_filtered_source\": " << input_rows_filtered_source << ",\n"
        << "  \"input_rows_filtered_start_date\": " << input_rows_filtered_start_date << ",\n"
        << "  \"input_rows_filtered_end_date\": " << input_rows_filtered_end_date << ",\n"
        << "  \"input_rows_parse_failed\": " << input_rows_parse_failed << "\n"
        << "}\n";

    std::cout << out.str();
    return 0;
}
