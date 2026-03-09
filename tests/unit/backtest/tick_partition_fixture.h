#pragma once

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "quant_hft/contracts/types.h"

#if QUANT_HFT_ENABLE_ARROW_PARQUET
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#endif

namespace quant_hft::backtest::test {
namespace detail {

inline bool WriteTextFile(const std::filesystem::path& path, const std::string& contents,
                          std::string* error) {
    std::ofstream output(path);
    if (!output.is_open()) {
        if (error != nullptr) {
            *error = "unable to open fixture file for write: " + path.string();
        }
        return false;
    }
    output << contents;
    if (!output.good()) {
        if (error != nullptr) {
            *error = "failed writing fixture file: " + path.string();
        }
        return false;
    }
    return true;
}

inline std::string FormatCsvDouble(double value) {
    std::ostringstream oss;
    oss.precision(17);
    oss << value;
    return oss.str();
}

inline bool WriteTickMetaFile(const std::filesystem::path& parquet_file, const std::vector<Tick>& ticks,
                              std::string* error) {
    if (ticks.empty()) {
        if (error != nullptr) {
            *error = "tick fixture requires at least one row";
        }
        return false;
    }

    auto [min_it, max_it] = std::minmax_element(
        ticks.begin(), ticks.end(),
        [](const Tick& lhs, const Tick& rhs) { return lhs.ts_ns < rhs.ts_ns; });

    std::ostringstream meta;
    meta << "min_ts_ns=" << min_it->ts_ns << '\n';
    meta << "max_ts_ns=" << max_it->ts_ns << '\n';
    meta << "row_count=" << ticks.size() << '\n';
    return WriteTextFile(parquet_file.string() + ".meta", meta.str(), error);
}

inline bool WriteTickSidecarFile(const std::filesystem::path& parquet_file,
                                 const std::vector<Tick>& ticks, std::string* error) {
    std::ostringstream sidecar;
    sidecar << "symbol,exchange,ts_ns,last_price,last_volume,bid_price1,bid_volume1,ask_price1,"
               "ask_volume1,volume,turnover,open_interest\n";
    for (const Tick& tick : ticks) {
        sidecar << tick.symbol << ',' << tick.exchange << ',' << tick.ts_ns << ','
                << FormatCsvDouble(tick.last_price) << ',' << tick.last_volume << ','
                << FormatCsvDouble(tick.bid_price1) << ',' << tick.bid_volume1 << ','
                << FormatCsvDouble(tick.ask_price1) << ',' << tick.ask_volume1 << ','
                << tick.volume << ',' << FormatCsvDouble(tick.turnover) << ','
                << tick.open_interest << '\n';
    }
    return WriteTextFile(parquet_file.string() + ".ticks.csv", sidecar.str(), error);
}

#if QUANT_HFT_ENABLE_ARROW_PARQUET
template <typename BuilderT, typename ValueT>
inline bool AppendArrowValue(BuilderT* builder, const ValueT& value, const char* field_name,
                             std::string* error) {
    if (builder == nullptr) {
        if (error != nullptr) {
            *error = std::string("missing arrow builder for field: ") + field_name;
        }
        return false;
    }
    const auto status = builder->Append(value);
    if (!status.ok()) {
        if (error != nullptr) {
            *error = std::string("failed to append field: ") + field_name + " (" +
                     status.ToString() + ")";
        }
        return false;
    }
    return true;
}

template <typename BuilderT>
inline bool FinishArrowArray(BuilderT* builder, const char* field_name,
                             std::shared_ptr<arrow::Array>* out_array, std::string* error) {
    if (builder == nullptr || out_array == nullptr) {
        if (error != nullptr) {
            *error = std::string("invalid arrow array output for field: ") + field_name;
        }
        return false;
    }
    const auto status = builder->Finish(out_array);
    if (!status.ok()) {
        if (error != nullptr) {
            *error = std::string("failed to finish arrow array for field: ") + field_name + " (" +
                     status.ToString() + ")";
        }
        return false;
    }
    return true;
}

inline bool WriteArrowTickParquet(const std::filesystem::path& parquet_file,
                                  const std::vector<Tick>& ticks, std::string* error) {
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

    for (const Tick& tick : ticks) {
        if (!AppendArrowValue(&symbol_builder, tick.symbol, "symbol", error) ||
            !AppendArrowValue(&exchange_builder, tick.exchange, "exchange", error) ||
            !AppendArrowValue(&ts_builder, tick.ts_ns, "ts_ns", error) ||
            !AppendArrowValue(&last_price_builder, tick.last_price, "last_price", error) ||
            !AppendArrowValue(&last_volume_builder, tick.last_volume, "last_volume", error) ||
            !AppendArrowValue(&bid_price1_builder, tick.bid_price1, "bid_price1", error) ||
            !AppendArrowValue(&bid_volume1_builder, tick.bid_volume1, "bid_volume1", error) ||
            !AppendArrowValue(&ask_price1_builder, tick.ask_price1, "ask_price1", error) ||
            !AppendArrowValue(&ask_volume1_builder, tick.ask_volume1, "ask_volume1", error) ||
            !AppendArrowValue(&volume_builder, tick.volume, "volume", error) ||
            !AppendArrowValue(&turnover_builder, tick.turnover, "turnover", error) ||
            !AppendArrowValue(&open_interest_builder, tick.open_interest, "open_interest",
                              error)) {
            return false;
        }
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

    if (!FinishArrowArray(&symbol_builder, "symbol", &symbol_array, error) ||
        !FinishArrowArray(&exchange_builder, "exchange", &exchange_array, error) ||
        !FinishArrowArray(&ts_builder, "ts_ns", &ts_array, error) ||
        !FinishArrowArray(&last_price_builder, "last_price", &last_price_array, error) ||
        !FinishArrowArray(&last_volume_builder, "last_volume", &last_volume_array, error) ||
        !FinishArrowArray(&bid_price1_builder, "bid_price1", &bid_price1_array, error) ||
        !FinishArrowArray(&bid_volume1_builder, "bid_volume1", &bid_volume1_array, error) ||
        !FinishArrowArray(&ask_price1_builder, "ask_price1", &ask_price1_array, error) ||
        !FinishArrowArray(&ask_volume1_builder, "ask_volume1", &ask_volume1_array, error) ||
        !FinishArrowArray(&volume_builder, "volume", &volume_array, error) ||
        !FinishArrowArray(&turnover_builder, "turnover", &turnover_array, error) ||
        !FinishArrowArray(&open_interest_builder, "open_interest", &open_interest_array, error)) {
        return false;
    }

    const auto schema = arrow::schema({
        arrow::field("symbol", arrow::utf8(), false),
        arrow::field("exchange", arrow::utf8(), false),
        arrow::field("ts_ns", arrow::int64(), false),
        arrow::field("last_price", arrow::float64(), false),
        arrow::field("last_volume", arrow::int32(), false),
        arrow::field("bid_price1", arrow::float64(), false),
        arrow::field("bid_volume1", arrow::int32(), false),
        arrow::field("ask_price1", arrow::float64(), false),
        arrow::field("ask_volume1", arrow::int32(), false),
        arrow::field("volume", arrow::int64(), false),
        arrow::field("turnover", arrow::float64(), false),
        arrow::field("open_interest", arrow::int64(), false),
    });

    const auto table = arrow::Table::Make(
        schema,
        {symbol_array, exchange_array, ts_array, last_price_array, last_volume_array,
         bid_price1_array, bid_volume1_array, ask_price1_array, ask_volume1_array, volume_array,
         turnover_array, open_interest_array});

    auto output_result = arrow::io::FileOutputStream::Open(parquet_file.string());
    if (!output_result.ok()) {
        if (error != nullptr) {
            *error =
                "failed to open parquet fixture output: " + output_result.status().ToString();
        }
        return false;
    }
    std::shared_ptr<arrow::io::FileOutputStream> output_stream = output_result.ValueOrDie();

    parquet::WriterProperties::Builder writer_props_builder;
    writer_props_builder.compression(parquet::Compression::UNCOMPRESSED);
    std::shared_ptr<parquet::WriterProperties> writer_props = writer_props_builder.build();
    parquet::ArrowWriterProperties::Builder arrow_props_builder;
    std::shared_ptr<parquet::ArrowWriterProperties> arrow_props = arrow_props_builder.build();

    const auto write_status = parquet::arrow::WriteTable(
        *table, arrow::default_memory_pool(), output_stream,
        std::max<std::int64_t>(1, static_cast<std::int64_t>(ticks.size())), writer_props,
        arrow_props);
    if (!write_status.ok()) {
        if (error != nullptr) {
            *error = "failed to write parquet fixture: " + write_status.ToString();
        }
        return false;
    }

    const auto close_status = output_stream->Close();
    if (!close_status.ok()) {
        if (error != nullptr) {
            *error = "failed to close parquet fixture: " + close_status.ToString();
        }
        return false;
    }
    return true;
}
#endif

}  // namespace detail

inline bool WriteTickPartitionFixture(const std::filesystem::path& parquet_file,
                                      const std::vector<Tick>& ticks, std::string* error) {
    if (ticks.empty()) {
        if (error != nullptr) {
            *error = "tick fixture requires at least one row";
        }
        return false;
    }

    std::error_code ec;
    std::filesystem::create_directories(parquet_file.parent_path(), ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to create fixture directories: " + ec.message();
        }
        return false;
    }

    if (!detail::WriteTickMetaFile(parquet_file, ticks, error) ||
        !detail::WriteTickSidecarFile(parquet_file, ticks, error)) {
        return false;
    }

#if QUANT_HFT_ENABLE_ARROW_PARQUET
    return detail::WriteArrowTickParquet(parquet_file, ticks, error);
#else
    return detail::WriteTextFile(parquet_file, "PAR1", error);
#endif
}

}  // namespace quant_hft::backtest::test
