#include "quant_hft/backtest/sub_strategy_indicator_trace_parquet_writer.h"

#include <algorithm>
#include <ctime>
#include <exception>
#include <filesystem>
#include <memory>
#include <string>

#if QUANT_HFT_ENABLE_ARROW_PARQUET
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#endif

namespace quant_hft {
namespace {

bool SetError(const std::string& message, std::string* error) {
    if (error != nullptr) {
        *error = message;
    }
    return false;
}

constexpr std::int64_t kNanosPerSecond = 1'000'000'000LL;

std::string FormatDateTimeFromEpochNs(EpochNanos ts_ns) {
    const std::time_t seconds = static_cast<std::time_t>(ts_ns / kNanosPerSecond);
    std::tm tm_value {};
#if defined(_WIN32)
    if (gmtime_s(&tm_value, &seconds) != 0) {
        return "";
    }
#else
    if (gmtime_r(&seconds, &tm_value) == nullptr) {
        return "";
    }
#endif

    char buffer[17];
    if (std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M", &tm_value) == 0) {
        return "";
    }
    return std::string(buffer);
}

#if QUANT_HFT_ENABLE_ARROW_PARQUET
bool ExpectArrowStatus(const arrow::Status& status, const std::string& prefix, std::string* error) {
    if (status.ok()) {
        return true;
    }
    return SetError(prefix + ": " + status.ToString(), error);
}

template <typename BuilderT>
bool FinishArray(BuilderT* builder, const std::string& name, std::shared_ptr<arrow::Array>* out,
                 std::string* error) {
    const auto status = builder->Finish(out);
    return ExpectArrowStatus(status, "failed to finalize sub-strategy trace field '" + name + "'",
                             error);
}
#endif

}  // namespace

bool SubStrategyIndicatorTraceParquetWriter::Open(const std::string& output_path,
                                                  std::string* error) {
    if (is_open_) {
        return SetError("sub-strategy indicator trace writer is already open", error);
    }
    if (output_path.empty()) {
        return SetError("sub-strategy indicator trace output path is empty", error);
    }

#if !QUANT_HFT_ENABLE_ARROW_PARQUET
    (void)output_path;
    return SetError("sub-strategy indicator trace requires QUANT_HFT_ENABLE_ARROW_PARQUET=ON",
                    error);
#else
    try {
        const std::filesystem::path path(output_path);
        if (std::filesystem::exists(path)) {
            return SetError("sub-strategy indicator trace output already exists: " + path.string(),
                            error);
        }
        if (!path.parent_path().empty()) {
            std::filesystem::create_directories(path.parent_path());
        }
    } catch (const std::exception& ex) {
        return SetError(
            std::string("failed to prepare sub-strategy indicator trace path: ") + ex.what(),
            error);
    }

    output_path_ = output_path;
    rows_written_ = 0;
    rows_.clear();
    is_open_ = true;
    return true;
#endif
}

bool SubStrategyIndicatorTraceParquetWriter::Append(const SubStrategyIndicatorTraceRow& row,
                                                    std::string* error) {
    if (!is_open_) {
        return SetError("sub-strategy indicator trace writer is not open", error);
    }
    if (row.instrument_id.empty()) {
        return SetError("sub-strategy indicator trace row instrument_id is empty", error);
    }
    if (row.strategy_id.empty()) {
        return SetError("sub-strategy indicator trace row strategy_id is empty", error);
    }
    if (row.strategy_type.empty()) {
        return SetError("sub-strategy indicator trace row strategy_type is empty", error);
    }

#if !QUANT_HFT_ENABLE_ARROW_PARQUET
    (void)row;
    return SetError("sub-strategy indicator trace requires QUANT_HFT_ENABLE_ARROW_PARQUET=ON",
                    error);
#else
    rows_.push_back(row);
    ++rows_written_;
    return true;
#endif
}

bool SubStrategyIndicatorTraceParquetWriter::Close(std::string* error) {
    if (!is_open_) {
        return true;
    }

#if !QUANT_HFT_ENABLE_ARROW_PARQUET
    return SetError("sub-strategy indicator trace requires QUANT_HFT_ENABLE_ARROW_PARQUET=ON",
                    error);
#else
    arrow::StringBuilder instrument_id_builder;
    arrow::Int64Builder ts_ns_builder;
    arrow::StringBuilder dt_utc_builder;
    arrow::Int32Builder timeframe_minutes_builder;
    arrow::StringBuilder strategy_id_builder;
    arrow::StringBuilder strategy_type_builder;
    arrow::DoubleBuilder bar_open_builder;
    arrow::DoubleBuilder bar_high_builder;
    arrow::DoubleBuilder bar_low_builder;
    arrow::DoubleBuilder bar_close_builder;
    arrow::DoubleBuilder bar_volume_builder;
    arrow::DoubleBuilder kama_builder;
    arrow::DoubleBuilder atr_builder;
    arrow::DoubleBuilder adx_builder;
    arrow::DoubleBuilder er_builder;
    arrow::DoubleBuilder stop_loss_price_builder;
    arrow::DoubleBuilder take_profit_price_builder;
    arrow::UInt8Builder market_regime_builder;

    const auto append_optional = [&](const std::optional<double>& value, arrow::DoubleBuilder* b,
                                     const std::string& field_name) -> bool {
        if (!value.has_value()) {
            return ExpectArrowStatus(b->AppendNull(), "failed appending null for " + field_name,
                                     error);
        }
        return ExpectArrowStatus(b->Append(*value), "failed appending " + field_name, error);
    };

    for (const auto& row : rows_) {
        if (!ExpectArrowStatus(instrument_id_builder.Append(row.instrument_id),
                               "failed appending instrument_id", error) ||
            !ExpectArrowStatus(ts_ns_builder.Append(row.ts_ns), "failed appending ts_ns", error) ||
            !ExpectArrowStatus(
                dt_utc_builder.Append(row.dt_utc.empty() ? FormatDateTimeFromEpochNs(row.ts_ns)
                                                         : row.dt_utc),
                               "failed appending dt_utc", error) ||
            !ExpectArrowStatus(timeframe_minutes_builder.Append(
                                   row.timeframe_minutes > 0 ? row.timeframe_minutes : 1),
                               "failed appending timeframe_minutes", error) ||
            !ExpectArrowStatus(strategy_id_builder.Append(row.strategy_id),
                               "failed appending strategy_id", error) ||
            !ExpectArrowStatus(strategy_type_builder.Append(row.strategy_type),
                               "failed appending strategy_type", error) ||
            !ExpectArrowStatus(bar_open_builder.Append(row.bar_open), "failed appending bar_open",
                               error) ||
            !ExpectArrowStatus(bar_high_builder.Append(row.bar_high), "failed appending bar_high",
                               error) ||
            !ExpectArrowStatus(bar_low_builder.Append(row.bar_low), "failed appending bar_low",
                               error) ||
            !ExpectArrowStatus(bar_close_builder.Append(row.bar_close),
                               "failed appending bar_close", error) ||
            !ExpectArrowStatus(bar_volume_builder.Append(row.bar_volume),
                               "failed appending bar_volume", error) ||
            !ExpectArrowStatus(
                market_regime_builder.Append(static_cast<std::uint8_t>(row.market_regime)),
                "failed appending market_regime", error) ||
            !append_optional(row.kama, &kama_builder, "kama") ||
            !append_optional(row.atr, &atr_builder, "atr") ||
            !append_optional(row.adx, &adx_builder, "adx") ||
            !append_optional(row.er, &er_builder, "er") ||
            !append_optional(row.stop_loss_price, &stop_loss_price_builder, "stop_loss_price") ||
            !append_optional(
                row.take_profit_price, &take_profit_price_builder, "take_profit_price")) {
            return false;
        }
    }

    std::shared_ptr<arrow::Array> instrument_id_array;
    std::shared_ptr<arrow::Array> ts_ns_array;
    std::shared_ptr<arrow::Array> dt_utc_array;
    std::shared_ptr<arrow::Array> timeframe_minutes_array;
    std::shared_ptr<arrow::Array> strategy_id_array;
    std::shared_ptr<arrow::Array> strategy_type_array;
    std::shared_ptr<arrow::Array> bar_open_array;
    std::shared_ptr<arrow::Array> bar_high_array;
    std::shared_ptr<arrow::Array> bar_low_array;
    std::shared_ptr<arrow::Array> bar_close_array;
    std::shared_ptr<arrow::Array> bar_volume_array;
    std::shared_ptr<arrow::Array> kama_array;
    std::shared_ptr<arrow::Array> atr_array;
    std::shared_ptr<arrow::Array> adx_array;
    std::shared_ptr<arrow::Array> er_array;
    std::shared_ptr<arrow::Array> stop_loss_price_array;
    std::shared_ptr<arrow::Array> take_profit_price_array;
    std::shared_ptr<arrow::Array> market_regime_array;

    if (!FinishArray(&instrument_id_builder, "instrument_id", &instrument_id_array, error) ||
        !FinishArray(&ts_ns_builder, "ts_ns", &ts_ns_array, error) ||
        !FinishArray(&dt_utc_builder, "dt_utc", &dt_utc_array, error) ||
        !FinishArray(&timeframe_minutes_builder, "timeframe_minutes", &timeframe_minutes_array,
                     error) ||
        !FinishArray(&strategy_id_builder, "strategy_id", &strategy_id_array, error) ||
        !FinishArray(&strategy_type_builder, "strategy_type", &strategy_type_array, error) ||
        !FinishArray(&bar_open_builder, "bar_open", &bar_open_array, error) ||
        !FinishArray(&bar_high_builder, "bar_high", &bar_high_array, error) ||
        !FinishArray(&bar_low_builder, "bar_low", &bar_low_array, error) ||
        !FinishArray(&bar_close_builder, "bar_close", &bar_close_array, error) ||
        !FinishArray(&bar_volume_builder, "bar_volume", &bar_volume_array, error) ||
        !FinishArray(&kama_builder, "kama", &kama_array, error) ||
        !FinishArray(&atr_builder, "atr", &atr_array, error) ||
        !FinishArray(&adx_builder, "adx", &adx_array, error) ||
        !FinishArray(&er_builder, "er", &er_array, error) ||
        !FinishArray(
            &stop_loss_price_builder, "stop_loss_price", &stop_loss_price_array, error) ||
        !FinishArray(&take_profit_price_builder, "take_profit_price", &take_profit_price_array,
                     error) ||
        !FinishArray(&market_regime_builder, "market_regime", &market_regime_array, error)) {
        return false;
    }

    auto schema = arrow::schema({
        arrow::field("instrument_id", arrow::utf8(), false),
        arrow::field("ts_ns", arrow::int64(), false),
        arrow::field("dt_utc", arrow::utf8(), false),
        arrow::field("timeframe_minutes", arrow::int32(), false),
        arrow::field("strategy_id", arrow::utf8(), false),
        arrow::field("strategy_type", arrow::utf8(), false),
        arrow::field("bar_open", arrow::float64(), false),
        arrow::field("bar_high", arrow::float64(), false),
        arrow::field("bar_low", arrow::float64(), false),
        arrow::field("bar_close", arrow::float64(), false),
        arrow::field("bar_volume", arrow::float64(), false),
        arrow::field("kama", arrow::float64(), true),
        arrow::field("atr", arrow::float64(), true),
        arrow::field("adx", arrow::float64(), true),
        arrow::field("er", arrow::float64(), true),
        arrow::field("stop_loss_price", arrow::float64(), true),
        arrow::field("take_profit_price", arrow::float64(), true),
        arrow::field("market_regime", arrow::uint8(), false),
    });

    auto table = arrow::Table::Make(schema,
                                    {instrument_id_array,
                                     ts_ns_array,
                                     dt_utc_array,
                                     timeframe_minutes_array,
                                     strategy_id_array,
                                     strategy_type_array,
                                     bar_open_array,
                                     bar_high_array,
                                     bar_low_array,
                                     bar_close_array,
                                     bar_volume_array,
                                     kama_array,
                                     atr_array,
                                     adx_array,
                                     er_array,
                                     stop_loss_price_array,
                                     take_profit_price_array,
                                     market_regime_array});

    const std::filesystem::path output_path(output_path_);
    const std::filesystem::path tmp_path(output_path_ + ".tmp");

    auto file_result = arrow::io::FileOutputStream::Open(tmp_path.string());
    if (!file_result.ok()) {
        return SetError("failed to open sub-strategy indicator trace parquet output: " +
                            file_result.status().ToString(),
                        error);
    }
    std::shared_ptr<arrow::io::FileOutputStream> output_stream = file_result.ValueOrDie();

    parquet::WriterProperties::Builder writer_props_builder;
    writer_props_builder.compression(parquet::Compression::SNAPPY);
    std::shared_ptr<parquet::WriterProperties> writer_props = writer_props_builder.build();
    parquet::ArrowWriterProperties::Builder arrow_props_builder;
    std::shared_ptr<parquet::ArrowWriterProperties> arrow_props = arrow_props_builder.build();

    const auto write_status = parquet::arrow::WriteTable(
        *table, arrow::default_memory_pool(), output_stream,
        std::max<std::int64_t>(1, rows_written_), writer_props, arrow_props);
    if (!write_status.ok()) {
        std::error_code ec;
        std::filesystem::remove(tmp_path, ec);
        return SetError(
            "failed to write sub-strategy indicator trace parquet: " + write_status.ToString(),
            error);
    }

    const auto close_status = output_stream->Close();
    if (!close_status.ok()) {
        std::error_code ec;
        std::filesystem::remove(tmp_path, ec);
        return SetError(
            "failed to close sub-strategy indicator trace parquet file: " + close_status.ToString(),
            error);
    }

    try {
        std::error_code ec;
        std::filesystem::remove(output_path, ec);
        std::filesystem::rename(tmp_path, output_path);
    } catch (const std::exception& ex) {
        std::error_code ec;
        std::filesystem::remove(tmp_path, ec);
        return SetError(
            std::string("failed to finalize sub-strategy indicator trace parquet: ") + ex.what(),
            error);
    }

    rows_.clear();
    is_open_ = false;
    return true;
#endif
}

}  // namespace quant_hft
