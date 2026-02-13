#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <memory>
#include <string>
#include <vector>

#include "quant_hft/backtest/backtest_data_feed.h"
#include "quant_hft/backtest/live_data_feed.h"
#include "quant_hft/interfaces/data_feed.h"

namespace py = pybind11;
using namespace quant_hft;
using namespace quant_hft::backtest;

class PyDataFeed : public DataFeed {
public:
    using DataFeed::DataFeed;

    void Subscribe(const std::vector<std::string>& symbols,
                   std::function<void(const Tick&)> on_tick,
                   std::function<void(const Bar&)> on_bar) override {
        PYBIND11_OVERRIDE_PURE(void, DataFeed, Subscribe, symbols, on_tick, on_bar);
    }

    std::vector<Bar> GetHistoryBars(const std::string& symbol,
                                    const Timestamp& start,
                                    const Timestamp& end,
                                    const std::string& timeframe) override {
        PYBIND11_OVERRIDE_PURE(std::vector<Bar>, DataFeed, GetHistoryBars, symbol, start, end,
                               timeframe);
    }

    std::vector<Tick> GetHistoryTicks(const std::string& symbol,
                                      const Timestamp& start,
                                      const Timestamp& end) override {
        PYBIND11_OVERRIDE_PURE(std::vector<Tick>, DataFeed, GetHistoryTicks, symbol, start, end);
    }

    void Run() override {
        PYBIND11_OVERRIDE_PURE(void, DataFeed, Run);
    }

    void Stop() override {
        PYBIND11_OVERRIDE_PURE(void, DataFeed, Stop);
    }

    Timestamp CurrentTime() const override {
        PYBIND11_OVERRIDE_PURE(Timestamp, DataFeed, CurrentTime);
    }

    bool IsLive() const override {
        PYBIND11_OVERRIDE_PURE(bool, DataFeed, IsLive);
    }
};

PYBIND11_MODULE(quant_hft_data_feed, m) {
    m.doc() = "Quant Platform Data Feed bindings";

    py::class_<Timestamp>(m, "Timestamp")
        .def(py::init<>())
        .def_static("from_sql", &Timestamp::FromSql)
        .def_static("now", &Timestamp::Now)
        .def("to_sql", &Timestamp::ToSql)
        .def("to_epoch_nanos", &Timestamp::ToEpochNanos)
        .def("__str__", &Timestamp::ToSql);

    py::class_<Tick>(m, "Tick")
        .def(py::init<>())
        .def_readwrite("symbol", &Tick::symbol)
        .def_readwrite("exchange", &Tick::exchange)
        .def_readwrite("ts_ns", &Tick::ts_ns)
        .def_readwrite("last_price", &Tick::last_price)
        .def_readwrite("last_volume", &Tick::last_volume);

    py::class_<Bar>(m, "Bar")
        .def(py::init<>())
        .def_readwrite("symbol", &Bar::symbol)
        .def_readwrite("exchange", &Bar::exchange)
        .def_readwrite("timeframe", &Bar::timeframe)
        .def_readwrite("ts_ns", &Bar::ts_ns)
        .def_readwrite("open", &Bar::open)
        .def_readwrite("high", &Bar::high)
        .def_readwrite("low", &Bar::low)
        .def_readwrite("close", &Bar::close);

    py::class_<DataFeed, PyDataFeed, std::shared_ptr<DataFeed>>(m, "DataFeed")
        .def("subscribe", &DataFeed::Subscribe, py::arg("symbols"), py::arg("on_tick"),
             py::arg("on_bar") = nullptr)
        .def("get_history_bars", &DataFeed::GetHistoryBars, py::arg("symbol"),
             py::arg("start"), py::arg("end"), py::arg("timeframe"))
        .def("get_history_ticks", &DataFeed::GetHistoryTicks, py::arg("symbol"),
             py::arg("start"), py::arg("end"))
        .def("run", &DataFeed::Run)
        .def("stop", &DataFeed::Stop)
        .def("current_time", &DataFeed::CurrentTime)
        .def("is_live", &DataFeed::IsLive);

    py::class_<BacktestDataFeed, DataFeed, std::shared_ptr<BacktestDataFeed>>(m,
                                                                               "BacktestDataFeed")
        .def(py::init<const std::string&, const Timestamp&, const Timestamp&>(),
             py::arg("parquet_root"), py::arg("start"), py::arg("end"));

    py::class_<LiveDataFeed, DataFeed, std::shared_ptr<LiveDataFeed>>(m, "LiveDataFeed")
        .def(py::init<>());
}
