#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <memory>
#include <string>

#include "quant_hft/backtest/backtest_data_feed.h"
#include "quant_hft/backtest/broker.h"
#include "quant_hft/backtest/engine.h"
#include "quant_hft/backtest/performance.h"
#include "quant_hft/strategy/base_strategy.h"

namespace py = pybind11;

namespace quant_hft {

class PyStrategy : public Strategy {
public:
    using Strategy::Strategy;

    void Initialize() override {
        PYBIND11_OVERRIDE_NAME(void, Strategy, "initialize", Initialize);
    }

    void OnTick(const Tick& tick) override {
        py::gil_scoped_acquire gil;
        PYBIND11_OVERRIDE_NAME(void, Strategy, "on_tick", OnTick, tick);
    }

    void OnBar(const Bar& bar) override {
        py::gil_scoped_acquire gil;
        PYBIND11_OVERRIDE_NAME(void, Strategy, "on_bar", OnBar, bar);
    }

    void OnOrder(const Order& order) override {
        py::gil_scoped_acquire gil;
        PYBIND11_OVERRIDE_NAME(void, Strategy, "on_order", OnOrder, order);
    }

    void OnTrade(const Trade& trade) override {
        py::gil_scoped_acquire gil;
        PYBIND11_OVERRIDE_NAME(void, Strategy, "on_trade", OnTrade, trade);
    }
};

}  // namespace quant_hft

PYBIND11_MODULE(quant_hft_strategy, m) {
    using namespace quant_hft;
    using namespace quant_hft::backtest;

    py::class_<BrokerConfig>(m, "BrokerConfig")
        .def(py::init<>())
        .def_readwrite("initial_capital", &BrokerConfig::initial_capital)
        .def_readwrite("commission_rate", &BrokerConfig::commission_rate)
        .def_readwrite("slippage", &BrokerConfig::slippage)
        .def_readwrite("partial_fill_enabled", &BrokerConfig::partial_fill_enabled)
        .def_readwrite("close_today_commission_rate", &BrokerConfig::close_today_commission_rate);

    py::class_<Strategy, PyStrategy, std::shared_ptr<Strategy>>(m, "Strategy")
        .def(py::init<>())
        .def("initialize", &Strategy::Initialize)
        .def("on_tick", &Strategy::OnTick)
        .def("on_bar", &Strategy::OnBar)
        .def("on_order", &Strategy::OnOrder)
        .def("on_trade", &Strategy::OnTrade)
        .def("buy", &Strategy::Buy)
        .def("sell", &Strategy::Sell)
        .def("cancel_order", &Strategy::CancelOrder);

    py::class_<BacktestEngine>(m, "BacktestEngine")
        .def(py::init([](const std::string& parquet_root,
                         const Timestamp& start,
                         const Timestamp& end,
                         std::shared_ptr<Strategy> strategy,
                         const BrokerConfig& config) {
            auto data_feed = std::make_unique<backtest::BacktestDataFeed>(parquet_root, start, end);
            auto broker = std::make_unique<backtest::SimulatedBroker>(config);
            return std::make_unique<backtest::BacktestEngine>(
                std::move(data_feed), std::move(broker), std::move(strategy));
        }))
        .def("run", &BacktestEngine::Run)
        .def("get_result", [](const BacktestEngine& engine) {
            const auto result = engine.GetResult();
            const auto performance = AnalyzePerformance(result);
            py::dict payload;
            payload["orders"] = result.orders.size();
            payload["trades"] = result.trades.size();
            payload["equity_points"] = result.equity_curve.size();
            if (!result.equity_curve.empty()) {
                payload["last_balance"] = result.equity_curve.back().balance;
            } else {
                payload["last_balance"] = 0.0;
            }
            py::dict perf;
            perf["initial_balance"] = performance.initial_balance;
            perf["final_balance"] = performance.final_balance;
            perf["net_profit"] = performance.net_profit;
            perf["total_return"] = performance.total_return;
            perf["max_drawdown"] = performance.max_drawdown;
            perf["max_drawdown_ratio"] = performance.max_drawdown_ratio;
            perf["return_volatility"] = performance.return_volatility;
            perf["sharpe_ratio"] = performance.sharpe_ratio;
            perf["order_count"] = performance.order_count;
            perf["trade_count"] = performance.trade_count;
            perf["commission_paid"] = performance.commission_paid;
            payload["performance"] = perf;
            return payload;
        });
}
