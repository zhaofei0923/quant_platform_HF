#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "quant_hft/contracts/types.h"
#include "quant_hft/core/ctp_md_adapter.h"
#include "quant_hft/core/ctp_trader_adapter.h"

namespace py = pybind11;

namespace quant_hft {
namespace {

std::string DictString(const py::dict& cfg, const char* key, const std::string& fallback = "") {
    if (!cfg.contains(py::str(key))) {
        return fallback;
    }
    return py::cast<std::string>(cfg[py::str(key)]);
}

int DictInt(const py::dict& cfg, const char* key, int fallback) {
    if (!cfg.contains(py::str(key))) {
        return fallback;
    }
    return py::cast<int>(cfg[py::str(key)]);
}

bool DictBool(const py::dict& cfg, const char* key, bool fallback) {
    if (!cfg.contains(py::str(key))) {
        return fallback;
    }
    return py::cast<bool>(cfg[py::str(key)]);
}

MarketDataConnectConfig ParseConnectConfig(const py::dict& cfg) {
    MarketDataConnectConfig out;
    out.market_front_address = DictString(cfg, "market_front_address");
    out.trader_front_address = DictString(cfg, "trader_front_address");
    out.flow_path = DictString(cfg, "flow_path", "./ctp_flow");
    out.broker_id = DictString(cfg, "broker_id");
    out.user_id = DictString(cfg, "user_id");
    out.investor_id = DictString(cfg, "investor_id", out.user_id);
    out.password = DictString(cfg, "password");
    out.app_id = DictString(cfg, "app_id");
    out.auth_code = DictString(cfg, "auth_code");
    out.is_production_mode = DictBool(cfg, "is_production_mode", false);
    out.enable_real_api = DictBool(cfg, "enable_real_api", false);
    out.enable_terminal_auth = DictBool(cfg, "enable_terminal_auth", true);
    out.connect_timeout_ms = DictInt(cfg, "connect_timeout_ms", 10000);
    out.reconnect_max_attempts = DictInt(cfg, "reconnect_max_attempts", 8);
    out.reconnect_initial_backoff_ms = DictInt(cfg, "reconnect_initial_backoff_ms", 500);
    out.reconnect_max_backoff_ms = DictInt(cfg, "reconnect_max_backoff_ms", 8000);
    out.query_retry_backoff_ms = DictInt(cfg, "query_retry_backoff_ms", 200);
    out.recovery_quiet_period_ms = DictInt(cfg, "recovery_quiet_period_ms", 3000);
    out.settlement_confirm_required = DictBool(cfg, "settlement_confirm_required", true);
    return out;
}

OrderIntent ParseOrderIntent(const py::dict& req) {
    OrderIntent intent;
    intent.account_id = DictString(req, "account_id");
    intent.client_order_id = DictString(req, "client_order_id");
    intent.strategy_id = DictString(req, "strategy_id");
    intent.instrument_id = DictString(req, "instrument_id");
    intent.volume = DictInt(req, "volume", 0);
    if (req.contains(py::str("price"))) {
        intent.price = py::cast<double>(req[py::str("price")]);
    } else if (req.contains(py::str("limit_price"))) {
        intent.price = py::cast<double>(req[py::str("limit_price")]);
    }
    intent.trace_id = DictString(req, "trace_id", intent.client_order_id);
    return intent;
}

py::dict ToOrderEventDict(const OrderEvent& event) {
    py::dict out;
    out["account_id"] = event.account_id;
    out["client_order_id"] = event.client_order_id;
    out["instrument_id"] = event.instrument_id;
    out["status"] = static_cast<int>(event.status);
    out["total_volume"] = event.total_volume;
    out["filled_volume"] = event.filled_volume;
    out["avg_fill_price"] = event.avg_fill_price;
    out["reason"] = event.reason;
    out["trace_id"] = event.trace_id;
    out["ts_ns"] = event.ts_ns;
    return out;
}

py::dict ToTickDict(const MarketSnapshot& snapshot) {
    py::dict out;
    out["instrument_id"] = snapshot.instrument_id;
    out["last_price"] = snapshot.last_price;
    out["bid_price_1"] = snapshot.bid_price_1;
    out["ask_price_1"] = snapshot.ask_price_1;
    out["bid_volume_1"] = snapshot.bid_volume_1;
    out["ask_volume_1"] = snapshot.ask_volume_1;
    out["volume"] = snapshot.volume;
    out["ts_ns"] = snapshot.recv_ts_ns;
    return out;
}

class PyCTPTraderAdapter {
public:
    PyCTPTraderAdapter(std::size_t query_qps_limit, std::size_t dispatcher_workers)
        : impl_(std::make_shared<CTPTraderAdapter>(query_qps_limit, dispatcher_workers)) {}

    bool connect(const py::dict& config) {
        return impl_->Connect(ParseConnectConfig(config));
    }

    void disconnect() {
        impl_->Disconnect();
    }

    bool confirm_settlement() {
        return impl_->ConfirmSettlement();
    }

    bool is_ready() const {
        return impl_->IsReady();
    }

    bool place_order(const py::dict& request) {
        return impl_->PlaceOrder(ParseOrderIntent(request));
    }

    bool cancel_order(const std::string& client_order_id, const std::string& trace_id) {
        return impl_->CancelOrder(client_order_id, trace_id);
    }

    void on_order_status(py::function callback) {
        impl_->RegisterOrderEventCallback(
            [callback = std::move(callback)](const OrderEvent& event) {
                py::gil_scoped_acquire gil;
                callback(ToOrderEventDict(event));
            });
    }

private:
    std::shared_ptr<CTPTraderAdapter> impl_;
};

class PyCTPMdAdapter {
public:
    PyCTPMdAdapter(std::size_t query_qps_limit, std::size_t dispatcher_workers)
        : impl_(std::make_shared<CTPMdAdapter>(query_qps_limit, dispatcher_workers)) {}

    bool connect(const py::dict& config) {
        return impl_->Connect(ParseConnectConfig(config));
    }

    void disconnect() {
        impl_->Disconnect();
    }

    bool is_ready() const {
        return impl_->IsReady();
    }

    bool subscribe(const std::vector<std::string>& instruments) {
        return impl_->Subscribe(instruments);
    }

    bool unsubscribe(const std::vector<std::string>& instruments) {
        return impl_->Unsubscribe(instruments);
    }

    void on_tick(py::function callback) {
        impl_->RegisterTickCallback(
            [callback = std::move(callback)](const MarketSnapshot& snapshot) {
                py::gil_scoped_acquire gil;
                callback(ToTickDict(snapshot));
            });
    }

private:
    std::shared_ptr<CTPMdAdapter> impl_;
};

}  // namespace
}  // namespace quant_hft

PYBIND11_MODULE(_ctp_wrapper, m) {
    py::class_<quant_hft::PyCTPTraderAdapter>(m, "CTPTraderAdapter")
        .def(py::init<std::size_t, std::size_t>(), py::arg("query_qps_limit") = 10,
             py::arg("dispatcher_workers") = 1)
        .def("connect", &quant_hft::PyCTPTraderAdapter::connect)
        .def("disconnect", &quant_hft::PyCTPTraderAdapter::disconnect)
        .def("confirm_settlement", &quant_hft::PyCTPTraderAdapter::confirm_settlement)
        .def("is_ready", &quant_hft::PyCTPTraderAdapter::is_ready)
        .def("place_order", &quant_hft::PyCTPTraderAdapter::place_order)
        .def("cancel_order", &quant_hft::PyCTPTraderAdapter::cancel_order)
        .def("on_order_status", &quant_hft::PyCTPTraderAdapter::on_order_status);

    py::class_<quant_hft::PyCTPMdAdapter>(m, "CTPMdAdapter")
        .def(py::init<std::size_t, std::size_t>(), py::arg("query_qps_limit") = 10,
             py::arg("dispatcher_workers") = 1)
        .def("connect", &quant_hft::PyCTPMdAdapter::connect)
        .def("disconnect", &quant_hft::PyCTPMdAdapter::disconnect)
        .def("is_ready", &quant_hft::PyCTPMdAdapter::is_ready)
        .def("subscribe", &quant_hft::PyCTPMdAdapter::subscribe)
        .def("unsubscribe", &quant_hft::PyCTPMdAdapter::unsubscribe)
        .def("on_tick", &quant_hft::PyCTPMdAdapter::on_tick);
}
