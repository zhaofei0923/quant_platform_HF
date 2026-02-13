#include "quant_hft/backtest/engine.h"

#include <utility>

#include "quant_hft/backtest/broker.h"
#include "quant_hft/interfaces/data_feed.h"
#include "quant_hft/strategy/base_strategy.h"

namespace quant_hft::backtest {

BacktestEngine::BacktestEngine(std::unique_ptr<DataFeed> data_feed,
                               std::unique_ptr<SimulatedBroker> broker,
                               std::shared_ptr<Strategy> strategy)
    : data_feed_(std::move(data_feed)),
      broker_(std::move(broker)),
      strategy_(std::move(strategy)) {
    strategy_->BindContext(data_feed_.get(), broker_.get());

    broker_->SetOrderCallback([this](const Order& order) { OnOrderUpdate(order); });
    broker_->SetFillCallback([this](const Trade& trade) { OnFill(trade); });
}

void BacktestEngine::Run() {
    strategy_->Initialize();

    data_feed_->Subscribe(
        {},
        [this](const Tick& tick) {
            Event event;
            event.type = EventType::kMarket;
            event.time = Timestamp(tick.ts_ns);
            event.data = tick;
            ProcessEvent(event);
        },
        nullptr);

    data_feed_->Run();
}

BacktestResult BacktestEngine::GetResult() const {
    return result_;
}

void BacktestEngine::SetTimeRange(const Timestamp& start, const Timestamp& end) {
    start_time_ = start;
    end_time_ = end;
}

void BacktestEngine::ProcessEvent(const Event& event) {
    switch (event.type) {
        case EventType::kMarket:
            OnMarketData(std::get<Tick>(event.data));
            return;
        case EventType::kOrder:
            OnOrderUpdate(std::get<Order>(event.data));
            return;
        case EventType::kFill:
            OnFill(std::get<Trade>(event.data));
            return;
        case EventType::kSignal:
            return;
    }
}

void BacktestEngine::OnMarketData(const Tick& tick) {
    strategy_->OnTick(tick);
    broker_->OnTick(tick);

    EquityPoint point;
    point.time = Timestamp(tick.ts_ns);
    point.balance = broker_->GetAccountBalance();
    result_.equity_curve.push_back(point);
}

void BacktestEngine::OnOrderUpdate(const Order& order) {
    result_.orders.push_back(order);
    strategy_->OnOrder(order);
}

void BacktestEngine::OnFill(const Trade& trade) {
    result_.trades.push_back(trade);
    strategy_->OnTrade(trade);
}

}  // namespace quant_hft::backtest
