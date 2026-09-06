
#include <chrono>
#include <cmath>
#include <iostream>
#include <sys/resource.h>
#include "quant_hft/strategy/composite_strategy.h"
#include "quant_hft/strategy/atomic_factory.h"
using namespace quant_hft;
int main(int argc,char**argv){
 if(argc!=2)return 2;const long long count=std::stoll(argv[1]);std::string error;
 if(!RegisterBuiltinAtomicStrategies(&error)){std::cerr<<error;return 1;}
 CompositeStrategyDefinition def;def.run_type="backtest";def.market_state_mode=false;
 SubStrategyDefinition sub;sub.id="kama_bench";sub.type="KamaTrendStrategy";sub.timeframe_minutes=5;
 sub.params={{"id","kama_bench"},{"er_period","10"},{"fast_period","2"},{"slow_period","30"},
 {"std_period","10"},{"default_volume","1"},{"stop_loss_mode","trailing_atr"},
 {"stop_loss_atr_period","14"},{"stop_loss_atr_multiplier","4.0"},
 {"take_profit_mode","atr_target"},{"take_profit_atr_period","14"},{"take_profit_atr_multiplier","30.0"}};
 def.sub_strategies.push_back(sub);CompositeStrategy strategy(def);
 StrategyContext ctx;ctx.strategy_id="composite_bench";ctx.account_id="synthetic_benchmark";
 ctx.metadata={{"run_type","backtest"},{"parameter_profile","sim"},{"timestamp_basis","utc"}};
 strategy.Initialize(ctx);strategy.SetBacktestAccountSnapshot(200000,0);
 constexpr EpochNanos base=1778806800000000000LL;
 for(int i=0;i<100;++i){StateSnapshot7D state;state.instrument_id="DCE.c2605";state.has_bar=true;
 state.timeframe_minutes=5;state.ts_ns=base+i*300000000000LL;state.market_regime=MarketRegime::kStrongTrend;
 state.bar_open=state.analysis_bar_open=100;state.bar_high=state.analysis_bar_high=102;
 state.bar_low=state.analysis_bar_low=98;state.bar_close=state.analysis_bar_close=100+std::sin(i*.1);
 state.bar_volume=1000;(void)strategy.OnState(state);}
 OrderEvent fill;fill.strategy_id="kama_bench";fill.instrument_id="DCE.c2605";
 fill.client_order_id=fill.exchange_order_id="synthetic_open";fill.side=Side::kBuy;fill.offset=OffsetFlag::kOpen;
 fill.status=OrderStatus::kFilled;fill.total_volume=fill.filled_volume=1;fill.avg_fill_price=100;fill.ts_ns=base;
 strategy.OnOrderEvent(fill);
 if(strategy.GetBacktestPositionOwner("DCE.c2605")!="kama_bench") return 3;
 auto started=std::chrono::steady_clock::now();long long intents=0;
 for(long long i=0;i<count;++i){MarketSnapshot tick;tick.instrument_id="DCE.c2605";tick.exchange_id="DCE";
 tick.last_price=100+0.01*(i%3);tick.exchange_ts_ns=tick.recv_ts_ns=base+i*1000000;
 intents+=strategy.OnMarketTick(tick).size();}
 auto ms=std::chrono::duration<double,std::milli>(std::chrono::steady_clock::now()-started).count();
 rusage usage{};getrusage(RUSAGE_SELF,&usage);
 std::cout<<"{\"ticks\":"<<count<<",\"warmup_5m_bars\":100,\"held_volume\":1,\"emitted_intents\":"<<intents
 <<",\"elapsed_ms\":"<<ms<<",\"peak_rss_kib\":"<<usage.ru_maxrss<<"}\n";
}
