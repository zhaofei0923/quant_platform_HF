#include <iostream>
#include <map>
#include <string>
#include <vector>
#include "quant_hft/apps/backtest_replay_support.h"
#include "quant_hft/services/timeframe_state_fanout.h"
using namespace quant_hft;
namespace qa = quant_hft::apps;
BarSnapshot make_bar(int minute) {
    BarSnapshot b;
    b.instrument_id="DCE.c2605"; b.exchange_id="DCE"; b.trading_day="20260515"; b.action_day=b.trading_day;
    char time[32]; std::snprintf(time,sizeof(time),"20260515 09:%02d",minute); b.minute=time;
    b.open=b.analysis_open=100+minute; b.high=b.analysis_high=102+minute;
    b.low=b.analysis_low=99+minute; b.close=b.analysis_close=101+minute;
    b.volume=10; b.ts_ns=(1778806800LL+minute*60LL+59LL)*1000000000LL;
    return b;
}
qa::detail::ReplayBarTickContext ctx(const BarSnapshot& b) {
    qa::detail::ReplayBarTickContext c; c.initialized=true;
    c.first_tick.instrument_id=c.last_tick.instrument_id=b.instrument_id;
    c.first_tick.exchange_id=c.last_tick.exchange_id=b.exchange_id;
    c.first_tick.trading_day=c.last_tick.trading_day=b.trading_day;
    c.first_tick.ts_ns=c.last_tick.ts_ns=b.ts_ns;
    c.first_tick.last_price=c.last_tick.last_price=b.close;
    return c;
}
int main() {
    qa::detail::ReplayTimeframeFanout replay({1,5});
    TimeframeStateFanout live({1,5});
    // Same detector ownership as RunBacktestSpec: instrument-only map (header:4015,5342).
    std::map<std::string,MarketStateDetector> replay_detectors;
    std::size_t replay_five_seen=0,live_five_seen=0;
    for(int m=0;m<=5;++m) {
        auto b=make_bar(m);
        for(const auto& e:replay.OnOneMinuteBar(b,ctx(b))) {
            auto it=replay_detectors.try_emplace(b.instrument_id).first;
            auto s=qa::BuildStateSnapshotFromBar(e.context.first_tick,e.context.last_tick,e.bar,e.bar.ts_ns,e.timeframe_minutes,&it->second);
            if(e.timeframe_minutes==5) replay_five_seen=s.market_state_bars_seen;
        }
        for(const auto& e:live.OnOneMinuteBar(b)) if(e.timeframe_minutes==5) live_five_seen=e.state.market_state_bars_seen;
    }
    std::cout<<"same_first_5m_bucket: replay_detector_bars="<<replay_five_seen<<" live_detector_bars="<<live_five_seen<<"\n";
    qa::detail::ReplayTimeframeFanout replay_gap({5});
    TimeframeStateFanout live_gap({5});
    bool replay_gap_has_bar=false,live_gap_eligible=true;
    int live_observed=-1;
    for(int m:{0,1,3,4,5}) {
        auto b=make_bar(m);
        for(const auto& e:replay_gap.OnOneMinuteBar(b,ctx(b))) {
            auto s=qa::BuildStateSnapshotFromBar(e.context.first_tick,e.context.last_tick,e.bar,e.bar.ts_ns,e.timeframe_minutes);
            if(e.bar.minute=="20260515 09:00") replay_gap_has_bar=s.has_bar;
        }
        for(const auto& e:live_gap.OnOneMinuteBar(b)) if(e.bar.minute=="20260515 09:00") {
            live_gap_eligible=e.strategy_eligible; live_observed=e.bar.observed_source_bars;
        }
    }
    std::cout<<"missing_09:02_in_5m_bucket: replay_strategy_has_bar="<<replay_gap_has_bar<<" live_strategy_eligible="<<live_gap_eligible<<" live_observed="<<live_observed<<"/5\n";
    return (replay_five_seen>1 && live_five_seen==1 && replay_gap_has_bar && !live_gap_eligible && live_observed==4)?0:1;
}