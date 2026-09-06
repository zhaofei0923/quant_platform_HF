
#include <chrono>
#include <filesystem>
#include <iostream>
#include <sys/resource.h>
#include "quant_hft/backtest/parquet_data_feed.h"
#include "tick_partition_fixture.h"
using namespace quant_hft;
int main(int argc, char** argv) {
 if(argc!=4) return 2;
 const std::filesystem::path path(argv[2]); const long long rows=std::stoll(argv[3]);
 std::string error;
 constexpr long long base=1704186000000000000LL;
 if(std::string(argv[1])=="generate") {
   std::vector<Tick> ticks; ticks.reserve(rows);
   for(long long i=0;i<rows;++i) {
    Tick t; t.symbol="DCE.c2605"; t.exchange="DCE"; t.ts_ns=base+i*1000000;
    t.last_price=100+(i%101)*0.01; t.bid_price1=t.last_price-0.01; t.ask_price1=t.last_price+0.01;
    t.volume=100+i; t.open_interest=10000+(i%31); ticks.push_back(t);
   }
   if(!backtest::test::WriteTickPartitionFixture(path,ticks,&error)) {std::cerr<<error;return 1;}
   return 0;
 }
 ParquetPartitionMeta partition; partition.file_path=path.string();partition.instrument_id="DCE.c2605";
 partition.min_ts_ns=base;partition.max_ts_ns=base+(rows-1)*1000000;partition.row_count=rows;
 ParquetTickCursor cursor;
 const auto started=std::chrono::steady_clock::now();
 if(!cursor.Open(partition,Timestamp(0),Timestamp(4102444799000000000LL),{},4096,&error)){std::cerr<<error;return 1;}
 long long read=0; Tick tick;bool has;
 for(;;){if(!cursor.Next(&tick,&has,&error)){std::cerr<<error;return 1;} if(!has)break;++read;}
 rusage usage{};getrusage(RUSAGE_SELF,&usage);
 auto ms=std::chrono::duration<double,std::milli>(std::chrono::steady_clock::now()-started).count();
 std::cout<<"{\"rows\":"<<read<<",\"resident_input_rows_high_water\":"<<cursor.metrics().buffered_rows_high_water
 <<",\"file_bytes\":"<<std::filesystem::file_size(path)<<",\"peak_rss_kib\":"<<usage.ru_maxrss
 <<",\"elapsed_ms\":"<<ms<<"}\n";
 return read==rows?0:1;
}
