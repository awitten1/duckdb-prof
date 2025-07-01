

#include "duckdb/main/connection.hpp"
#include "perfcxx/perf-lib.hpp"
#include <iostream>
#include "duckdb.hpp"
#include <fmt/core.h>
#include <stdexcept>

void run_tpch(duckdb::Connection& conn) {

    auto res = conn.Query("ATTACH 'db' as db; use db; ");
    if (res->HasError()) {
        throw std::runtime_error{fmt::format("got error attaching db. reason: {}", res->GetError())};
    }

    res = conn.Query("install tpch; load tpch; ");
    if (res->HasError()) {
        throw std::runtime_error{fmt::format("got error loading tpch. reason: {}", res->GetError())};
    }
    res = conn.Query("from information_schema.tables select table_name");
    if (res->RowCount() > 0) {
        return;
    }
    res = conn.Query("CALL dbgen(sf = 20); ");
    if (res->HasError()) {
        throw std::runtime_error{fmt::format("error running dbgen. reason: {}", res->GetError())};
    }
}

void run_queries(duckdb::Connection& conn, int query) {
    auto res = conn.Query(fmt::format("pragma tpch({})", query));
    if (res->HasError()) {
        throw std::runtime_error{fmt::format("got error running tpch 18. reason: {}", res->GetError())};
    }
}

int main() {
    duckdb::DuckDB db;
    duckdb::Connection conn(db);
    run_tpch(conn);

    for (int i = 1; i <= 22; ++i) {
        std::cout << "\n\n" << "query " << i << std::endl;
        PerfEventGroup events(PERF_COUNT_HW_CPU_CYCLES, PERF_TYPE_HARDWARE, "cycles");
        events.AddEvent(PERF_COUNT_HW_INSTRUCTIONS, PERF_TYPE_HARDWARE, "ins");
        events.AddEvent(PERF_COUNT_HW_CACHE_L1D |
                (PERF_COUNT_HW_CACHE_OP_READ << 8) |
                (PERF_COUNT_HW_CACHE_RESULT_MISS << 16), PERF_TYPE_HW_CACHE, "l1d_cache_misses");

        events.AddEvent(PERF_COUNT_HW_CACHE_L1D |
                (PERF_COUNT_HW_CACHE_OP_PREFETCH << 8) |
                (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16), PERF_TYPE_HW_CACHE, "l1d_cache_prefetch");

        events.AddEvent(PERF_COUNT_HW_CACHE_L1D |
                (PERF_COUNT_HW_CACHE_OP_READ << 8) |
                (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16), PERF_TYPE_HW_CACHE, "l1d_cache_accesses");

        events.AddEvent(PERF_COUNT_HW_CACHE_MISSES, PERF_TYPE_HARDWARE, "llc_cache_misses");
        events.AddEvent(PERF_COUNT_HW_CACHE_REFERENCES, PERF_TYPE_HARDWARE, "llc_cache_accesses");

        events.Enable();
        run_queries(conn, i);
        events.Disable();

        auto counters = events.ReadEvents();
        for (const auto [k,v] : counters) {
            std::cout << k << ", " << *v << std::endl;
        }
        std::cout << "l1d cache miss rate = " << static_cast<double>(*counters["l1d_cache_misses"]) / (*counters["l1d_cache_accesses"]) << std::endl;
        std::cout << "l1d cache miss rate = " << static_cast<double>(*counters["l1d_cache_misses"]) / (*counters["l1d_cache_accesses"]) << std::endl;
        std::cout << "IPC = " << static_cast<double>(*counters["ins"])/(*counters["cycles"]) << std::endl;
    }
    return 0;
}