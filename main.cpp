

#include "duckdb/main/connection.hpp"
#include "perfcxx/perf-lib.hpp"
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include "duckdb.hpp"
#include <fmt/core.h>
#include <sched.h>
#include <stdexcept>
#include <thread>
#include <unistd.h>
#include <semaphore.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <vector>

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
    res = conn.Query("CALL dbgen(sf = 23); ");
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

void run_queries(duckdb::Connection& conn, std::string query) {
    pid_t pid = fork();

    if (pid == 0) {
        pid_t parent_pid = getppid();
        std::string cmd = fmt::format("perf record -F 99 -p {} -g", parent_pid);
        //system(cmd.c_str());
        exit(EXIT_SUCCESS);
    }
    auto res = conn.Query(query);
    if (res->HasError()) {
        throw std::runtime_error{fmt::format("got error: message = {}", res->GetError())};
    }
    ::kill(pid, SIGTERM);
    int loc;
    waitpid(pid, &loc, 0);
}

int main() {
    duckdb::DuckDB db;
    duckdb::Connection conn(db);
    run_tpch(conn);

    std::vector<std::string> queries = {
R"(
SELECT
    nation,
    o_year,
    sum(amount) AS sum_profit
FROM (
    SELECT
        n_name AS nation,
        extract(year FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
    FROM
        part,
        supplier,
        lineitem,
        partsupp,
        orders,
        nation
    WHERE
        s_suppkey = l_suppkey
        AND ps_suppkey = l_suppkey
        AND ps_partkey = l_partkey
        AND p_partkey = l_partkey
        AND o_orderkey = l_orderkey
        AND s_nationkey = n_nationkey
        AND p_name LIKE '%green%') AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC;

    )"
    };

    auto& events = GetGlobalPerfEventGroup();
    events.AddEvent(PERF_COUNT_HW_INSTRUCTIONS, PERF_TYPE_HARDWARE, "ins");
    events.AddEvent(PERF_COUNT_HW_CPU_CYCLES, PERF_TYPE_HARDWARE, "cycles");
    // events.AddEvent(PERF_COUNT_HW_CACHE_L1D |
    //         (PERF_COUNT_HW_CACHE_OP_READ << 8) |
    //         (PERF_COUNT_HW_CACHE_RESULT_MISS << 16), PERF_TYPE_HW_CACHE, "l1d_cache_misses");

    // events.AddEvent(PERF_COUNT_HW_CACHE_L1D |
    //         (PERF_COUNT_HW_CACHE_OP_READ << 8) |
    //         (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16), PERF_TYPE_HW_CACHE, "l1d_cache_accesses");

    events.AddEvent(PERF_COUNT_HW_CACHE_L1I |
            (PERF_COUNT_HW_CACHE_OP_READ << 8) |
            (PERF_COUNT_HW_CACHE_RESULT_MISS << 16), PERF_TYPE_HW_CACHE, "l1i_cache_misses");

    events.AddEvent(PERF_COUNT_HW_CACHE_L1I |
            (PERF_COUNT_HW_CACHE_OP_READ << 8) |
            (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16), PERF_TYPE_HW_CACHE, "l1i_cache_accesses");

    events.AddEvent(PERF_COUNT_HW_CACHE_MISSES, PERF_TYPE_HARDWARE, "llc_cache_misses");
    events.AddEvent(PERF_COUNT_HW_CACHE_REFERENCES, PERF_TYPE_HARDWARE, "llc_cache_accesses");
    run_queries(conn, queries[0]);
    auto counters = events.ReadEvents();
    for (const auto [k,v] : counters) {
        std::cout << k << ", " << *v << std::endl;
    }
    //std::cout << "l1d cache miss rate = " << static_cast<double>(*counters["l1d_cache_misses"]) / (*counters["l1d_cache_accesses"]) << std::endl;
    std::cout << "l1i cache miss rate = " << static_cast<double>(*counters["l1i_cache_misses"]) / (*counters["l1i_cache_accesses"]) << std::endl;
    //std::cout << "llc cache miss rate = " << static_cast<double>(*counters["llc_cache_misses"]) / (*counters["llc_cache_accesses"]) << std::endl;

    std::cout << "IPC = " << static_cast<double>(*counters["ins"])/(*counters["cycles"]) << std::endl;
    //int num_tpch_queries = 22;
    //for (int i = 1; i <= num_tpch_queries + queries.size(); ++i) {
        //std::cout << "\n\n" << "query " << i << std::endl;
        // PerfEventGroup events(PERF_COUNT_HW_CPU_CYCLES, PERF_TYPE_HARDWARE, "cycles");
        // events.AddEvent(PERF_COUNT_HW_INSTRUCTIONS, PERF_TYPE_HARDWARE, "ins");
        // // events.AddEvent(PERF_COUNT_HW_CACHE_L1D |
        // //         (PERF_COUNT_HW_CACHE_OP_READ << 8) |
        // //         (PERF_COUNT_HW_CACHE_RESULT_MISS << 16), PERF_TYPE_HW_CACHE, "l1d_cache_misses");

        // // events.AddEvent(PERF_COUNT_HW_CACHE_L1D |
        // //         (PERF_COUNT_HW_CACHE_OP_READ << 8) |
        // //         (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16), PERF_TYPE_HW_CACHE, "l1d_cache_accesses");

        // events.AddEvent(PERF_COUNT_HW_CACHE_L1I |
        //         (PERF_COUNT_HW_CACHE_OP_READ << 8) |
        //         (PERF_COUNT_HW_CACHE_RESULT_MISS << 16), PERF_TYPE_HW_CACHE, "l1i_cache_misses");

        // events.AddEvent(PERF_COUNT_HW_CACHE_L1I |
        //         (PERF_COUNT_HW_CACHE_OP_READ << 8) |
        //         (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16), PERF_TYPE_HW_CACHE, "l1i_cache_accesses");

        // events.AddEvent(PERF_COUNT_HW_CACHE_MISSES, PERF_TYPE_HARDWARE, "llc_cache_misses");
        // events.AddEvent(PERF_COUNT_HW_CACHE_REFERENCES, PERF_TYPE_HARDWARE, "llc_cache_accesses");

        //events.Enable();
        //if (i <= num_tpch_queries) {
            //run_queries(conn, i);
        //} else {
            //run_queries(conn, queries[i - num_tpch_queries - 1]);
        //}
        //events.Disable();

        // auto counters = events.ReadEvents();
        // for (const auto [k,v] : counters) {
        //     std::cout << k << ", " << *v << std::endl;
        // }
        // //std::cout << "l1d cache miss rate = " << static_cast<double>(*counters["l1d_cache_misses"]) / (*counters["l1d_cache_accesses"]) << std::endl;
        // std::cout << "l1i cache miss rate = " << static_cast<double>(*counters["l1i_cache_misses"]) / (*counters["l1i_cache_accesses"]) << std::endl;
        // //std::cout << "llc cache miss rate = " << static_cast<double>(*counters["llc_cache_misses"]) / (*counters["llc_cache_accesses"]) << std::endl;

        // std::cout << "IPC = " << static_cast<double>(*counters["ins"])/(*counters["cycles"]) << std::endl;
    //}
    return 0;
}