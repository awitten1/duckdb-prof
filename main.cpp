

#include "duckdb/main/connection.hpp"
#include "perfcxx/perf-lib.hpp"
#include <algorithm>
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

inline void serialize() {
    asm volatile("xor %%eax, %%eax \n cpuid " ::: "%eax","%ebx","%ecx","%edx");
}

void sum_numbers() {
    std::vector<uint64_t> numbers(1000000000);

    std::transform(numbers.begin(), numbers.end(), numbers.begin(), [](int) {
        return rand() % 1000000;
    });
    PerfEventGroup events(PERF_COUNT_HW_CPU_CYCLES, PERF_TYPE_HARDWARE, "cycles");
    events.AddEvent(PERF_COUNT_HW_INSTRUCTIONS, PERF_TYPE_HARDWARE, "ins");

    uint64_t res = 0;

    events.Enable();
    serialize();
    for (int i = 0; i < numbers.size(); ++i) {
        res += numbers[i];
    }
    serialize();
    events.Disable();

    auto n = events.ReadEvents();

    std::cout << "res = " << res << std::endl;
    std::cout << "cycles = " << *n["cycles"] << ", " << "ins = "
        << *n["ins"] << ", IPC = " << (double)*n["ins"]/ *n["cycles"] << ", ms = " << *n["wall_clock_ms"] << std::endl;

}

void ddb_sum(duckdb::Connection& conn) {
    auto res = conn.Query("ATTACH ':memory:' as mem; use mem; ");
    if (res->HasError()) {
        throw std::runtime_error{"blah"};
    }
    conn.Query("create table x as select (random()*1000000)::ubigint a from range(1000000000);");


    PerfEventGroup events(PERF_COUNT_HW_CPU_CYCLES, PERF_TYPE_HARDWARE, "cycles");
    events.AddEvent(PERF_COUNT_HW_INSTRUCTIONS, PERF_TYPE_HARDWARE, "ins");

    events.Enable();
    serialize();
    res = conn.Query("select sum(a) from x;");
    serialize();
    events.Disable();

    auto n = events.ReadEvents();

    auto p = res->Fetch()->GetValue(0, 0);

    std::cout << "res = " << p << std::endl;
    std::cout << "cycles = " << *n["cycles"] << ", " << "ins = "
        << *n["ins"] << ", IPC = " << (double)*n["ins"]/ *n["cycles"] << ", ms = " << *n["wall_clock_ms"] << std::endl;


    serialize();
}

int main() {
    duckdb::DuckDB db;
    duckdb::Connection conn(db);

    sum_numbers();

    ddb_sum(conn);

    return 0;
}