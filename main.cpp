

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/main/config.hpp"
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

void run_tpch_query(duckdb::Connection& conn, int query) {
    auto res = conn.Query(fmt::format("pragma tpch({})", query));
    if (res->HasError()) {
        throw std::runtime_error{fmt::format("got error running tpch {}. reason: {}", query, res->GetError())};
    }
}

void run_all_tpch_queries(duckdb::Connection& conn) {
    PerfEventGroup events(PERF_COUNT_HW_CPU_CYCLES, PERF_TYPE_HARDWARE, "cycles");
    events.AddEvent(PERF_COUNT_HW_INSTRUCTIONS, PERF_TYPE_HARDWARE, "ins");
    events.AddEvent(0xff45, PERF_TYPE_RAW, "dTLB_misses");

    for (int i = 1; i <= 22; i++) {
        events.Enable(true);
        auto t1 = std::chrono::steady_clock::now();
        run_tpch_query(conn, i);
        auto t2 = std::chrono::steady_clock::now();
        events.Disable();
        std::cout << "query " << i << " took " <<
            std::chrono::duration<double, std::ratio<1, 1>>(t2 - t1).count()
            << " seconds " << std::endl;
        auto revents = events.ReadEvents();
        for (auto &key : revents) {
            std::cout << key.first << ", " << *key.second << std::endl;
        }
    }
}

void run_queries(duckdb::Connection& conn, std::string query) {
    auto res = conn.Query(query);
    if (res->HasError()) {
        throw std::runtime_error{fmt::format("got error: message = {}", res->GetError())};
    }
}

inline void serialize() {
    asm volatile("xor %%eax, %%eax \n cpuid " ::: "%eax","%ebx","%ecx","%edx");
}


template<typename Callable>
void profile_code(const Callable& c) {
    pid_t pid = fork();
    if (pid > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        c();
        kill(pid, SIGTERM);
        int loc;
        waitpid(pid, &loc, 0);
    } else if (pid == 0) {
        pid_t parent_pid = getppid();
        std::string cmd = fmt::format("perf record -F 99 -p {} --call-graph dwarf", parent_pid);
        system(cmd.c_str());
        exit(EXIT_SUCCESS);
    }
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
    serialize();
    conn.Query("select sum(a) from x;");
    PerfEventGroup events(PERF_COUNT_HW_CPU_CYCLES, PERF_TYPE_HARDWARE, "cycles");
    events.AddEvent(PERF_COUNT_HW_INSTRUCTIONS, PERF_TYPE_HARDWARE, "ins");

    events.Enable();
    serialize();
    res = conn.Query("set threads = 1; select sum(a) from x;");
    serialize();
    events.Disable();

    auto n = events.ReadEvents();

    auto p = res->Fetch()->GetValue(0, 0);

    std::cout << "res = " << p << std::endl;
    std::cout << "cycles = " << *n["cycles"] << ", " << "ins = "
        << *n["ins"] << ", IPC = " << (double)*n["ins"]/ *n["cycles"] << ", ms = " << *n["wall_clock_ms"] << std::endl;


    serialize();
}

// typedef data_ptr_t (*allocate_function_ptr_t)(PrivateAllocatorData *private_data, idx_t size);
// typedef void (*free_function_ptr_t)(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size);
// typedef data_ptr_t (*reallocate_function_ptr_t)(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
//                                                 idx_t size);


// static char* mem;
// static std::vector<int> free_blocks;

// static void init_mem() {

// }


static duckdb::data_ptr_t allocate_fun(duckdb::PrivateAllocatorData *private_data, duckdb::idx_t size) {

}

static void free_fun(duckdb::PrivateAllocatorData* private_data, duckdb::data_ptr_t pointer, duckdb::idx_t size) {

}

static duckdb::data_ptr_t realloc_fun(duckdb::PrivateAllocatorData* private_data, duckdb::data_ptr_t pointer, duckdb::idx_t old_size) {

}


int main() {
    duckdb::DuckDB db;
    duckdb::Connection conn(db);

    //sum_numbers();

    //profile_code([&conn]() { ddb_sum(conn); });
    run_tpch(conn);
    run_all_tpch_queries(conn);

    return 0;
}