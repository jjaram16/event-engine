// Tests for BoundedQueue: basic semantics, backpressure, close(), and
// multi-producer / multi-consumer safety. Plain checks, no framework.
// CHECK is always active (unlike assert, it is not compiled out by NDEBUG).

#include "bounded_queue.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

#define CHECK(cond)                                                            \
    do {                                                                       \
        if (!(cond)) {                                                         \
            std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
            std::exit(1);                                                      \
        }                                                                      \
    } while (0)

using namespace std::chrono_literals;

static void test_basic_fifo() {
    BoundedQueue<int> q(4);
    CHECK(q.size() == 0);
    CHECK(q.push(1));
    CHECK(q.push(2));
    CHECK(q.push(3));
    CHECK(q.size() == 3);

    int v = 0;
    CHECK(q.pop(v) && v == 1);
    CHECK(q.pop(v) && v == 2);
    CHECK(q.pop(v) && v == 3);
    CHECK(q.size() == 0);
}

static void test_close_drains_then_fails() {
    BoundedQueue<int> q(4);
    CHECK(q.push(10));
    CHECK(q.push(20));
    q.close();

    CHECK(!q.push(30)); // push after close is rejected

    int v = 0;
    CHECK(q.pop(v) && v == 10); // already-queued items still drain
    CHECK(q.pop(v) && v == 20);
    CHECK(!q.pop(v));           // then pop reports closed+empty
}

static void test_push_blocks_when_full() {
    BoundedQueue<int> q(2);
    CHECK(q.push(1));
    CHECK(q.push(2));

    std::atomic<bool> pushed{false};
    std::thread producer([&] {
        CHECK(q.push(3)); // must block until space frees up
        pushed.store(true);
    });

    std::this_thread::sleep_for(100ms);
    CHECK(!pushed.load()); // still blocked: backpressure
    CHECK(q.size() == 2);  // capacity never exceeded

    int v = 0;
    CHECK(q.pop(v) && v == 1);
    producer.join();
    CHECK(pushed.load());
    CHECK(q.size() == 2);

    CHECK(q.pop(v) && v == 2);
    CHECK(q.pop(v) && v == 3);
}

static void test_close_unblocks_waiters() {
    // blocked producer is released with false
    BoundedQueue<int> full(1);
    CHECK(full.push(1));
    std::atomic<int> push_result{-1};
    std::thread producer([&] { push_result.store(full.push(2) ? 1 : 0); });
    std::this_thread::sleep_for(50ms);
    CHECK(push_result.load() == -1);
    full.close();
    producer.join();
    CHECK(push_result.load() == 0);

    // blocked consumer is released with false
    BoundedQueue<int> empty(1);
    std::atomic<int> pop_result{-1};
    std::thread consumer([&] {
        int v;
        pop_result.store(empty.pop(v) ? 1 : 0);
    });
    std::this_thread::sleep_for(50ms);
    CHECK(pop_result.load() == -1);
    empty.close();
    consumer.join();
    CHECK(pop_result.load() == 0);
}

// N producers push M tagged items each through a small queue (heavy
// contention); C consumers drain it. Every item must come out exactly once,
// each producer's items must stay in order, and size() must never exceed
// capacity.
static void test_mpmc_exactly_once() {
    constexpr unsigned kProducers = 8;
    constexpr unsigned kConsumers = 8;
    constexpr uint64_t kPerProducer = 50000;
    constexpr std::size_t kCapacity = 16;

    BoundedQueue<uint64_t> q(kCapacity);

    // item = (producer << 32) | seq
    std::vector<std::thread> producers;
    for (unsigned p = 0; p < kProducers; ++p) {
        producers.emplace_back([&, p] {
            for (uint64_t i = 0; i < kPerProducer; ++i) {
                CHECK(q.push((uint64_t(p) << 32) | i));
            }
        });
    }

    std::vector<std::vector<uint64_t>> received(kConsumers);
    std::vector<std::thread> consumers;
    for (unsigned c = 0; c < kConsumers; ++c) {
        consumers.emplace_back([&, c] {
            uint64_t v;
            while (q.pop(v)) received[c].push_back(v);
        });
    }

    std::atomic<bool> done{false};
    std::atomic<std::size_t> max_seen{0};
    std::thread monitor([&] {
        while (!done.load()) {
            std::size_t s = q.size();
            if (s > max_seen.load()) max_seen.store(s);
        }
    });

    for (auto& t : producers) t.join();
    q.close(); // consumers drain the remainder, then exit
    for (auto& t : consumers) t.join();
    done.store(true);
    monitor.join();

    CHECK(max_seen.load() <= kCapacity);

    std::vector<uint8_t> seen(kProducers * kPerProducer, 0);
    uint64_t total = 0;
    for (const auto& vec : received) {
        std::vector<int64_t> last(kProducers, -1);
        for (uint64_t v : vec) {
            uint64_t p = v >> 32;
            uint64_t i = v & 0xffffffffu;
            CHECK(p < kProducers && i < kPerProducer);
            CHECK(int64_t(i) > last[p]); // per-producer FIFO order preserved
            last[p] = int64_t(i);
            uint8_t& slot = seen[p * kPerProducer + i];
            CHECK(slot == 0); // no duplicates
            slot = 1;
            ++total;
        }
    }
    CHECK(total == kProducers * kPerProducer); // no loss
    for (uint8_t s : seen) CHECK(s == 1);
}

int main() {
    struct { const char* name; void (*fn)(); } tests[] = {
        {"basic_fifo", test_basic_fifo},
        {"close_drains_then_fails", test_close_drains_then_fails},
        {"push_blocks_when_full", test_push_blocks_when_full},
        {"close_unblocks_waiters", test_close_unblocks_waiters},
        {"mpmc_exactly_once", test_mpmc_exactly_once},
    };
    for (auto& t : tests) {
        t.fn();
        std::printf("PASS %s\n", t.name);
    }
    std::printf("all %zu tests passed\n", sizeof(tests) / sizeof(tests[0]));
    return 0;
}
