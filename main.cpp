#include <iostream>
#include "bounded_queue.hpp"
#include "event.hpp"

#include <thread>
#include <atomic>
#include <chrono>
#include <mutex>
#include <random>
#include <vector>
#include <memory>
#include <cmath>
#include <limits>


std::mutex cout_mtx;

//Atomics multiple threads can change it without corrupting the memory
std::atomic<uint64_t> produced{0}; // how many events were made
std::atomic<uint64_t> consumed{0}; // hpw many were processed
std::atomic<uint64_t> latency_sum_us{0};//timing stats
std::atomic<uint64_t> latency_max_us{0};
std::atomic<uint64_t> alerts_total{0}; //how many alerts happened
std::atomic<uint64_t> alerts_p0{0};
std::atomic<uint64_t> alerts_p1{0};
std::atomic<uint64_t> alerts_p2{0};
std::atomic<uint64_t> anomaly_alerts{0};


static inline void atomic_max_u64(std::atomic<uint64_t>& target, uint64_t v) {
    uint64_t prev = target.load(std::memory_order_relaxed);
    while (v > prev && !target.compare_exchange_weak(prev, v, std::memory_order_relaxed)) {
    }
}

int main() {
    const bool verbose = false;
    const uint32_t num_sources = 4096;
    const int run_seconds = 5;

    //threshold
    const double threshold = 80.0; //if its >80, trigger alert

    // ROC; did the number change to quickly compared to previous number?
    const double roc_threshold = 25.0; // if difference is > 25 medium-alert
    const double roc_high_threshold = 40.0; //if difference is >40 = BIG alert

    //unusual compared to normla behavior
    const uint64_t anomaly_warmup = 30;
    const double z_thresh = 3.0;

    // fraction of events that are injected outliers (~1 in 2000)
    const double spike_probability = 0.0005;

    //Worker count (how many workers i spawned)
    unsigned worker_count_raw = std::thread::hardware_concurrency(); //asks computer how many CPU threads do you have?
    unsigned worker_count = (worker_count_raw == 0 ? 4 : worker_count_raw); //if you dont answer pick 4

    const size_t queue_capacity = 20000;

    auto start = std::chrono::steady_clock::now();
    auto ms_since_start = [&]() -> long long {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start
        ).count();
    };

    // one queue per worker (pointers because BoundedQueue isn't movable)
    std::vector<std::unique_ptr<BoundedQueue<Event>>> queues;
    queues.reserve(worker_count);
    for (unsigned i = 0; i < worker_count; ++i) {
        queues.emplace_back(std::make_unique<BoundedQueue<Event>>(queue_capacity));
    }

    std::atomic<bool> stop{false};
    std::atomic<bool> metrics_stop{false};

    // PRODUCER
    std::thread producer([&]() {
        uint64_t id = 1;

        std::mt19937 rng(std::random_device{}());
        std::uniform_real_distribution<double> dist(0.0, 100.0);
        std::uniform_int_distribution<uint32_t> src_dist(0, num_sources - 1);

        // rare spikes (sensor glitch / burst) far outside the normal 0-100 band.
        // uniform(0,100) has stddev ~28.9, so z > 3 needs |x - 50| > ~87;
        // spikes land 100-200 beyond the band so the anomaly rule can fire.
        std::bernoulli_distribution spike(spike_probability);
        std::uniform_real_distribution<double> spike_mag(100.0, 200.0);
        std::bernoulli_distribution spike_up(0.5);

        while (!stop.load(std::memory_order_relaxed)) {
            Event e;
            e.id = id++;
            e.source_id = src_dist(rng);
            e.created = std::chrono::steady_clock::now();
            e.value = dist(rng);
            if (spike(rng)) {
                e.value = spike_up(rng) ? 100.0 + spike_mag(rng) : -spike_mag(rng);
            }

            unsigned w = e.source_id % worker_count;
            bool ok = queues[w]->push(e);
            if (!ok) break;

            produced.fetch_add(1, std::memory_order_relaxed);

            if (verbose) {
                std::lock_guard<std::mutex> g(cout_mtx);
                std::cout << ms_since_start() << "ms [producer] pushed id=" << e.id
                          << " src=" << e.source_id
                          << " value=" << e.value << "\n";
            }
        }

        if (verbose) {
            std::lock_guard<std::mutex> g(cout_mtx);
            std::cout << ms_since_start() << "ms [producer] exiting\n";
        }
    });

    // WORKER
    std::vector<std::thread> workers;
    workers.reserve(worker_count);
    for (unsigned wi = 0; wi < worker_count; ++wi) {
        workers.emplace_back([&, wi]() {

            // per-worker state (no locks)
            std::vector<double> last_value(num_sources, std::numeric_limits<double>::quiet_NaN());
            std::vector<uint64_t> n(num_sources, 0);
            std::vector<double> mean(num_sources, 0.0);
            std::vector<double> m2(num_sources, 0.0);

            Event e;
            while (queues[wi]->pop(e)) {
                consumed.fetch_add(1, std::memory_order_relaxed);

                auto now = std::chrono::steady_clock::now();
                uint64_t lat_us = (uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
                    now - e.created
                ).count();

                latency_sum_us.fetch_add(lat_us, std::memory_order_relaxed);
                atomic_max_u64(latency_max_us, lat_us);

                // --- ANOMALY DETECTION (Welford) ---
                bool is_anomaly = false;

                uint32_t s = e.source_id;
                double x = e.value;

                uint64_t n1 = ++n[s];
                double d1 = x - mean[s];
                mean[s] += d1 / (double)n1;
                double d2 = x - mean[s];
                m2[s] += d1 * d2;

                if (n1 >= anomaly_warmup) {
                    double var = (n1 > 1) ? (m2[s] / (double)(n1 - 1)) : 0.0;
                    double stddev = (var > 0.0) ? std::sqrt(var) : 0.0;
                    if (stddev > 0.0) {
                        double z = std::fabs(x - mean[s]) / stddev;
                        if (z > z_thresh) {
                            is_anomaly = true;
                            anomaly_alerts.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                }

                // --- prioritized rule engine (threshold + ROC + anomaly) ---
                int priority = -1;

                // P2: threshold
                if (e.value > threshold) {
                    priority = 2;
                }

                // ROC: compare to last value for this source
                double prev = last_value[s];
                if (!std::isnan(prev)) {
                    double delta = e.value - prev;
                    double ad = std::fabs(delta);

                    if (ad > roc_high_threshold) priority = 0;            // P0
                    else if (ad > roc_threshold && priority != 0) priority = 1; // P1
                }
                last_value[s] = e.value;

                // anomaly overrides everything -> P0
                if (is_anomaly) {
                    priority = 0;
                }

                if (priority != -1) {
                    alerts_total.fetch_add(1, std::memory_order_relaxed);
                    if (priority == 0) alerts_p0.fetch_add(1, std::memory_order_relaxed);
                    else if (priority == 1) alerts_p1.fetch_add(1, std::memory_order_relaxed);
                    else alerts_p2.fetch_add(1, std::memory_order_relaxed);
                }

                if (verbose) {
                    std::lock_guard<std::mutex> g(cout_mtx);
                    std::cout << ms_since_start() << "ms [worker " << wi << "] id=" << e.id
                            << " src=" << e.source_id
                            << " value=" << e.value
                            << " pri=" << priority
                            << " lat_us=" << lat_us
                            << " anomaly=" << (is_anomaly ? 1 : 0)
                            << "\n";
                }
            }


            if (verbose) {
                std::lock_guard<std::mutex> g(cout_mtx);
                std::cout << ms_since_start() << "ms [worker " << wi << "] exiting\n";
            }
        });
    }


    // METRICS 
    std::thread metrics([&]() {
        uint64_t last_p = 0, last_c = 0;
        uint64_t last_alerts = 0;
        uint64_t last_p0 = 0, last_p1 = 0, last_p2 = 0;
        uint64_t last_anom =0;


        while (!metrics_stop.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(std::chrono::seconds(1));

            uint64_t p = produced.load(std::memory_order_relaxed);
            uint64_t c = consumed.load(std::memory_order_relaxed);
            uint64_t a  = alerts_total.load(std::memory_order_relaxed);
            uint64_t p0 = alerts_p0.load(std::memory_order_relaxed);
            uint64_t p1 = alerts_p1.load(std::memory_order_relaxed);
            uint64_t p2 = alerts_p2.load(std::memory_order_relaxed);
            uint64_t anom = anomaly_alerts.load(std::memory_order_relaxed);

            uint64_t pps = p - last_p;
            uint64_t cps = c - last_c;
            uint64_t aps = a - last_alerts;
            uint64_t p0ps = p0 - last_p0;
            uint64_t p1ps = p1 - last_p1;
            uint64_t p2ps = p2 - last_p2;
            uint64_t anomps = anom - last_anom;

            last_p = p;
            last_c = c;
            last_alerts = a;
            last_p0 = p0; last_p1 = p1; last_p2 = p2;
            last_anom = anom;

            size_t depth_total = 0;
            size_t depth_max = 0;
            for (unsigned i = 0; i < worker_count; ++i) {
                size_t d = queues[i]->size();
                depth_total += d;
                if (d > depth_max) depth_max = d;
            }

            uint64_t sum_us = latency_sum_us.load(std::memory_order_relaxed);
            uint64_t max_us = latency_max_us.load(std::memory_order_relaxed);
            uint64_t avg_us = (c > 0) ? (sum_us / c) : 0;

            std::lock_guard<std::mutex> g(cout_mtx);
            std::cout   << ms_since_start() << "ms [metrics] "
                        << "prod=" << p << " cons=" << c
                        << " pps=" << pps << " cps=" << cps
                        << " avg_us=" << avg_us << " max_us=" << max_us
                        << " depth_total=" << depth_total << " depth_max=" << depth_max
                        << " alerts=" << a << " aps=" << aps
                        << " p0=" << p0 << " p0ps=" << p0ps
                        << " p1=" << p1 << " p1ps=" << p1ps
                        << " p2=" << p2 << " p2ps=" << p2ps
                        <<" anom=" << anom << " annomps=" << anomps
                        << "\n";
        }
    });

    // run
    std::this_thread::sleep_for(std::chrono::seconds(run_seconds));

    stop.store(true, std::memory_order_relaxed);

    for (unsigned i = 0; i < worker_count; ++i) {
        queues[i]->close();
    }

    producer.join();
    for (auto& w : workers) w.join();

    metrics_stop.store(true, std::memory_order_relaxed);
    metrics.join();

    uint64_t p = produced.load();
    uint64_t c = consumed.load();
    uint64_t avg_us = (c > 0) ? (latency_sum_us.load() / c) : 0;

    std::cout << "Totals: produced=" << p
              << " consumed=" << c
              << " avg_latency_us=" << avg_us
              << " max_latency_us=" << latency_max_us.load()
              << " alerts=" << alerts_total.load()
              << " p0=" << alerts_p0.load()
              << " p1=" << alerts_p1.load()
              << " p2=" << alerts_p2.load()
              <<" anomaly=" << anomaly_alerts.load()
              << "\n";
    std::cout << "Done.\n";
    return 0;
}

// build: make            (see Makefile)
// run:   ./build/event_engine
