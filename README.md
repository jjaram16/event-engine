# event-engine

A multithreaded, in-memory event processing engine in C++20. A producer
generates a stream of sensor-style events from thousands of sources. A pool of
worker threads runs each event through a prioritized rule engine (threshold,
rate-of-change, and streaming z-score anomaly detection), and a metrics thread
reports throughput, latency, queue depth, and alert counts once a second.

It is a small study of a common pattern: sharded workers fed by bounded
queues, with backpressure and lock-free per-worker state.

## Architecture

```
                         source_id % N
              ┌──────────────────────────────┐
              │                              │
 ┌──────────┐ │   ┌──────────────┐   ┌──────────┐
 │ producer │─┼──▶│ BoundedQueue │──▶│ worker 0 │──┐
 └──────────┘ │   └──────────────┘   └──────────┘  │
              │   ┌──────────────┐   ┌──────────┐  │   atomic counters
              ├──▶│ BoundedQueue │──▶│ worker 1 │──┼──▶ (produced, consumed,
              │   └──────────────┘   └──────────┘  │    latency, alerts)
              │         ...              ...       │          │
              │   ┌──────────────┐   ┌──────────┐  │          ▼
              └──▶│ BoundedQueue │──▶│ worker N │──┘   ┌──────────────┐
                  └──────────────┘   └──────────┘      │ metrics (1s) │
                                                       └──────────────┘
```

**Producer.** A single thread creates `Event`s (`id`, `source_id`,
`created` timestamp, `value`) for 4096 sources. Values are uniform in 0–100.
About 1 in 2000 events (`spike_probability`) is an injected outlier, a value
100–200 units outside the normal band, which stands in for a sensor glitch or
burst.

**Worker pool, sharded by `source_id`.** There is one worker per hardware
thread, and each worker has its own queue. The producer routes an event to
worker `source_id % N`, so every event from a given source goes to the same
worker. That means per-source state (last value, running mean and variance)
lives in plain vectors owned by one thread and needs no locks.

**Bounded queue and backpressure.** `BoundedQueue<T>` (`bounded_queue.hpp`)
is a mutex and two condition variables around a `std::deque` with a fixed
capacity (20,000 per worker). `push` blocks while the queue is full, so a slow
worker throttles the producer and memory use stays bounded. `pop` blocks
while it is empty. `close()` wakes every waiter. After a close, `push` returns
`false`, while `pop` keeps draining the remaining items and returns `false`
only once the queue is empty, so shutdown loses no events.

**Rule engine (per event, in the worker).**

| Priority | Rule |
|---|---|
| P2 | threshold: `value > 80` |
| P1 | rate of change vs. the source's previous value: `|Δ| > 25` |
| P0 | large rate of change: `|Δ| > 40` |
| P0 | anomaly: z-score `> 3` against the source's running mean/stddev (Welford), after a 30-sample warmup; overrides everything else |

An event raises at most one alert, at its highest priority.

**Metrics thread.** Once a second it prints cumulative counts and per-second
rates for produced/consumed events, average and max end-to-end latency
(producer timestamp to worker pickup), total and max queue depth, and alerts
by priority. All counters are relaxed atomics.

## Build and run

Requires a C++20 compiler (clang or gcc) and `make`.

```sh
make              # builds build/event_engine and build/test_queue
make run          # runs the engine for 5 seconds
make test         # bounded-queue tests
make tsan         # same tests under ThreadSanitizer
make clean
```

Docker (builds and runs the tests inside the image):

```sh
docker build -t event-engine .
docker run --rm event-engine
```

Settings (source count, run time, thresholds, queue capacity, spike rate)
are constants at the top of `main()` in `main.cpp`.

## Tests

`tests/test_queue.cpp` covers the concurrency-critical piece, `BoundedQueue`:

- FIFO push/pop and `size()`
- `close()` semantics: rejects new pushes, drains queued items, then reports empty
- backpressure: `push` on a full queue blocks until a `pop` frees space
- `close()` releases blocked producers and consumers
- MPMC stress: 8 producers × 50,000 items through a capacity-16 queue with 8
  consumers. Every item must arrive exactly once (no loss or duplication), each
  producer's items must stay in order, and `size()` must never exceed capacity.

## Performance

Measured on an Apple Silicon Mac (arm64), `-O2`, one worker per hardware
thread, 5-second run, after anomaly injection was added:

| Metric | Value |
|---|---|
| Throughput | ~1.3–1.4M events/s (6.76M events in 5s) |
| Avg latency (enqueue → worker) | ~116 µs |
| Max latency | ~12 ms |
| Queue depth | single digits to low tens per worker; no sustained backlog |
| Alerts | ~63% of events (P0 ~36%, P1 ~20%, P2 ~7%) |
| Anomalies | 3,315 (~98% of the ~3,378 injected spikes; the rest hit a source during warmup) |

An earlier build measured ~290k events/s, ~24 µs average latency, and
~1.8 ms max latency (see the sample output below). Results depend heavily on
core count and load.

### Sample output

Current build:

```
1001ms [metrics] prod=1352839 cons=1352760 pps=1352839 cps=1352760 avg_us=41 max_us=429 depth_total=59 depth_max=22 alerts=854611 aps=854611 p0=486205 p0ps=486205 p1=273107 p1ps=273107 p2=95299 p2ps=95299 anom=638 annomps=638
2006ms [metrics] prod=2746839 cons=2746839 pps=1394000 cps=1394079 avg_us=228 max_us=12431 depth_total=3 depth_max=1 alerts=1737090 aps=882479 p0=989195 p0ps=502990 p1=554878 p1ps=281771 p2=193017 p2ps=97718 anom=1331 annomps=693
3011ms [metrics] prod=4102255 cons=4102247 pps=1355416 cps=1355408 avg_us=166 max_us=12431 depth_total=7 depth_max=3 alerts=2595409 aps=858319 p0=1477951 p0ps=488756 p1=829716 p1ps=274838 p2=287742 p2ps=94725 anom=1977 annomps=646
4016ms [metrics] prod=5439646 cons=5439636 pps=1337391 cps=1337389 avg_us=135 max_us=12431 depth_total=13 depth_max=8 alerts=3441840 aps=846431 p0=1960063 p0ps=482112 p1=1100847 p1ps=271131 p2=380930 p2ps=93188 anom=2682 annomps=705
5020ms [metrics] prod=6755089 cons=6755089 pps=1315443 cps=1315453 avg_us=116 max_us=12431 depth_total=0 depth_max=0 alerts=4274558 aps=832718 p0=2434625 p0ps=474562 p1=1367026 p1ps=266179 p2=472907 p2ps=91977 anom=3315 annomps=633
Totals: produced=6755089 consumed=6755089 avg_latency_us=116 max_latency_us=12431 alerts=4274558 p0=2434625 p1=1367026 p2=472907 anomaly=3315
Done.
```

Earlier build, before spike injection (`anom` stayed at 0 because uniform 0–100
values cap the z-score at about 1.73). The first line is truncated in the
original:

```
... p1ps=64659 p2=23081 p2ps=23081 anom=0 annomps=0
2002ms [metrics] prod=610915 cons=610914 pps=288764 cps=288764 avg_us=24 max_us=1696 depth_total=3 depth_max=1 alerts=384753 aps=182571 p0=218110 p0ps=103668 p1=123349 p1ps=58690 p2=43294 p2ps=20213 anom=0 annomps=0
3003ms [metrics] prod=901680 cons=901680 pps=290765 cps=290766 avg_us=22 max_us=1696 depth_total=5 depth_max=2 alerts=569082 aps=184329 p0=323358 p0ps=105248 p1=182155 p1ps=58806 p2=63569 p2ps=20275 anom=0 annomps=0
4004ms [metrics] prod=1195572 cons=1195570 pps=293892 cps=293890 avg_us=25 max_us=1795 depth_total=2 depth_max=1 alerts=754736 aps=185654 p0=429228 p0ps=105870 p1=241368 p1ps=59213 p2=84140 p2ps=20571 anom=0 annomps=0
5006ms [metrics] prod=1479932 cons=1479932 pps=284360 cps=284362 avg_us=24 max_us=1795 depth_total=0 depth_max=0 alerts=934218 aps=179482 p0=531352 p0ps=102124 p1=298921 p1ps=57553 p2=103945 p2ps=19805 anom=0 annomps=0
Totals: produced=1479932 consumed=1479932 avg_latency_us=24 max_latency_us=1795 alerts=934218 p0=531352 p1=298921 p2=103945 anomaly=0
Done.
```

## Known limitations

- The anomaly detector folds the current value into the running stats before
  scoring it, so a spike slightly inflates its own baseline. Stats never decay,
  so one outlier permanently widens that source's stddev.
- The single producer is the throughput ceiling. Workers mostly sit idle on
  near-empty queues.
- The metric label `annomps` is misspelled in the output and is kept for
  compatibility with existing logs.
