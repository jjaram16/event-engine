#pragma once
#include <cstdint>
#include <chrono>

struct Event {
    uint64_t id; //id = unique identifier
    uint32_t source_id;//which stream it belongs to? where the number came from
    std::chrono::steady_clock::time_point created; //created = timestamps to emasure latency
    double value;
};