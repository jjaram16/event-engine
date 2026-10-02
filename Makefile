CXX      ?= c++
CXXFLAGS ?= -std=c++20 -O2 -pthread -Wall -Wextra
LDFLAGS  ?= -pthread

BUILD    := build
HEADERS  := bounded_queue.hpp event.hpp

.PHONY: all run test tsan clean

all: $(BUILD)/event_engine $(BUILD)/test_queue

$(BUILD):
	mkdir -p $(BUILD)

$(BUILD)/event_engine: main.cpp $(HEADERS) | $(BUILD)
	$(CXX) $(CXXFLAGS) main.cpp -o $@ $(LDFLAGS)

$(BUILD)/test_queue: tests/test_queue.cpp bounded_queue.hpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -I. tests/test_queue.cpp -o $@ $(LDFLAGS)

run: $(BUILD)/event_engine
	./$(BUILD)/event_engine

test: $(BUILD)/test_queue
	./$(BUILD)/test_queue

# Same tests under ThreadSanitizer to catch data races in the queue.
tsan: tests/test_queue.cpp bounded_queue.hpp | $(BUILD)
	$(CXX) -std=c++20 -O1 -g -fsanitize=thread -pthread -I. tests/test_queue.cpp -o $(BUILD)/test_queue_tsan
	./$(BUILD)/test_queue_tsan

clean:
	rm -rf $(BUILD)
