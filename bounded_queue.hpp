#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>

template <typename T>
class BoundedQueue{
public :
    explicit BoundedQueue(std::size_t capacity)
        : capacity_(capacity) {}

    
    bool push(T item){
        std::unique_lock<std::mutex> lock(mtx_);

        //wait
        not_full_.wait(lock, [this](){
            return closed_ || q_.size() < capacity_;
        });

        if (closed_){
            return false;
        }

        q_.push_back(std::move(item));
        not_empty_.notify_one();

        return true;

    }

    bool pop (T& out){
        std::unique_lock<std::mutex> lock(mtx_);

        //wait
        not_empty_.wait(lock, [this](){
            return closed_ || !q_.empty();
        });

        if (q_.empty() && closed_){
            return false;
        }
        
        out = std::move(q_.front());
        q_.pop_front();

        not_full_.notify_one();

        return true;
    }

    void close(){
        std::lock_guard<std::mutex> lock(mtx_);
        closed_ = true;
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    std::size_t size() const{
        std::lock_guard<std::mutex> lock(mtx_);
        return q_.size();
    }

private:
    const std::size_t capacity_;

    mutable std::mutex mtx_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;

    std::deque<T> q_;
    bool closed_ = false;
};