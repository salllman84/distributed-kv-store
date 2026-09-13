#include "common/thread_pool.hpp"

namespace common {

ThreadPool::ThreadPool(size_t threads) : stop_(false) {
    for(size_t i = 0; i < threads; ++i) {
        workers_.emplace_back([this] {
            while(true) {
                std::function<void()> task;
                {
                    // Lock the queue to check for tasks
                    std::unique_lock<std::mutex> lock(this->queue_mutex_);
                    
                    // Sleep until there is a task OR we are shutting down
                    this->condition_.wait(lock, [this] { 
                        return this->stop_ || !this->tasks_.empty(); 
                    });
                    
                    // Exit thread if stopped and queue is empty
                    if(this->stop_ && this->tasks_.empty()) {
                        return;
                    }
                    
                    // Pop the task from the queue
                    task = std::move(this->tasks_.front());
                    this->tasks_.pop();
                }
                // Execute the task outside the lock so other threads can keep fetching
                task();
            }
        });
    }
}

void ThreadPool::enqueue(std::function<void()> task) {
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if(stop_) {
            return; // Don't accept new tasks if shutting down
        }
        tasks_.push(std::move(task));
    }
    // Wake up one sleeping worker
    condition_.notify_one();
}

ThreadPool::~ThreadPool() {
    stop_ = true;
    
    // Wake up all threads so they can exit their loops
    condition_.notify_all();
    
    // Wait for all threads to finish gracefully
    for(std::thread &worker : workers_) {
        if(worker.joinable()) {
            worker.join();
        }
    }
}

} // namespace common