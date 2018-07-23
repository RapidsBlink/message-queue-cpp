#pragma once

#include <set>
#include <mutex>
#include <algorithm>


template<typename T, typename Compare = std::less<T>>
class ConcurrentSet {
private:
    std::set<T, Compare> set_;
    std::mutex mutex_;

public:
    typedef typename std::set<T, Compare>::iterator iterator;

    std::pair<iterator, bool>
    insert(const T &val) {
        std::unique_lock<std::mutex> lock(this->mutex_);
        return set_.insert(val);
    }

    size_t size() {
        std::unique_lock<std::mutex> lock(this->mutex_);
        return set_.size();
    }

};