//
// Created by will on 7/6/18.
//

#pragma once

#include <cstdlib>
#include <mutex>
#include <condition_variable>

#include <functional>

class Barrier {
public:
    explicit Barrier(std::size_t iCount) :
            mThreshold(iCount),
            mCount(iCount),
            mGeneration(0) {
    }

    void Wait(std::function<void()> func) {
        std::unique_lock<std::mutex> lLock{mMutex};
        auto lGen = mGeneration;
        if (!--mCount) {
            mGeneration++;
            mCount = mThreshold;
            func();
            mCond.notify_all();
        } else {
            mCond.wait(lLock, [this, lGen] { return lGen != mGeneration; });
        }
    }

private:
    std::mutex mMutex;
    std::condition_variable mCond;
    std::size_t mThreshold;
    std::size_t mCount;
    std::size_t mGeneration;
};